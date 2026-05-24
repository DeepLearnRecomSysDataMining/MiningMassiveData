import pickle
import pandas as pd
import numpy as np
import logging
import os
import pyarrow.parquet as pq
import gcsfs
from torch.utils.data import IterableDataset, get_worker_info
from config.training_config import TrainingConfig

logger = logging.getLogger("data_utils")

class PrecomputedEmbeddingLookup:
    """Hệ thống tra cứu Vector siêu tốc bằng Memory-Mapping."""
    def __init__(self, embeddings_npy, id_to_idx):
        self.embeddings = embeddings_npy
        self.id_to_idx = id_to_idx
        self.dim = embeddings_npy.shape[1]

    def get_embedding(self, item_id):
        idx = self.id_to_idx.get(item_id)
        if idx is None:
            return np.zeros(self.dim, dtype=np.float32)
        return self.embeddings[idx]

def load_precomputed_embeddings():
    """Nạp dữ liệu vector đã tính toán trước."""
    emb_path = TrainingConfig.ITEM_EMBEDDINGS_PATH
    idx_path = TrainingConfig.ITEM_INDEX_PATH
    
    if not os.path.exists(emb_path) or not os.path.exists(idx_path):
        logger.error("KHÔNG TÌM THẤY DỮ LIỆU PRECOMPUTED!")
        return None
        
    with open(idx_path, "rb") as f:
        id_to_idx = pickle.load(f)
    embeddings = np.load(emb_path, mmap_mode='r')
    # Check consistency
    if len(id_to_idx) != embeddings.shape[0]:
        raise ValueError(
            f"Index/vector mismatch: index={len(id_to_idx):,}, vectors={embeddings.shape[0]:,}"
        )

    max_idx = max(id_to_idx.values())
    if max_idx >= embeddings.shape[0]:
        raise ValueError(
            f"Index out of range: max_idx={max_idx}, vectors={embeddings.shape[0]}"
        )
    return PrecomputedEmbeddingLookup(embeddings, id_to_idx)

def load_eval_dataset():
    """Tải tập Evaluation (Pickle) để đánh giá. Tự động tải từ GCS nếu cần."""
    path = TrainingConfig.EVAL_PKL_PATH
    
    if not os.path.exists(path):
        if TrainingConfig.IS_CLOUD:
            logger.info(f"Đang tải evaluation_dataset.pkl từ GCS về: {path}")
            import subprocess
            gcs_src = f"{TrainingConfig.GCS_PREPARED_DATA}/evaluation_dataset.pkl"
            result = subprocess.run(["gsutil", "cp", gcs_src, path], capture_output=True)
            if result.returncode != 0:
                logger.warning(f"Không thể tải file từ {gcs_src} (Có thể file chưa tồn tại trên GCS).")
        
        # Fallback kiểm tra lần cuối
        if not os.path.exists(path):
            path_local = "data/prepared_data_improved/evaluation_dataset.pkl"
            if os.path.exists(path_local):
                path = path_local
            else:
                raise FileNotFoundError(f"Evaluation dataset không tìm thấy tại {path} hoặc {path_local}")
            
    with open(path, 'rb') as f:
        return pickle.load(f)

def load_interactions_df():
    """
    Tải lịch sử tương tác sử dụng PyArrow (Thay thế hoàn toàn thư viện datasets).
    Chỉ nạp tỷ lệ phần trăm dữ liệu được chỉ định (mặc định 25%).
    """
    import gcsfs
    path = TrainingConfig.GCS_INTERACTIONS if TrainingConfig.IS_CLOUD else "data/all_interactions"
    fraction = TrainingConfig.DATA_FRACTION
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE
    
    if rank == 0:
        logger.info(f"==> [PyArrow] Đang nạp {int(fraction*100)}% tương tác từ: {path}")
    
    # 1. Kết nối GCS nếu cần
    fs = gcsfs.GCSFileSystem() if TrainingConfig.IS_CLOUD else None
    arrow_path = path.replace("gs://", "") if TrainingConfig.IS_CLOUD else path
    
    # 2. Đọc toàn bộ bảng (Chỉ lấy 2 cột ID để tiết kiệm RAM)
    dataset = pq.ParquetDataset(arrow_path, filesystem=fs)
    all_fragments = list(dataset.fragments)

    # 1. Lấy fraction theo thứ tự global trước
    total_fragments = len(all_fragments)
    target_fragments = max(1, int(total_fragments * fraction))
    selected_fragments = all_fragments[:target_fragments]

    # 2. Chia fragment cho từng rank
    if world_size > 1:
        rank_fragments = selected_fragments[rank::world_size]
    else:
        rank_fragments = selected_fragments

    asin_list = []
    product_list = []
    local_rows = 0

    if rank == 0:
        logger.info(
            f"==> Tổng fragments={total_fragments}, dùng={len(selected_fragments)}, "
            f"world_size={world_size}"
        )

    for i, frag in enumerate(rank_fragments):
        table = frag.to_table(columns=["asin", "product_id"])

        asin_arr = table.column("asin").to_numpy()
        product_arr = table.column("product_id").to_numpy()

        asin_list.append(asin_arr)
        product_list.append(product_arr)

        local_rows += len(asin_arr)

        logger.info(
            f"Rank {rank}: loaded fragment {i+1}/{len(rank_fragments)} | "
            f"local_rows={local_rows:,}"
        )

        del table, asin_arr, product_arr

    asin_all = np.concatenate(asin_list).astype(str)
    product_all = np.concatenate(product_list).astype(str)

    # Shuffle nhẹ 1 lần để batch đa dạng hơn, tránh DataLoader shuffle gây OOM
    rng = np.random.default_rng(42 + rank)
    perm = rng.permutation(len(asin_all))

    asin_all = asin_all[perm]
    product_all = product_all[perm]

    logger.info(
        f"Rank {rank}: final local interactions={len(asin_all):,}"
    )

    return {
        "asin": asin_all,
        "product_id": product_all
    }

class LLMCHGNNEmbeddingLookup:
    def __init__(self, embeddings_npy, id_to_idx):
        self.embeddings = embeddings_npy
        self.id_to_idx = id_to_idx
        self.dim = embeddings_npy.shape[1]

    def get_embedding(self, item_id):
        idx = self.id_to_idx.get(item_id)
        if idx is None:
            return np.zeros(self.dim, dtype=np.float32)
        return self.embeddings[idx]


def load_llm_chgnn_embeddings():
    import subprocess

    local_npy = TrainingConfig.LLM_CHGNN_EMBEDDINGS_PATH
    local_pkl = TrainingConfig.LLM_CHGNN_INDEX_PATH

    if TrainingConfig.IS_CLOUD:
        gcs_dir = TrainingConfig.GCS_LLM_CHGNN_EMBEDDINGS

        if not os.path.exists(local_npy):
            subprocess.run(
                ["gsutil", "cp", f"{gcs_dir}/llm_chgnn_embeddings.npy", local_npy],
                check=True
            )

        if not os.path.exists(local_pkl):
            subprocess.run(
                ["gsutil", "cp", f"{gcs_dir}/llm_chgnn_index.pkl", local_pkl],
                check=True
            )

    if not os.path.exists(local_npy) or not os.path.exists(local_pkl):
        raise FileNotFoundError("Missing LLM-CHGNN precomputed embeddings.")

    with open(local_pkl, "rb") as f:
        id_to_idx = pickle.load(f)

    embeddings = np.load(local_npy, mmap_mode="r")

    return LLMCHGNNEmbeddingLookup(embeddings, id_to_idx)

class LLMCHGNNParquetDataset(IterableDataset):
    """
    Dataset đọc Parquet streaming cho LLM-CHGNN.
    Fork-safe với DataLoader(num_workers=1):
    - __init__ chỉ lưu path string
    - __iter__ mới tạo gcsfs/pyarrow trong worker process
    """
    def __init__(self):
        path = (
            TrainingConfig.GCS_LLM_CHGNN_TRAIN
            if TrainingConfig.IS_CLOUD
            else "data/llm_chgnn_train_dataset"
        )

        self.path = path
        self.is_cloud = TrainingConfig.IS_CLOUD
        self.cols = [
            "query_asin",
            "query_text",
            "query_specs",
            "query_category",
            "candidate_ids",
            "candidate_texts",
            "candidate_specs",
            "candidate_categories",
            "true_vn_id",
        ]

        # Chỉ list file path string, không giữ fs/dataset/fragment object.
        if self.is_cloud:
            fs = gcsfs.GCSFileSystem()
            raw_path = path.replace("gs://", "")
            files = sorted([f"gs://{f}" for f in fs.ls(raw_path) if f.endswith(".parquet")])
        else:
            import glob
            files = sorted(glob.glob(os.path.join(path, "*.parquet")))

        rank = TrainingConfig.RANK
        world_size = TrainingConfig.WORLD_SIZE

        self.files = files[rank::world_size] if world_size > 1 else files

        # Tính length bằng metadata, nhưng không giữ filesystem object.
        self._length = 0
        for f in self.files:
            try:
                fs = gcsfs.GCSFileSystem() if self.is_cloud else None
                table_path = f.replace("gs://", "") if self.is_cloud else f
                pf = pq.ParquetFile(table_path, filesystem=fs)
                self._length += pf.metadata.num_rows
            except Exception:
                pass

    def __iter__(self):
        files = self.files

        worker_info = get_worker_info()
        if worker_info is not None:
            files = files[worker_info.id::worker_info.num_workers]

        # Tạo filesystem riêng trong worker process để tránh lỗi fork-safe.
        fs = gcsfs.GCSFileSystem() if self.is_cloud else None

        for i, file_path in enumerate(files):
            arrow_path = file_path.replace("gs://", "") if self.is_cloud else file_path

            table = pq.read_table(
                arrow_path,
                columns=self.cols,
                filesystem=fs
            )

            rows = table.to_pylist()

            logger.info(
                f"Rank {TrainingConfig.RANK}: streaming LLM-CHGNN file "
                f"{i+1}/{len(files)} | rows={len(rows):,} | file={os.path.basename(file_path)}"
            )

            for row in rows:
                yield row

            del table, rows

    def __len__(self):
        return self._length

def load_llm_chgnn_train_dataset():
    return LLMCHGNNParquetDataset()