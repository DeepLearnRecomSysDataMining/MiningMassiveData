import pickle
import pandas as pd
import numpy as np
import logging
import os
import pyarrow.parquet as pq
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
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> [PyArrow] Đang nạp {int(fraction*100)}% tương tác từ: {path}")
    
    # 1. Kết nối GCS nếu cần
    fs = gcsfs.GCSFileSystem() if TrainingConfig.IS_CLOUD else None
    arrow_path = path.replace("gs://", "") if TrainingConfig.IS_CLOUD else path
    
    # 2. Đọc toàn bộ bảng (Chỉ lấy 2 cột ID để tiết kiệm RAM)
    dataset = pq.ParquetDataset(arrow_path, filesystem=fs)
    fragments = list(dataset.fragments)

    total_rows = 0
    for frag in fragments:
        total_rows += frag.metadata.num_rows

    target_rows = int(total_rows * fraction)

    chunks = []
    loaded_rows = 0

    for frag in fragments:
        if loaded_rows >= target_rows:
            break

        table = frag.to_table(columns=["asin", "product_id"])

        remaining = target_rows - loaded_rows
        if table.num_rows > remaining:
            table = table.slice(0, remaining)

        df_chunk = table.to_pandas()
        chunks.append(df_chunk)
        loaded_rows += len(df_chunk)

        if TrainingConfig.RANK == 0:
            logger.info(f"  - Đã nạp {loaded_rows:,}/{target_rows:,} dòng")

    if not chunks:
        raise ValueError("Không đọc được interaction nào.")

    df = pd.concat(chunks, ignore_index=True)

    if TrainingConfig.RANK == 0:
        logger.info(
            f"==> Thành công! Đã nạp {len(df):,} dòng tương tác "
            f"trên tổng {total_rows:,} dòng"
        )

    return df


    # table = dataset.read(columns=['asin', 'product_id'])
    
    # # 3. Thực hiện Slice (Cắt) lấy 25% đầu tiên
    # num_rows = table.num_rows
    # target_rows = int(num_rows * fraction)
    # table_subset = table.slice(0, target_rows)
    
    # if TrainingConfig.RANK == 0:
    #     logger.info(f"==> Thành công! Đã nạp {table_subset.num_rows:,} dòng tương tác (Tổng file: {num_rows:,})")
    
    # # Chuyển sang Pandas để các file Trainer truy cập được bằng index [idx]
    # return table_subset.to_pandas()
