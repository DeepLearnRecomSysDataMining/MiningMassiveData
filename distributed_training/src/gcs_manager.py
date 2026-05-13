import os
import subprocess
import logging
import numpy as np
import pickle
from config.training_config import TrainingConfig

logger = logging.getLogger("gcs_manager")

def download_training_data():
    """Tải dữ liệu .pkl và các file vector đã precompute (nếu có) từ GCS."""
    local_dir = TrainingConfig.LOCAL_DATA_DIR
    gcs_path = TrainingConfig.GCS_PREPARED_DATA
    
    logger.info(f"Đang đồng bộ dữ liệu từ {gcs_path} về {local_dir}...")
    
    # Tải các file .pkl cơ bản
    subprocess.run(["gsutil", "-m", "cp", f"{gcs_path}/*.pkl", local_dir], check=False)
    
    # Kiểm tra và tải file vector 12GB nếu đã tồn tại trên GCS (Tiết kiệm 1 tiếng precompute)
    emb_file = "item_embeddings.npy"
    idx_file = "item_index.pkl"
    
    for f in [emb_file, idx_file]:
        remote_f = f"{gcs_path}/{f}"
        local_f = os.path.join(local_dir, f)
        if not os.path.exists(local_f):
            logger.info(f"Đang kiểm tra {f} trên GCS...")
            # Kiểm tra file có tồn tại trên GCS không trước khi tải
            result = subprocess.run(["gsutil", "-q", "stat", remote_f], capture_output=True)
            if result.returncode == 0:
                logger.info(f"Tìm thấy {f} trên GCS. Đang tải về siêu tốc...")
                subprocess.run(["gsutil", "-m", "cp", remote_f, local_f], check=True)

def upload_precomputed_data():
    """Upload file vector và index tổng lên GCS."""
    local_dir = TrainingConfig.LOCAL_DATA_DIR
    gcs_path = TrainingConfig.GCS_PREPARED_DATA
    
    emb_file = os.path.join(local_dir, "item_embeddings.npy")
    idx_file = os.path.join(local_dir, "item_index.pkl")
    
    if os.path.exists(emb_file) and os.path.exists(idx_file):
        logger.info("Đang upload bộ nhớ đệm Vector tổng lên GCS...")
        subprocess.run(["gsutil", "-m", "cp", emb_file, gcs_path], check=True)
        subprocess.run(["gsutil", "-m", "cp", idx_file, gcs_path], check=True)
        
        # Ghi cờ hoàn tất cuối cùng để các Rank khác biết
        done_flag = f"{gcs_path}/_final_done.txt"
        subprocess.run(["gsutil", "cp", "/dev/null", done_flag], check=True)

def merge_precomputed_chunks():
    """
    Hợp nhất các file chunk từ GCS thành:
    - item_embeddings.npy
    - item_index.pkl

    Nguyên tắc:
    - Chỉ giữ các vector còn được trỏ bởi pkl index.
    - Bỏ orphan vectors do duplicate ID trong chunk.
    - Bỏ duplicate global giữa các chunk.
    - Tạo .npy đúng chuẩn bằng np.lib.format.open_memmap.
    """
    local_dir = TrainingConfig.LOCAL_DATA_DIR
    gcs_chunks_path = f"{TrainingConfig.GCS_PREPARED_DATA}/chunks"

    final_npy_path = TrainingConfig.ITEM_EMBEDDINGS_PATH
    final_pkl_path = TrainingConfig.ITEM_INDEX_PATH

    logger.info(">>> BẮT ĐẦU QUY TRÌNH HỢP NHẤT CHUNKS...")

    # 1. Tải toàn bộ chunk về local
    subprocess.run(["gsutil", "-m", "cp", f"{gcs_chunks_path}/*.npy", local_dir], check=True)
    subprocess.run(["gsutil", "-m", "cp", f"{gcs_chunks_path}/*.pkl", local_dir], check=True)

    all_npy_files = sorted([
        f for f in os.listdir(local_dir)
        if f.endswith(".npy") and f != "item_embeddings.npy"
    ])

    if not all_npy_files:
        logger.warning("Không tìm thấy file chunk nào để gộp!")
        return

    # 2. Khảo sát index thực sự dùng được
    file_info = []
    seen_global = set()
    total_rows = 0
    orphan_total = 0
    global_dup_total = 0

    for npy_f in all_npy_files:
        data_path = os.path.join(local_dir, npy_f)

        # Chỉ đọc header để lấy số vector, không load toàn bộ RAM
        n_vectors = np.load(data_path, mmap_mode="r").shape[0]

        pkl_f = npy_f.replace(".npy", ".pkl")
        pkl_path = os.path.join(local_dir, pkl_f)

        if not os.path.exists(pkl_path):
            raise FileNotFoundError(f"Không tìm thấy index pkl tương ứng với {npy_f}: {pkl_path}")

        with open(pkl_path, "rb") as f_in:
            chunk_idx = pickle.load(f_in)

        unique_items = []
        global_duplicates = 0

        for key, local_idx in chunk_idx.items():
            if local_idx < 0 or local_idx >= n_vectors:
                raise ValueError(
                    f"Index out of range trong {pkl_f}: "
                    f"{key} -> {local_idx}, n_vectors={n_vectors}"
                )

            # Nếu cùng ID đã xuất hiện ở chunk trước, bỏ bản sau
            if key in seen_global:
                global_duplicates += 1
                continue

            seen_global.add(key)
            unique_items.append((key, local_idx))

        # Vector mồ côi = vector có trong .npy nhưng không còn key trong dict pkl
        # Thường do duplicate ID trong cùng chunk làm dict ghi đè.
        orphan_count = n_vectors - len(chunk_idx)

        orphan_total += orphan_count
        global_dup_total += global_duplicates
        total_rows += len(unique_items)

        logger.info(
            f"[CHECK] {npy_f}: "
            f"vectors={n_vectors:,}, "
            f"chunk_index={len(chunk_idx):,}, "
            f"orphan_vectors={orphan_count:,}, "
            f"kept_unique={len(unique_items):,}, "
            f"global_duplicates={global_duplicates:,}"
        )

        file_info.append((npy_f, unique_items))

    logger.info(
        f"Tổng hợp {len(all_npy_files)} mảnh. "
        f"Final unique vectors={total_rows:,}, "
        f"orphan_vectors={orphan_total:,}, "
        f"global_duplicates={global_dup_total:,}"
    )

    if total_rows == 0:
        raise ValueError("Không có vector hợp lệ nào để merge.")

    # 3. Tạo file .npy đúng chuẩn, RAM-safe
    fp = np.lib.format.open_memmap(
        final_npy_path,
        dtype="float32",
        mode="w+",
        shape=(total_rows, 768)
    )

    final_index = {}
    curr_offset = 0

    # 4. Gộp từng vector có index
    for npy_f, unique_items in file_info:
        logger.info(f"Đang gộp {npy_f}: giữ {len(unique_items):,} vector có index...")

        data_path = os.path.join(local_dir, npy_f)
        data = np.load(data_path, mmap_mode="r")

        for key, local_idx in unique_items:
            fp[curr_offset] = data[local_idx]
            final_index[key] = curr_offset
            curr_offset += 1

        pkl_path = os.path.join(local_dir, npy_f.replace(".npy", ".pkl"))

        logger.info(
            f"  -> Lũy kế sau {npy_f}: "
            f"Tổng Vector={curr_offset:,}, Tổng Index={len(final_index):,}"
        )

        if curr_offset != len(final_index):
            raise Exception(f"Merge corruption after chunk {npy_f}")

        # Giải phóng SSD local
        os.remove(data_path)
        os.remove(pkl_path)

    fp.flush()
    del fp

    # 5. Lưu index tổng
    with open(final_pkl_path, "wb") as f_out:
        pickle.dump(final_index, f_out)

    logger.info(
        f"==> Hợp nhất hoàn tất. "
        f"Final Vectors={curr_offset:,}, Final Index={len(final_index):,}"
    )

    if curr_offset != len(final_index):
        raise Exception("Final merge integrity failed. Pipeline aborted.")

    upload_precomputed_data()

def upload_model_checkpoint(local_path):
    gcs_dest = f"{TrainingConfig.GCS_OUTPUT_DIR}/models_checkpoints/"
    subprocess.run(["gsutil", "cp", local_path, gcs_dest], check=True)
