import os
import torch
import numpy as np
import logging
import pyarrow.parquet as pq
from sentence_transformers import SentenceTransformer
from config.training_config import TrainingConfig
import torch.distributed as dist
import time
import pickle
import gc
from datetime import timedelta

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] Rank %(rank)s: %(message)s',
    force=True
)

def precompute_item_embeddings():
    """
    Sử dụng đa GPU để mã hóa sản phẩm thành vector.
    Bản 'Zero-RAM-Merge': Ghi trực tiếp vào file memmap để tránh dồn toa bộ nhớ.
    """
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE
    device = TrainingConfig.DEVICE
    logger = logging.LoggerAdapter(logging.getLogger("precompute"), {'rank': rank})

    # 0. Khởi tạo Distributed
    if world_size > 1 and not dist.is_initialized():
        dist.init_process_group(backend="nccl", timeout=timedelta(minutes=150))

    # 1. Chuẩn bị đường dẫn và File Memmap
    output_path = os.path.join(TrainingConfig.LOCAL_DATA_DIR, "item_embeddings.npy")
    index_path = os.path.join(TrainingConfig.LOCAL_DATA_DIR, "item_index.pkl")
    os.makedirs(TrainingConfig.LOCAL_DATA_DIR, exist_ok=True)
    
    # 2. Đọc Metadata để biết tổng số lượng
    path = TrainingConfig.GCS_ITEM_NODES if TrainingConfig.IS_CLOUD else "data/item_nodes"
    try:
        import gcsfs
        fs = gcsfs.GCSFileSystem() if TrainingConfig.IS_CLOUD else None
        
        # Chỉ Rank 0 khảo sát tổng thể và tạo file trống
        if TrainingConfig.IS_CLOUD:
            all_files = [f"gs://{f}" for f in fs.ls(path) if f.endswith(".parquet")]
        else:
            import glob
            all_files = sorted(glob.glob(os.path.join(path, "*.parquet")))
            
        table = pq.read_table(all_files, columns=['product_id', 'asin', 'full_text'], filesystem=fs)
        total_items = table.num_rows
        
        # Rank 0 tạo file .npy trống (Header + Zeroes)
        if rank == 0:
            logger.info(f"Khởi tạo file memmap 12GB cho {total_items:,} items...")
            # Tạo file npy trống với kích thước chuẩn (mmap_mode='w+')
            dtype = np.float32
            shape = (total_items, 768)
            fp = np.memmap(output_path, dtype=dtype, mode='w+', shape=shape)
            del fp # Đóng file để các Rank khác truy cập
            
        if world_size > 1: dist.barrier()
        
        # 3. Mỗi Rank lấy phần dữ liệu của mình
        chunk_size = (total_items + world_size - 1) // world_size
        start_idx = rank * chunk_size
        end_idx = min(start_idx + chunk_size, total_items)
        
        my_ids = table['product_id'].slice(start_idx, end_idx - start_idx).to_pylist()
        my_asins = table['asin'].slice(start_idx, end_idx - start_idx).to_pylist()
        my_texts = table['full_text'].slice(start_idx, end_idx - start_idx).to_pylist()
        
        # Tạo index tra cứu (Sẽ gộp ở cuối)
        my_local_index = {}
        for i, (p_id, asin) in enumerate(zip(my_ids, my_asins)):
            global_idx = start_idx + i
            if p_id: my_local_index[p_id] = global_idx
            if asin: my_local_index[asin] = global_idx
            
        del table
        gc.collect()
        
    except Exception as e:
        logger.error(f"Lỗi chuẩn bị dữ liệu: {e}")
        raise e

    # 4. Huấn luyện BERT
    logger.info(f"Đang xử lý {len(my_texts):,} items (Vị trí: {start_idx} -> {end_idx})")
    model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    # Chia nhỏ việc encode thành từng cụm 100k để log tiến độ và giải phóng RAM
    sub_batch_size = 100000
    for i in range(0, len(my_texts), sub_batch_size):
        sub_texts = my_texts[i : i + sub_batch_size]
        sub_embeddings = model.encode(sub_texts, batch_size=512, convert_to_numpy=True)
        
        # GHI TRỰC TIẾP VÀO FILE 12GB (Không đợi đến cuối)
        sub_start = start_idx + i
        sub_end = sub_start + len(sub_texts)
        
        # Mở file memmap ở đúng vị trí cần ghi
        fp = np.memmap(output_path, dtype='float32', mode='r+', shape=(total_items, 768))
        fp[sub_start:sub_end] = sub_embeddings
        fp.flush() # Ép ghi xuống đĩa ngay lập tức
        del fp, sub_embeddings
        gc.collect()
        
        logger.info(f"Hoàn tất cụm {i//sub_batch_size + 1}. Tiến độ Rank: {sub_end - start_idx:,}/{len(my_texts):,}")

    # 5. Gộp Index (Chỉ Rank 0 làm)
    # Lưu index cục bộ để Rank 0 đọc
    tmp_idx_path = f"/tmp/idx_rank_{rank}.pkl"
    with open(tmp_idx_path, "wb") as f:
        pickle.dump(my_local_index, f)
        
    if world_size > 1: dist.barrier()
    
    if rank == 0:
        logger.info("Đang hợp nhất danh bạ Index...")
        final_index = {}
        for r in range(world_size):
            with open(f"/tmp/idx_rank_{r}.pkl", "rb") as f:
                final_index.update(pickle.load(f))
        
        with open(index_path, "wb") as f:
            pickle.dump(final_index, f)
            
        logger.info(f"==> XONG! Toàn bộ dữ liệu đã sẵn sàng tại {output_path}")
        
        # ĐẨY LÊN GCS NGAY LẬP TỨC
        try:
            from src.gcs_manager import upload_precomputed_data
            logger.info("Đang bắt đầu upload 12GB dữ liệu lên GCS (Dự kiến 2-5 phút)...")
            upload_precomputed_data()
            logger.info("==> TẤT CẢ ĐÃ ĐƯỢC LƯU AN TOÀN TRÊN GCS!")
        except Exception as e:
            logger.error(f"Lỗi khi upload lên GCS: {e}")

if __name__ == "__main__":
    precompute_item_embeddings()
