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

# Cấu hình logging chuyên sâu
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] Rank %(rank)s: %(message)s',
    force=True
)

def precompute_item_embeddings():
    """
    Sử dụng đa GPU để mã hóa sản phẩm thành vector.
    Bản 'Super Safe' chống treo, tiết kiệm RAM và tự dọn rác.
    """
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE
    device = TrainingConfig.DEVICE
    
    # Custom logger để hiện Rank trong mỗi dòng log
    logger = logging.LoggerAdapter(logging.getLogger("precompute"), {'rank': rank})

    # 0. Khởi tạo Distributed với TIMEOUT để chống treo vô hạn
    if world_size > 1 and not dist.is_initialized():
        logger.info(f"Đang khởi tạo Process Group (backend=nccl, timeout=120m)...")
        try:
            dist.init_process_group(
                backend="nccl", 
                timeout=timedelta(minutes=120) 
            )
            logger.info("Khởi tạo DDP thành công.")
        except Exception as e:
            logger.error(f"Lỗi khởi tạo DDP: {e}")
            raise e

    # 1. Dọn dẹp và chuẩn bị thư mục tạm
    tmp_dir = "/tmp/embeddings_chunks"
    if rank == 0:
        if os.path.exists(tmp_dir):
            import shutil
            logger.info(f"Dọn dẹp dữ liệu rác từ lần chạy trước tại: {tmp_dir}")
            shutil.rmtree(tmp_dir)
        os.makedirs(tmp_dir, exist_ok=True)

    # Đợi Rank 0 dọn dẹp xong mới cho các Rank khác chạy tiếp
    if world_size > 1: dist.barrier()

    # 2. Nạp dữ liệu phân tán (Chỉ nạp file được giao cho mỗi Rank)
    path = TrainingConfig.GCS_ITEM_NODES if TrainingConfig.IS_CLOUD else "data/item_nodes"
    target_cols = ['product_id', 'asin', 'full_text']
    
    logger.info(f"Đang khảo sát thư mục dữ liệu: {path}...")
    
    try:
        import gcsfs
        fs = gcsfs.GCSFileSystem() if TrainingConfig.IS_CLOUD else None
        
        # Liệt kê tất cả các file parquet
        if TrainingConfig.IS_CLOUD:
            # gs.ls trả về đường dẫn không có gs://, cần thêm vào
            all_files = [f"gs://{f}" for f in fs.ls(path) if f.endswith(".parquet")]
        else:
            import glob
            all_files = sorted(glob.glob(os.path.join(path, "*.parquet")))
        
        if not all_files:
            raise FileNotFoundError(f"Không tìm thấy file parquet nào tại {path}")

        # Chia danh sách file cho từng GPU
        num_files = len(all_files)
        files_per_rank = (num_files + world_size - 1) // world_size
        my_files = all_files[rank * files_per_rank : (rank + 1) * files_per_rank]
        
        logger.info(f"Tổng: {num_files} files. Rank {rank} nhận xử lý {len(my_files)} files.")
        
        if not my_files:
            logger.warning(f"Rank {rank} không có file nào để xử lý!")
            my_ids, my_asins, my_texts = [], [], []
        else:
            # Chi tiết từng file để debug
            for f in my_files:
                logger.info(f"Processing file: {os.path.basename(f)}")
                
            # CHỈ ĐỌC NHỮNG FILE THUỘC VỀ MÌNH
            table = pq.read_table(my_files, columns=target_cols, filesystem=fs)
            
            my_ids = table['product_id'].to_pylist()
            my_asins = table['asin'].to_pylist()
            my_texts = table['full_text'].to_pylist()
            
            # GIẢI PHÓNG RAM TABLE NGAY LẬP TỨC
            del table
            gc.collect()
        
        logger.info(f"Nạp xong {len(my_texts):,} items từ {len(my_files)} files.")
    except Exception as e:
        logger.error(f"Lỗi nạp dữ liệu phân tán: {e}")
        raise e
    
    # 3. Khởi tạo Encoder và Chạy Encoding
    logger.info(f"Đang nạp model SentenceTransformer lên {device}...")
    model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    logger.info(">>> BẮT ĐẦU ENCODING (Dự kiến 45-60 phút) <<<")
    t0 = time.time()
    
    embeddings = model.encode(
        my_texts, 
        batch_size=512, 
        show_progress_bar=(rank == 0), 
        convert_to_numpy=True
    )
    
    duration = time.time() - t0
    logger.info(f"<<< ENCODING XONG! Thời gian: {duration/60:.1f} phút. Tốc độ: {len(my_texts)/duration:.1f} items/s")
    
    # 4. Lưu kết quả tạm thời
    emb_file = f"{tmp_dir}/emb_rank_{rank}.npy"
    logger.info(f"Đang lưu chunk vào {emb_file}...")
    np.save(emb_file, embeddings)
    
    with open(f"{tmp_dir}/ids_rank_{rank}.pkl", "wb") as f:
        pickle.dump({'ids': my_ids, 'asins': my_asins}, f)
    
    # GIẢI PHÓNG RAM MODEL VÀ VECTOR TRƯỚC KHI GOM FILE
    del embeddings
    del model
    gc.collect()
    torch.cuda.empty_cache()
        
    # 5. Đợi các GPU khác hội quân (Barrier)
    if world_size > 1:
        logger.info("Đang đợi các GPU khác hoàn tất tại Barrier...")
        try:
            dist.barrier()
            logger.info("Tất cả các Rank đã hội quân thành công.")
        except Exception as e:
            logger.error(f"Lỗi Barrier (Có thể 1 GPU bị kẹt/die): {e}")
            raise e
        
    # 6. Rank 0 Hợp nhất dữ liệu
    if rank == 0:
        logger.info("Rank 0: Bắt đầu tiến trình Hợp nhất dữ liệu (Consolidation)...")
        all_embs = []
        id_to_idx = {}
        curr_idx = 0
        
        for r in range(world_size):
            logger.info(f"Đang nạp chunk từ Rank {r}...")
            emb_chunk = np.load(f"{tmp_dir}/emb_rank_{r}.npy")
            with open(f"{tmp_dir}/ids_rank_{r}.pkl", "rb") as f:
                data = pickle.load(f)
            
            all_embs.append(emb_chunk)
            for p_id, asin in zip(data['ids'], data['asins']):
                if p_id: id_to_idx[p_id] = curr_idx
                if asin: id_to_idx[asin] = curr_idx
                curr_idx += 1
            
            # Xóa file tạm ngay để giải phóng Disk (Cực kỳ quan trọng)
            os.remove(f"{tmp_dir}/emb_rank_{r}.npy")
            os.remove(f"{tmp_dir}/ids_rank_{r}.pkl")
        
        logger.info("Rank 0: Đang vstack tất cả vector...")
        final_embs = np.vstack(all_embs)
        
        output_path = os.path.join(TrainingConfig.LOCAL_DATA_DIR, "item_embeddings.npy")
        index_path = os.path.join(TrainingConfig.LOCAL_DATA_DIR, "item_index.pkl")
        
        os.makedirs(TrainingConfig.LOCAL_DATA_DIR, exist_ok=True)
        
        logger.info(f"Rank 0: Đang ghi file cuối cùng (12GB+) ra {output_path}...")
        np.save(output_path, final_embs)
        
        with open(index_path, "wb") as f:
            pickle.dump(id_to_idx, f)
            
        logger.info(f"==> XONG! Tổng cộng: {final_embs.shape[0]:,} vector.")
        logger.info(f"==> Path: {output_path}")

if __name__ == "__main__":
    precompute_item_embeddings()
