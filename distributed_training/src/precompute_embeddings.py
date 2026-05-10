import os
import torch
import numpy as np
import logging
import pyarrow.parquet as pq
from sentence_transformers import SentenceTransformer
from config.training_config import TrainingConfig
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("precompute")

def precompute_item_embeddings():
    """
    Sử dụng 4 GPU để mã hóa 4 triệu sản phẩm thành vector (mất ~1 tiếng).
    Lưu kết quả thành file .npy để huấn luyện siêu tốc.
    """
    device = TrainingConfig.DEVICE
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE
    
    # 1. Load toàn bộ danh sách sản phẩm (Chỉ lấy ID và Văn bản)
    path = TrainingConfig.GCS_ITEM_NODES if TrainingConfig.IS_CLOUD else "data/item_nodes"
    target_cols = ['product_id', 'asin', 'full_text']
    
    logger.info(f"Rank {rank} đang nạp dữ liệu từ {path}...")
    import gcsfs
    fs = gcsfs.GCSFileSystem() if TrainingConfig.IS_CLOUD else None
    arrow_path = path.replace("gs://", "") if TrainingConfig.IS_CLOUD else path
    table = pq.read_table(arrow_path, columns=target_cols, filesystem=fs)
    
    # 2. Chia mảnh dữ liệu cho từng GPU
    total_items = table.num_rows
    chunk_size = (total_items + world_size - 1) // world_size
    start_idx = rank * chunk_size
    end_idx = min(start_idx + chunk_size, total_items)
    
    my_ids = table['product_id'].slice(start_idx, end_idx - start_idx).to_pylist()
    my_asins = table['asin'].slice(start_idx, end_idx - start_idx).to_pylist()
    my_texts = table['full_text'].slice(start_idx, end_idx - start_idx).to_pylist()
    
    logger.info(f"Rank {rank}: Xử lý từ {start_idx:,} đến {end_idx:,} ({len(my_texts):,} items)")
    
    # 3. Khởi tạo Encoder trên GPU tương ứng
    model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    # 4. Chạy Encoding theo Batch lớn
    embeddings = model.encode(
        my_texts, 
        batch_size=512, 
        show_progress_bar=(rank == 0), 
        convert_to_numpy=True
    )
    
    # 5. Lưu kết quả tạm thời của từng Rank
    tmp_dir = "/tmp/embeddings_chunks"
    os.makedirs(tmp_dir, exist_ok=True)
    np.save(f"{tmp_dir}/emb_rank_{rank}.npy", embeddings)
    with open(f"{tmp_dir}/ids_rank_{rank}.pkl", "wb") as f:
        import pickle
        pickle.dump({'ids': my_ids, 'asins': my_asins}, f)
        
    # Chờ tất cả các GPU xong việc
    if world_size > 1:
        torch.distributed.init_process_group(backend="nccl")
        torch.distributed.barrier()
        
    # 6. Rank 0 gom tất cả lại thành 1 file duy nhất (12GB)
    if rank == 0:
        logger.info("Đang gom các mảnh vector thành file tổng duy nhất...")
        all_embs = []
        id_to_idx = {}
        curr_idx = 0
        
        for r in range(world_size):
            emb_chunk = np.load(f"{tmp_dir}/emb_rank_{r}.npy")
            with open(f"{tmp_dir}/ids_rank_{r}.pkl", "rb") as f:
                data = pickle.load(f)
            
            all_embs.append(emb_chunk)
            for p_id, asin in zip(data['ids'], data['asins']):
                if p_id: id_to_idx[p_id] = curr_idx
                if asin: id_to_idx[asin] = curr_idx
                curr_idx += 1
        
        final_embs = np.vstack(all_embs)
        output_path = os.path.join(TrainingConfig.LOCAL_DATA_DIR, "item_embeddings.npy")
        index_path = os.path.join(TrainingConfig.LOCAL_DATA_DIR, "item_index.pkl")
        
        np.save(output_path, final_embs)
        with open(index_path, "wb") as f:
            pickle.dump(id_to_idx, f)
            
        logger.info(f"==> HOÀN TẤT! Đã lưu 12GB vector tại: {output_path}")
        logger.info(f"==> File Index tra cứu tại: {index_path}")

if __name__ == "__main__":
    precompute_item_embeddings()
