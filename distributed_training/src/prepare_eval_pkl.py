import pandas as pd
import pickle
import os
import logging
import gc
import time
from config.training_config import TrainingConfig
from datasets import load_dataset # Lazy Loading để tiết kiệm RAM
from tqdm import tqdm

# Cấu hình logging giống style Spark
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("prepare_eval_pkl_v2")

def prepare_evaluation_pickle_optimized():
    """
    PHIÊN BẢN V2 (OPTIMIZED): Dành riêng cho máy Coordinator 16GB RAM.
    Sử dụng Memory-Mapping của Hugging Face Datasets thay vì Pandas.
    """
    t_start = time.time()
    logger.info("============================================================")
    logger.info("   BẮT ĐẦU CHUẨN BỊ EVALUATION PKL (MEMORY-MAPPING MODE)")
    logger.info("============================================================")

    # 1. Xác định đường dẫn
    mode = "CLOUD (GCS)" if TrainingConfig.IS_CLOUD else "LOCAL"
    logger.info(f"[BƯỚC 1] Chế độ thực thi: {mode}")

    eval_parquet_path = TrainingConfig.GCS_EVAL_PARQUET
    item_nodes_path = TrainingConfig.GCS_ITEM_NODES
    output_pkl = TrainingConfig.EVAL_PKL_PATH
    
    if not TrainingConfig.IS_CLOUD:
        eval_parquet_path = "data/evaluation_dataset"
        item_nodes_path = "data/item_nodes"

    logger.info(f" -> Path Eval: {eval_parquet_path}")
    logger.info(f" -> Path Item: {item_nodes_path}")

    # --- BƯỚC 2: ĐỌC TẬP ID ---
    logger.info("[BƯỚC 2] Đang đọc danh sách ID từ bộ Evaluation (Pandas)...")
    eval_df = pd.read_parquet(eval_parquet_path)
    
    # Thu thập tất cả ID cần thiết để filter (Chỉ tốn vài MB RAM)
    needed_query_ids = set(eval_df['query_id'].unique())
    needed_cand_ids = set([item for sublist in eval_df['candidate_ids'] for item in sublist])
    all_needed_ids = needed_query_ids.union(needed_cand_ids)
    
    logger.info(f" -> Tìm thấy {len(eval_df):,} queries.")
    logger.info(f" -> Cần truy xuất metadata cho {len(all_needed_ids):,} sản phẩm duy nhất.")

    # --- BƯỚC 3: MỞ DATASET (0 RAM) ---
    logger.info(f"[BƯỚC 3] Đang ánh xạ Item Metadata (Hugging Face Datasets)...")
    
    # Chỉ định rõ các cột cần lấy để tránh lỗi với cột 'parsed_specs' (kiểu Map không tương thích)
    target_cols = ['product_id', 'asin', 'product_name', 'full_text']
    
    if TrainingConfig.IS_CLOUD:
        # Tải metadata trực tiếp từ GCS
        item_ds = load_dataset('parquet', data_files=f"{item_nodes_path}/*.parquet", split='train', columns=target_cols)
    else:
        item_ds = load_dataset('parquet', data_dir=item_nodes_path, split='train', columns=target_cols)

    # --- BƯỚC 4: XÂY DỰNG LOOKUP ---
    logger.info("[BƯỚC 4] Đang lọc và xây dựng bản đồ tra cứu (Lookup Map)...")
    lookup = {}
    
    # Duyệt qua từng dòng của file metadata (4 triệu dòng) mà không nạp hết vào RAM
    for row in tqdm(item_ds, desc="Building Lookup", total=len(item_ds)):
        p_id = row.get('product_id')
        asin = row.get('asin')
        
        # Chỉ bốc dữ liệu nếu ID nằm trong tập cần thiết
        if p_id in all_needed_ids or asin in all_needed_ids:
            # Ưu tiên full_text, nếu không có thì lấy product_name
            final_text = row.get('full_text') or row.get('product_name') or ""
            
            meta = {
                'text': str(final_text)
            }
            if p_id: lookup[p_id] = meta
            if asin: lookup[asin] = meta

    # Giải phóng Dataset thô để lấy lại RAM
    del item_ds
    gc.collect()

    # --- BƯỚC 5: GỘP DỮ LIỆU ---
    logger.info("[BƯỚC 5] Đang đóng gói dữ liệu vào bộ Evaluation (Enriching)...")
    enriched_data = []
    
    for i, (_, row) in enumerate(eval_df.iterrows()):
        if (i+1) % 50 == 0:
            logger.info(f" -> Đã xử lý {i+1}/{len(eval_df)} queries...")
            
        q_id = row['query_id']
        cand_ids = row['candidate_ids']
        labels = row['labels']
        
        q_meta = lookup.get(q_id, {'text': ""})
        
        cand_texts = []
        true_vn_id = None
        
        for idx, cid in enumerate(cand_ids):
            c_meta = lookup.get(cid, {'text': ""})
            cand_texts.append(c_meta['text'])
            if labels[idx] == 1: 
                true_vn_id = cid
        
        if true_vn_id:
            enriched_data.append({
                'query_id': q_id, 
                'query_text': q_meta['text'], 
                'candidate_ids': list(cand_ids),
                'candidate_texts': cand_texts, 
                'true_vn_id': true_vn_id
            })

    # --- BƯỚC 6: LƯU KẾT QUẢ ---
    logger.info(f"[BƯỚC 6] Đang lưu {len(enriched_data)} queries ra file Pickle...")
    os.makedirs(os.path.dirname(output_pkl), exist_ok=True)
    with open(output_pkl, 'wb') as f:
        pickle.dump(enriched_data, f)
    
    logger.info(f" -> Đã lưu tại: {output_pkl}")

    if TrainingConfig.IS_CLOUD:
        try:
            import subprocess
            logger.info(f" -> Đang upload lên GCS: {TrainingConfig.GCS_EVAL_PKL}")
            subprocess.run(["gsutil", "cp", output_pkl, TrainingConfig.GCS_EVAL_PKL], check=True)
        except Exception as e:
            logger.error(f" -> Lỗi upload GCS: {e}")

    elapsed = time.time() - t_start
    logger.info("============================================================")
    logger.info(f"   HOÀN TẤT TRONG {elapsed:.1f}s | QUERIES: {len(enriched_data):,}")
    logger.info("============================================================")

if __name__ == "__main__":
    prepare_evaluation_pickle_optimized()
