import pandas as pd
import pickle
import os
import logging
import gc
from config.training_config import TrainingConfig
import time

# Cấu hình logging giống style Spark
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("prepare_eval_pkl_v2")

def prepare_evaluation_pickle_optimized():
    """
    PHIÊN BẢN V2 (OPTIMIZED): Tránh tràn RAM bằng kỹ thuật ID-Filtering.
    Học tập từ tư duy 'Selective Processing' của Spark V2.
    """
    t_start = time.time()
    logger.info("============================================================")
    logger.info("   BẮT ĐẦU QUY TRÌNH CHUẨN BỊ EVALUATION DATASET (.PKL)")
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
    logger.info("[BƯỚC 2] Đang đọc danh sách ID từ bộ Evaluation...")
    eval_df = pd.read_parquet(eval_parquet_path)
    
    # Thu thập tất cả ID cần thiết để filter (Tránh load 100% item_nodes)
    needed_query_ids = set(eval_df['query_id'].unique())
    needed_cand_ids = set([item for sublist in eval_df['candidate_ids'] for item in sublist])
    all_needed_ids = needed_query_ids.union(needed_cand_ids)
    
    logger.info(f" -> Tìm thấy {len(eval_df):,} queries.")
    logger.info(f" -> Cần truy xuất metadata cho {len(all_needed_ids):,} sản phẩm duy nhất.")

    # --- BƯỚC 3: ĐỌC METADATA ---
    logger.info(f"[BƯỚC 3] Đang lọc Metadata từ kho 4 triệu sản phẩm (CPU Processing)...")
    t3 = time.time()
    
    target_columns = ['product_id', 'asin', 'product_name', 'category', 'parsed_specs']
    item_df = pd.read_parquet(item_nodes_path, columns=target_columns)
    
    # Lọc
    item_df = item_df[item_df['product_id'].isin(all_needed_ids) | item_df['asin'].isin(all_needed_ids)]
    
    logger.info(f" -> Lọc thành công {len(item_df):,} items metadata hợp lệ.")
    logger.info(f" -> Thời gian đọc & lọc: {time.time()-t3:.1f}s")

    # --- BƯỚC 4: XÂY DỰNG LOOKUP ---
    logger.info("[BƯỚC 4] Đang xây dựng bản đồ tra cứu (Lookup Map)...")
    lookup = {}
    for _, row in item_df.iterrows():
        specs = row['parsed_specs']
        if specs is None: specs = {}
        elif isinstance(specs, str):
            try: 
                import json
                specs = json.loads(specs.replace("'", '"')) 
            except: specs = {}

        meta = {
            'text': str(row['product_name']) if pd.notnull(row['product_name']) else "",
            'name': str(row['product_name']) if pd.notnull(row['product_name']) else "",
            'category': str(row['category']) if pd.notnull(row['category']) else "other",
            'specs': specs
        }
        if row['product_id']: lookup[row['product_id']] = meta
        if row['asin']: lookup[row['asin']] = meta

    del item_df
    gc.collect()

    # --- BƯỚC 5: ENRICH DATA ---
    logger.info("[BƯỚC 5] Đang gộp Metadata vào bộ Evaluation (Enriching)...")
    enriched_data = []
    
    for i, (_, row) in enumerate(eval_df.iterrows()):
        if (i+1) % 50 == 0:
            logger.info(f" -> Đã xử lý {i+1}/{len(eval_df)} queries...")
            
        q_id = row['query_id']
        cand_ids = row['candidate_ids']
        labels = row['labels']
        
        q_meta = lookup.get(q_id, {'text': "", 'name': "", 'category': 'other', 'specs': {}})
        
        cand_texts, cand_categories, cand_specs = [], [], []
        true_vn_id = None
        
        for idx, cid in enumerate(cand_ids):
            c_meta = lookup.get(cid, {'text': f"Unknown {cid}", 'name': "", 'category': 'other', 'specs': {}})
            cand_texts.append(c_meta['text'])
            cand_categories.append(c_meta['category'])
            cand_specs.append(c_meta['specs'])
            if labels[idx] == 1: true_vn_id = cid
        
        if true_vn_id:
            enriched_data.append({
                'query_id': q_id, 'query_text': q_meta['text'], 'query_category': q_meta['category'],
                'query_specs': q_meta['specs'], 'candidate_ids': list(cand_ids),
                'candidate_texts': cand_texts, 'candidate_categories': cand_categories,
                'candidate_specs': cand_specs, 'true_vn_id': true_vn_id
            })

    # --- BƯỚC 6: LƯU KẾT QUẢ ---
    logger.info(f"[BƯỚC 6] Đang lưu {len(enriched_data)} queries ra file Pickle...")
    os.makedirs(os.path.dirname(output_pkl), exist_ok=True)
    with open(output_pkl, 'wb') as f:
        pickle.dump(enriched_data, f)
    
    logger.info(f" -> Đã lưu tại local: {output_pkl}")

    if TrainingConfig.IS_CLOUD:
        try:
            import subprocess
            logger.info(f" -> Đang upload lên GCS: {TrainingConfig.GCS_EVAL_PKL}")
            subprocess.run(["gsutil", "cp", output_pkl, TrainingConfig.GCS_EVAL_PKL], check=True)
            logger.info(" -> Upload GCS THÀNH CÔNG!")
        except Exception as e:
            logger.error(f" -> Lỗi upload GCS: {e}")

    elapsed = time.time() - t_start
    logger.info("============================================================")
    logger.info(f"   HOÀN TẤT TRONG {elapsed:.1f}s | QUERIES: {len(enriched_data):,}")
    logger.info("============================================================")

if __name__ == "__main__":
    prepare_evaluation_pickle_optimized()
