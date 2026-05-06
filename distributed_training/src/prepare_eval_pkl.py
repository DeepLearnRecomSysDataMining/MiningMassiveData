import pandas as pd
import pickle
import os
import logging
import gc
from config.training_config import TrainingConfig

# Cấu hình logging giống style Spark
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("prepare_eval_pkl_v2")

def prepare_evaluation_pickle_optimized():
    """
    PHIÊN BẢN V2 (OPTIMIZED): Tránh tràn RAM bằng kỹ thuật ID-Filtering.
    Học tập từ tư duy 'Selective Processing' của Spark V2.
    """
    # 1. Xác định đường dẫn
    eval_parquet_path = TrainingConfig.GCS_EVAL_PARQUET
    item_nodes_path = TrainingConfig.GCS_ITEM_NODES
    output_pkl = TrainingConfig.EVAL_PKL_PATH
    
    if not TrainingConfig.IS_CLOUD:
        eval_parquet_path = "data/evaluation_dataset"
        item_nodes_path = "data/item_nodes"

    # --- BƯỚC 1: ĐỌC TẬP ID TRƯỚC (RẤT NHẸ) ---
    logger.info(f"Đang đọc Evaluation ID-Only (Step 1): {eval_parquet_path}")
    eval_df = pd.read_parquet(eval_parquet_path)
    
    # Thu thập tất cả ID cần thiết để filter (Tránh load 100% item_nodes)
    needed_query_ids = set(eval_df['query_id'].unique())
    needed_cand_ids = set([item for sublist in eval_df['candidate_ids'] for item in sublist])
    all_needed_ids = needed_query_ids.union(needed_cand_ids)
    
    logger.info(f"Tổng số ID cần metadata: {len(all_needed_ids):,}")

    # --- BƯỚC 2: ĐỌC METADATA CÓ CHỌN LỌC (HỌC TỪ SPARK V2) ---
    logger.info(f"Đang đọc Item Nodes (Step 2) với Filter và Column Selection...")
    
    # Kỹ thuật tối ưu: Chỉ load các cột cần thiết và dùng filter 'isin'
    # Bổ sung thêm cột 'parsed_specs' cho các model Hybrid và CHGNN
    target_columns = ['product_id', 'asin', 'product_name', 'category', 'parsed_specs']
    
    try:
        item_df = pd.read_parquet(
            item_nodes_path,
            columns=target_columns,
            filters=[('product_id', 'in', list(all_needed_ids))]
        )
    except Exception as e:
        logger.warning(f"Filter trực tiếp thất bại ({e}), đang load toàn bộ và filter thủ công...")
        item_df = pd.read_parquet(item_nodes_path, columns=target_columns)
        item_df = item_df[item_df['product_id'].isin(all_needed_ids) | item_df['asin'].isin(all_needed_ids)]

    logger.info(f"Đã load thành công {len(item_df):,} items.")

    # --- BƯỚC 3: XÂY DỰNG LOOKUP DICTIONARY SIÊU NHẸ ---
    lookup = {}
    for _, row in item_df.iterrows():
        # Chuyển đổi specs sang dict nếu nó là chuỗi hoặc None
        specs = row['parsed_specs']
        if specs is None: specs = {}
        elif isinstance(specs, str):
            try: specs = eval(specs) 
            except: specs = {}

        meta = {
            'text': row['product_name'] if pd.notnull(row['product_name']) else "",
            'name': row['product_name'] if pd.notnull(row['product_name']) else "",
            'category': row['category'] if pd.notnull(row['category']) else "other",
            'specs': specs
        }
        if row['product_id']: lookup[row['product_id']] = meta
        if row['asin']: lookup[row['asin']] = meta

    # Giải phóng dataframe trung gian để hồi bộ nhớ
    del item_df
    gc.collect()

    # --- BƯỚC 4: ENRICH DATA ---
    enriched_data = []
    logger.info(f"Bắt đầu gộp metadata cho {len(eval_df)} queries...")
    
    for _, row in eval_df.iterrows():
        q_id = row['query_id']
        cand_ids = row['candidate_ids']
        labels = row['labels']
        
        # Mapping Query (Amazon)
        q_meta = lookup.get(q_id, {'text': "", 'name': "", 'category': 'other', 'specs': {}})
        
        cand_texts = []
        cand_categories = []
        cand_specs = []
        true_vn_id = None
        
        # Mapping Candidates (VN)
        for i, cid in enumerate(cand_ids):
            c_meta = lookup.get(cid, {'text': f"Unknown Candidate {cid}", 'name': "", 'category': 'other', 'specs': {}})
            cand_texts.append(c_meta['text'])
            cand_categories.append(c_meta['category'])
            cand_specs.append(c_meta['specs'])
            
            if labels[i] == 1: true_vn_id = cid
        
        if true_vn_id is None: continue
            
        # Cấu trúc Dict khớp 100% với yêu cầu của Hybrid và CHGNN model
        enriched_data.append({
            'query_id': q_id,
            'query_text': q_meta['text'],
            'query_name': q_meta['name'],
            'query_category': q_meta['category'],
            'query_specs': q_meta['specs'],
            'candidate_ids': list(cand_ids),
            'candidate_texts': cand_texts,
            'candidate_categories': cand_categories,
            'candidate_specs': cand_specs,
            'true_vn_id': true_vn_id
        })

    # --- BƯỚC 5: LƯU KẾT QUẢ ---
    os.makedirs(os.path.dirname(output_pkl), exist_ok=True)
    logger.info(f"Đang lưu {len(enriched_data)} queries ra file Pickle tại local: {output_pkl}")
    with open(output_pkl, 'wb') as f:
        pickle.dump(enriched_data, f)
    
    # --- BƯỚC 6: UPLOAD LÊN GCS (DÀNH CHO CLOUD) ---
    if TrainingConfig.IS_CLOUD:
        try:
            import subprocess
            gcs_dest = TrainingConfig.GCS_EVAL_PKL
            logger.info(f"Đang upload file lên GCS: {gcs_dest}")
            subprocess.run(["gsutil", "cp", output_pkl, gcs_dest], check=True)
            logger.info("Upload GCS THÀNH CÔNG!")
        except Exception as e:
            logger.error(f"Lỗi khi upload lên GCS: {e}")

    logger.info("HOÀN TẤT TẠI CHỖ (V2-OPTIMIZED)!")

if __name__ == "__main__":
    prepare_evaluation_pickle_optimized()
