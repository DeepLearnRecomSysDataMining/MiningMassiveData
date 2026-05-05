import pandas as pd
import pickle
import os
import logging
from config.training_config import TrainingConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("prepare_eval_pkl")

def prepare_evaluation_pickle():
    """
    Gộp dữ liệu ID-Only từ Spark Evaluation với Metadata từ Item Nodes
    để tạo file .pkl tương thích với các model training (Baseline 1-6).
    """
    # 1. Đường dẫn (Cấu hình từ TrainingConfig)
    eval_parquet_path = TrainingConfig.GCS_PREPARED_DATA.replace("prepared_data_improved", "evaluation_dataset")
    item_nodes_path = TrainingConfig.GCS_ITEM_NODES
    output_pkl = TrainingConfig.EVAL_PKL_PATH
    
    # Nếu chạy local, bạn có thể thay đổi path trỏ vào thư mục data local
    if not TrainingConfig.IS_CLOUD:
        # Giả sử bạn đã tải data về thư mục data/
        eval_parquet_path = "data/evaluation_dataset"
        item_nodes_path = "data/item_nodes"
    
    logger.info(f"Đang đọc Item Nodes từ: {item_nodes_path}")
    item_df = pd.read_parquet(item_nodes_path)
    
    # Xây dựng lookup dictionary để truy xuất nhanh
    # Key: product_id (VN) hoặc asin (Amazon)
    # Value: {text, category}
    logger.info("Đang xây dựng lookup dictionary cho metadata...")
    lookup = {}
    for _, row in item_df.iterrows():
        meta = {
            'text': row.get('product_name', ''),
            'category': row.get('category', 'other')
        }
        # Lưu theo product_id
        if row.get('product_id'):
            lookup[row['product_id']] = meta
        # Lưu theo asin (quan trọng cho Amazon query)
        if row.get('asin'):
            lookup[row['asin']] = meta

    logger.info(f"Đã load metadata cho {len(lookup)} items.")

    logger.info(f"Đang đọc Evaluation ID-Only từ: {eval_parquet_path}")
    eval_df = pd.read_parquet(eval_parquet_path)
    
    enriched_data = []
    logger.info(f"Bắt đầu gộp metadata cho {len(eval_df)} queries...")
    
    for _, row in eval_df.iterrows():
        q_id = row['query_id']
        cand_ids = row['candidate_ids']
        labels = row['labels']
        
        # Lấy metadata cho Query (Amazon)
        q_meta = lookup.get(q_id, {'text': f"Unknown Query {q_id}", 'category': 'other'})
        
        cand_texts = []
        cand_categories = []
        true_vn_id = None
        
        # Lấy metadata cho từng Candidate (VN)
        for i, cid in enumerate(cand_ids):
            c_meta = lookup.get(cid, {'text': f"Unknown Candidate {cid}", 'category': 'other'})
            cand_texts.append(c_meta['text'])
            cand_categories.append(c_meta['category'])
            
            # Xác định sản phẩm đúng (Positive)
            if labels[i] == 1:
                true_vn_id = cid
        
        # Nếu không tìm thấy true_vn_id (label 1), query này không hợp lệ cho evaluation
        if true_vn_id is None:
            continue
            
        enriched_data.append({
            'query_id': q_id,
            'query_text': q_meta['text'],
            'query_category': q_meta['category'],
            'candidate_ids': list(cand_ids),
            'candidate_texts': cand_texts,
            'candidate_categories': cand_categories,
            'true_vn_id': true_vn_id
        })

    logger.info(f"Hoàn tất gộp. Giữ lại {len(enriched_data)} queries hợp lệ.")
    
    # 4. Lưu ra file Pickle
    os.makedirs(os.path.dirname(output_pkl), exist_ok=True)
    logger.info(f"Đang lưu kết quả ra: {output_pkl}")
    with open(output_pkl, 'wb') as f:
        pickle.dump(enriched_data, f)
    
    logger.info("XONG!")

if __name__ == "__main__":
    prepare_evaluation_pickle()
