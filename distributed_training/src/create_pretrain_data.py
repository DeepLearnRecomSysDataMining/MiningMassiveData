import pandas as pd
import random
import logging
import os
from config.training_config import TrainingConfig

logger = logging.getLogger("pretrain_data")

def create_amazon_triplets(n_samples=10000):
    """
    Tạo tập dữ liệu Tự Giám Sát (Self-Supervised) từ Amazon Item Nodes.
    Chiến thuật:
    - Anchor: Một sản phẩm ngẫu nhiên.
    - Positive: Cùng category.
    - Negative: Khác category.
    """
    logger.info(f"Loading item nodes to create {n_samples} Amazon triplets...")
    df = pd.read_parquet(TrainingConfig.GCS_ITEM_NODES)
    df_amz = df[df['domain'] == 'amazon']
    
    # Gom nhóm theo category để lấy positive nhanh
    cat_groups = df_amz.groupby('category')['asin'].apply(list).to_dict()
    all_asins = df_amz['asin'].tolist()
    
    triplets = []
    for _ in range(n_samples):
        # 1. Chọn Anchor
        anchor = df_amz.sample(1).iloc[0]
        a_id, a_cat = anchor['asin'], anchor['category']
        
        # 2. Chọn Positive (Cùng category, khác ID)
        pos_candidates = cat_groups.get(a_cat, [])
        if len(pos_candidates) > 1:
            p_id = random.choice([x for x in pos_candidates if x != a_id])
        else:
            continue # Bỏ qua nếu category chỉ có 1 sp
            
        # 3. Chọn Negative (Khác category)
        # Cách đơn giản: lấy ngẫu nhiên 1 sp khác
        n_id = random.choice(all_asins)
        
        triplets.append({'anchor': a_id, 'positive': p_id, 'negative': n_id})
        
    # Lưu kết quả
    output_path = os.path.join(TrainingConfig.LOCAL_DATA_DIR, "amazon_pretrain_triplets.parquet")
    pd.DataFrame(triplets).to_parquet(output_path)
    logger.info(f"Created and saved {len(triplets)} triplets to {output_path}")
    return output_path
