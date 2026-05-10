import pickle
import pandas as pd
import numpy as np
import ast
import logging
import os
import gc
from config.training_config import TrainingConfig
from datasets import load_dataset

logger = logging.getLogger("data_utils")

class LazyItemLookup:
    """
    Hệ thống tra cứu Metadata:
    - Sử dụng PyArrow Table (Dữ liệu dạng nhị phân, cực nhẹ RAM).
    - Chỉ lưu bảng Index ID -> RowIndex.
    """
    def __init__(self, table, id_to_idx):
        self.table = table
        self.id_to_idx = id_to_idx

    def get(self, item_id, default=None):
        idx = self.id_to_idx.get(item_id)
        if idx is None:
            return default if default is not None else {'text': "", 'full_text': "", 'category': "other"}
        
        # Lấy dữ liệu từ PyArrow Table (tốc độ O(1))
        row_dict = {col: str(self.table[col][idx]) for col in self.table.column_names}
        return {
            'text': row_dict.get('product_name', ''),
            'full_text': row_dict.get('full_text', ''),
            'category': row_dict.get('category', 'other')
        }

def load_eval_dataset():
    """Tải tập Evaluation (Pickle) để đánh giá."""
    path = TrainingConfig.EVAL_PKL_PATH
    if not os.path.exists(path):
        # Fallback cho trường hợp chạy trên Cloud nhưng file chưa tải về /tmp
        path = "data/prepared_data_improved/evaluation_dataset.pkl"
        
    if TrainingConfig.RANK == 0:
        logger.info(f"Loading EVAL dataset from {path}")
        
    with open(path, 'rb') as f:
        return pickle.load(f)

def load_item_nodes_lookup():
    """
    Tải Metadata sản phẩm sử dụng PyArrow (Tránh lỗi Map type và tiết kiệm RAM).
    """
    import pyarrow.parquet as pq
    import gcsfs
    
    path = TrainingConfig.GCS_ITEM_NODES if TrainingConfig.IS_CLOUD else "data/item_nodes"
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang tải Item Metadata qua PyArrow từ: {path}")

    # 1. Chỉ đọc các cột cần thiết (Bỏ qua hoàn toàn parsed_specs để tránh lỗi)
    target_cols = ['product_id', 'asin', 'product_name', 'full_text', 'category']
    
    fs = gcsfs.GCSFileSystem() if TrainingConfig.IS_CLOUD else None
    
    # Đọc trực tiếp thành Table (Nhẹ hơn nhiều so với Pandas DataFrame)
    arrow_path = path.replace("gs://", "") if TrainingConfig.IS_CLOUD else path
    table = pq.read_table(arrow_path, columns=target_cols, filesystem=fs)

    # 2. Xây dựng Index
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang xây dựng bảng chỉ mục cho {table.num_rows:,} sản phẩm...")
    
    id_to_idx = {}
    p_ids = table['product_id'].to_pylist()
    asins = table['asin'].to_pylist()
    
    for idx, (p_id, asin) in enumerate(zip(p_ids, asins)):
        if p_id: id_to_idx[p_id] = idx
        if asin: id_to_idx[asin] = idx

    return LazyItemLookup(table, id_to_idx)

def load_interactions_df():
    """Tải lịch sử tương tác (Interactions) bằng Memory-Mapping."""
    path = TrainingConfig.GCS_INTERACTIONS if TrainingConfig.IS_CLOUD else "data/all_interactions"
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang tải Interactions từ: {path} (Memory-Mapping Mode)")
    
    if TrainingConfig.IS_CLOUD:
        # Chỉ lấy 2 cột cần thiết nhất để huấn luyện cơ bản
        df = load_dataset('parquet', data_files=f"{path}/*.parquet", split='train', columns=['asin', 'product_id'])
    else:
        df = load_dataset('parquet', data_dir=path, split='train', columns=['asin', 'product_id'])
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đã nạp {len(df):,} tương tác.")
    return df
