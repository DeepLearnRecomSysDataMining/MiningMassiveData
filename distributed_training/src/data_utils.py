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
    Hệ thống tra cứu Metadata thông minh: 
    - Giữ dữ liệu trên ổ cứng (Memory-Mapping).
    - Chỉ giữ bảng Index ID -> RowIndex trong RAM.
    - Tiết kiệm 99% RAM so với dùng Dictionary thông thường.
    """
    def __init__(self, dataset, id_to_idx):
        self.dataset = dataset
        self.id_to_idx = id_to_idx

    def get(self, item_id, default=None):
        idx = self.id_to_idx.get(item_id)
        if idx is None:
            return default if default is not None else {'text': "", 'full_text': "", 'category': "other"}
        
        # Bốc dữ liệu từ ổ cứng (Cực nhanh nhờ memory-mapping)
        row = self.dataset[idx]
        return {
            'text': str(row.get('product_name', '')),
            'full_text': str(row.get('full_text', '')),
            'category': str(row.get('category', 'other'))
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
    Tải Metadata sản phẩm sử dụng Memory-Mapping để tiết kiệm RAM tối đa cho DDP.
    """
    path = TrainingConfig.GCS_ITEM_NODES if TrainingConfig.IS_CLOUD else "data/item_nodes"
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang khởi tạo Lazy Item Lookup từ: {path}")

    # 1. Mở dataset bằng HF Datasets (0 RAM)
    # TỐI ƯU: Loại bỏ parsed_specs để tránh lỗi kiểu Map và tiết kiệm RAM
    target_cols = ['product_id', 'asin', 'product_name', 'full_text', 'category']
    
    if TrainingConfig.IS_CLOUD:
        # Lưu ý: HF Datasets tự động handle gs:// và cache về local disk
        item_ds = load_dataset('parquet', data_files=f"{path}/*.parquet", split='train', columns=target_cols)
    else:
        item_ds = load_dataset('parquet', data_dir=path, split='train', columns=target_cols)

    # 2. Xây dựng bảng Index (Chỉ tốn vài trăm MB RAM thay vì hàng chục GB)
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang xây dựng bảng chỉ mục cho {len(item_ds):,} sản phẩm...")
    
    id_to_idx = {}
    # Lấy nhanh 2 cột ID để build index
    p_ids = item_ds['product_id']
    asins = item_ds['asin']
    
    for idx, (p_id, asin) in enumerate(zip(p_ids, asins)):
        if p_id: id_to_idx[p_id] = idx
        if asin: id_to_idx[asin] = idx

    if TrainingConfig.RANK == 0:
        logger.info(f"==> Hoàn tất! Index size: {len(id_to_idx):,} IDs.")
    
    return LazyItemLookup(item_ds, id_to_idx)

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
