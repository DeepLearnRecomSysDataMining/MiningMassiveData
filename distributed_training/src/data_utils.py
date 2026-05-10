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

class PrecomputedEmbeddingLookup:
    """
    Hệ thống tra cứu Vector siêu tốc:
    - Sử dụng Memory-Mapping để nạp file 12GB mà không tốn RAM.
    - Trả về trực tiếp vector 768 chiều.
    """
    def __init__(self, embeddings_npy, id_to_idx):
        self.embeddings = embeddings_npy # Đây là mmap array
        self.id_to_idx = id_to_idx
        self.dim = embeddings_npy.shape[1]

    def get_embedding(self, item_id):
        idx = self.id_to_idx.get(item_id)
        if idx is None:
            return np.zeros(self.dim, dtype=np.float32)
        return self.embeddings[idx]

def load_precomputed_embeddings():
    """
    Nạp dữ liệu vector đã tính toán trước.
    Dùng mmap_mode='r' để không làm tràn RAM máy ảo.
    """
    emb_path = TrainingConfig.ITEM_EMBEDDINGS_PATH
    idx_path = TrainingConfig.ITEM_INDEX_PATH
    
    if not os.path.exists(emb_path) or not os.path.exists(idx_path):
        logger.error("KHÔNG TÌM THẤY DỮ LIỆU PRECOMPUTED! Vui lòng chạy src/precompute_embeddings.py trước.")
        return None
        
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang nạp Precomputed Embeddings từ: {emb_path}")
        
    # Nạp index tra cứu
    with open(idx_path, "rb") as f:
        id_to_idx = pickle.load(f)
        
    # Nạp vector bằng Memory-Mapping (Cực kỳ tiết kiệm RAM)
    embeddings = np.load(emb_path, mmap_mode='r')
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Hoàn tất! Đã sẵn sàng tra cứu {len(id_to_idx):,} sản phẩm (Tốc độ O(1)).")
        
    return PrecomputedEmbeddingLookup(embeddings, id_to_idx)

def load_eval_dataset():
    """Tải tập Evaluation (Pickle) để đánh giá."""
    path = TrainingConfig.EVAL_PKL_PATH
    if not os.path.exists(path):
        path = "data/prepared_data_improved/evaluation_dataset.pkl"
    with open(path, 'rb') as f:
        return pickle.load(f)

def load_interactions_df():
    """Tải lịch sử tương tác (Interactions) với tỷ lệ chỉ định."""
    path = TrainingConfig.GCS_INTERACTIONS if TrainingConfig.IS_CLOUD else "data/all_interactions"
    fraction = int(TrainingConfig.DATA_FRACTION * 100)
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang tải {fraction}% Interactions từ: {path}")
    
    # Sử dụng tính năng cắt (Slicing) của HF Datasets: 'train[:25%]'
    split_str = f"train[:{fraction}%]"
    
    if TrainingConfig.IS_CLOUD:
        df = load_dataset('parquet', data_files=f"{path}/*.parquet", split=split_str, columns=['asin', 'product_id'])
    else:
        df = load_dataset('parquet', data_dir=path, split=split_str, columns=['asin', 'product_id'])
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đã nạp {len(df):,} tương tác ({fraction}% tổng số).")
    return df
