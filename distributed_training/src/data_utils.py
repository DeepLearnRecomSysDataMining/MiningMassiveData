import pickle
import pandas as pd
import numpy as np
import ast
import logging
from config.training_config import TrainingConfig

logger = logging.getLogger("data_utils")

def load_eval_dataset():
    """Tải tập Evaluation (Pickle) để đánh giá."""
    if TrainingConfig.RANK == 0:
        logger.info(f"Loading EVAL dataset from {TrainingConfig.EVAL_PKL_PATH}")
    with open(TrainingConfig.EVAL_PKL_PATH, 'rb') as f:
        return pickle.load(f)

def load_item_nodes_lookup(columns=None):
    """
    Tải Metadata sản phẩm từ Parquet (Item Nodes) một cách tối ưu RAM.
    Mặc định load: product_id, asin, product_name, category, parsed_specs.
    """
    if columns is None:
        columns = ['product_id', 'asin', 'product_name', 'category', 'parsed_specs']
        
    path = TrainingConfig.GCS_ITEM_NODES if TrainingConfig.IS_CLOUD else "data/item_nodes"
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang tải Item Metadata từ: {path} (RAM-Efficient Mode)")
    
    # Đọc Parquet với các cột đã chọn
    df = pd.read_parquet(path, columns=columns)
    
    lookup = {}
    # Sử dụng itertuples() để duyệt nhanh và tiết kiệm RAM hơn iterrows()
    for row in df.itertuples(index=False):
        # Trích xuất dữ liệu (hỗ trợ cả các cột có thể thiếu)
        meta = {
            'text': str(row.product_name) if hasattr(row, 'product_name') and pd.notnull(row.product_name) else "",
            'category': str(row.category) if hasattr(row, 'category') and pd.notnull(row.category) else "other",
            'specs': row.parsed_specs if hasattr(row, 'parsed_specs') else {}
        }
        
        # Ánh xạ theo cả ID và ASIN để tối đa khả năng Join
        if hasattr(row, 'product_id') and row.product_id:
            lookup[row.product_id] = meta
        if hasattr(row, 'asin') and row.asin:
            lookup[row.asin] = meta
            
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Hoàn tất! Đã nạp metadata cho {len(lookup):,} sản phẩm vào RAM.")
    
    del df # Giải phóng dataframe ngay lập tức
    import gc; gc.collect()
    return lookup

def load_interactions_df():
    """Tải lịch sử tương tác (Interactions) để làm dữ liệu Train bằng Memory-Mapping."""
    path = TrainingConfig.GCS_INTERACTIONS if TrainingConfig.IS_CLOUD else "data/all_interactions"
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đang tải Interactions từ: {path} (Memory-Mapping Mode)")
    
    from datasets import load_dataset
    if TrainingConfig.IS_CLOUD:
        # Datasets sẽ tải file về cache và map trực tiếp ổ cứng (0 RAM)
        df = load_dataset('parquet', data_files=f"gs://mining-data-2/output/all_interactions/*.parquet", split='train')
    else:
        df = load_dataset('parquet', data_dir=path, split='train')
    
    if TrainingConfig.RANK == 0:
        logger.info(f"==> Đã nạp {len(df):,} tương tác (RAM tốn xấp xỉ 0GB).")
    return df

def clean_text(val):
    """Làm sạch dữ liệu văn bản từ dataframe."""
    if isinstance(val, list): return " ".join([str(x) for x in val])
    if isinstance(val, str):
        if val.startswith('['):
            try:
                val_list = ast.literal_eval(val)
                if isinstance(val_list, list): return " ".join([str(x) for x in val_list])
            except: pass
        return val
    if pd.isna(val): return ""
    return str(val)

def parse_specs(spec_text):
    """Chuyển đổi chuỗi specs hoặc list specs sang dictionary key-value."""
    specs = {}
    if isinstance(spec_text, list):
        for item in spec_text:
            if '::' in str(item):
                parts = str(item).split('::', 1)
                if len(parts) == 2: specs[parts[0].strip().lower()] = parts[1].strip().lower()
    elif isinstance(spec_text, str) and spec_text.startswith('['):
        try:
            items = ast.literal_eval(spec_text)
            for item in items:
                if '::' in str(item):
                    parts = str(item).split('::', 1)
                    if len(parts) == 2: specs[parts[0].strip().lower()] = parts[1].strip().lower()
        except: pass
    elif isinstance(spec_text, dict):
        return {str(k).lower(): str(v).lower() for k, v in spec_text.items()}
    return specs
