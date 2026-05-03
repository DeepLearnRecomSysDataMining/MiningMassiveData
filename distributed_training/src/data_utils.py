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

def load_item_nodes_lookup():
    """Tải Metadata sản phẩm từ Parquet (Item Nodes) để lấy Text/Specs."""
    if TrainingConfig.RANK == 0:
        logger.info(f"Loading Item Nodes lookup from {TrainingConfig.GCS_ITEM_NODES}...")
    
    # Sử dụng pandas để đọc Parquet từ GCS
    df = pd.read_parquet(TrainingConfig.GCS_ITEM_NODES)
    lookup = df.set_index('asin')[['product_name', 'full_text', 'category']].to_dict('index')
    
    if TrainingConfig.RANK == 0:
        logger.info(f"Loaded metadata for {len(lookup)} items.")
    return lookup

def load_interactions_df():
    """Tải lịch sử tương tác (Interactions) để làm dữ liệu Train."""
    if TrainingConfig.RANK == 0:
        logger.info(f"Loading Interactions from {TrainingConfig.GCS_INTERACTIONS}...")
    
    df = pd.read_parquet(TrainingConfig.GCS_INTERACTIONS)
    
    if TrainingConfig.RANK == 0:
        logger.info(f"Loaded {len(df)} interactions.")
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
