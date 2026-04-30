# ============================================================
# distributed_training/data_loader/parquet_loader.py
# Bộ đọc dữ liệu Parquet phân tán cho PyTorch
# ============================================================

import torch
import pandas as pd
from torch.utils.data import Dataset, DataLoader, DistributedSampler

class ParquetDataset(Dataset):
    def __init__(self, parquet_path):
        # Đọc dữ liệu chuẩn hóa từ Spark
        self.df = pd.read_parquet(parquet_path)
        
    def __len__(self):
        return len(self.df)
    
    def __getitem__(self, idx):
        row = self.df.iloc[idx]
        # Trả về query và candidates cho model xử lý
        return {
            "query_text": row["query_text"],
            "candidate_texts": list(row["candidate_texts"]),
            "true_vn_id": row["true_vn_id"]
        }

def get_dist_dataloader(parquet_path, batch_size, world_size, rank):
    dataset = ParquetDataset(parquet_path)
    # DistributedSampler đảm bảo mỗi Node chỉ đọc một phần dữ liệu khác nhau
    sampler = DistributedSampler(
        dataset,
        num_replicas=world_size,
        rank=rank,
        shuffle=True,
        seed=42
    )
    
    loader = DataLoader(
        dataset,
        batch_size=batch_size,
        sampler=sampler,
        num_workers=4,
        pin_memory=True
    )
    return loader
