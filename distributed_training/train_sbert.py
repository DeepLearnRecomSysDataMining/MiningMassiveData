# ============================================================
# distributed_training/train_sbert.py
# Huấn luyện SBERT Baseline sử dụng PyTorch DDP (Multi-node)
# ============================================================

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from data_loader.parquet_loader import get_dist_dataloader
from config.cluster_config import ClusterConfig
import os
import argparse

def setup(rank, world_size):
    # Thiết lập môi trường phân tán (từ repo hkproj)
    os.environ['MASTER_ADDR'] = ClusterConfig.MASTER_ADDR
    os.environ['MASTER_PORT'] = ClusterConfig.MASTER_PORT

    # Khởi tạo process group (dùng backend gloo cho CPU hoặc nccl cho GPU)
    backend = "nccl" if torch.cuda.is_available() else "gloo"
    dist.init_process_group(backend, rank=rank, world_size=world_size)

def cleanup():
    dist.destroy_process_group()

def train(rank, world_size, args):
    print(f"Node Rank {rank} đang khởi động...")
    setup(rank, world_size)
    
    device = torch.device(f"cuda:{rank}" if torch.cuda.is_available() else "cpu")
    
    # 1. Khởi tạo Data Loader phân tán
    train_loader = get_dist_dataloader(
        ClusterConfig.DATA_PATH, 
        batch_size=args.batch_size, 
        world_size=world_size, 
        rank=rank
    )
    
    # 2. Khởi tạo Model (SBERT mẫu)
    # Ở đây chúng ta sẽ load model từ transformers hoặc custom
    # Giả sử: model = SentenceTransformer('all-MiniLM-L6-v2').to(device)
    model = torch.nn.Linear(768, 768).to(device) # Placeholder
    
    # Bọc model bằng DDP để tự động đồng bộ Gradient giữa các máy
    model = DDP(model, device_ids=[rank] if torch.cuda.is_available() else None)
    
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-5)
    criterion = torch.nn.CrossEntropyLoss()

    # 3. Vòng lặp huấn luyện
    model.train()
    for epoch in range(args.epochs):
        train_loader.sampler.set_epoch(epoch) # Quan trọng để shuffle đúng giữa các node
        for batch in train_loader:
            optimizer.zero_grad()
            # Thực hiện forward pass và backward pass...
            # loss = ...
            # loss.backward()
            optimizer.step()
            
        if rank == 0:
            print(f"Epoch {epoch} hoàn tất. Đang lưu checkpoint...")
            torch.save(model.module.state_dict(), f"{ClusterConfig.MODEL_SAVE_PATH}/sbert_final.pt")

    cleanup()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, default=32)
    parser.add_argument("--epochs", type=int, default=10)
    args = parser.parse_args()
    
    # Lấy Rank từ biến môi trường của torchrun
    rank = int(os.environ["RANK"])
    world_size = ClusterConfig.WORLD_SIZE
    
    train(rank, world_size, args)
