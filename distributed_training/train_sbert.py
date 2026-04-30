import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from data_loader.parquet_loader import get_dist_dataloader
from config.cluster_config import ClusterConfig
import os
import argparse
import subprocess
import logging
import random
import numpy as np

# --- Production Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Rank %(rank)s] %(message)s"
)

def get_logger(rank):
    logger = logging.getLogger("train_sbert")
    # Add rank to logger context
    return logging.LoggerAdapter(logger, {"rank": rank})

def set_seed(seed=42):
    """Ensure reproducibility across all nodes."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

def setup(rank, world_size):
    os.environ['MASTER_ADDR'] = ClusterConfig.MASTER_ADDR
    os.environ['MASTER_PORT'] = ClusterConfig.MASTER_PORT
    # nccl is the standard for NVIDIA GPUs
    backend = "nccl" if torch.cuda.is_available() else "gloo"
    dist.init_process_group(backend, rank=rank, world_size=world_size)

def cleanup():
    dist.destroy_process_group()

def upload_to_gcs(local_path, gcs_path, logger):
    """High-performance upload using gsutil."""
    try:
        subprocess.run(["gsutil", "cp", local_path, gcs_path], check=True)
        logger.info(f"Checkpoint successfully synced to GCS: {gcs_path}")
    except Exception as e:
        logger.error(f"GCS Upload Failed: {e}")

def train(rank, world_size, args):
    logger = get_logger(rank)
    logger.info(f"Initializing distributed training on {world_size} nodes...")
    
    set_seed(args.seed)
    setup(rank, world_size)
    
    device = torch.device(f"cuda:{rank % torch.cuda.device_count()}" if torch.cuda.is_available() else "cpu")
    
    # 1. Pro Data Loading
    train_loader = get_dist_dataloader(
        ClusterConfig.DATA_PATH, 
        batch_size=args.batch_size, 
        world_size=world_size, 
        rank=rank
    )
    
    # 2. Model & Optimizer
    # Note: Replace with real SBERT model (e.g. from sentence_transformers)
    model = torch.nn.Linear(768, 768).to(device) 
    model = DDP(model, device_ids=[rank % torch.cuda.device_count()] if torch.cuda.is_available() else None)
    
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr)
    
    # 3. Training Loop
    model.train()
    for epoch in range(args.epochs):
        train_loader.sampler.set_epoch(epoch)
        epoch_loss = 0
        
        for i, batch in enumerate(train_loader):
            optimizer.zero_grad()
            # Forward pass placeholder
            # loss = compute_loss(model, batch)
            # loss.backward()
            optimizer.step()
            
            if i % 10 == 0 and rank == 0:
                logger.info(f"Epoch {epoch} | Batch {i}/{len(train_loader)} | Training...")

        # 4. Global Checkpointing (Rank 0 only)
        if rank == 0:
            logger.info(f"Epoch {epoch} complete. Starting GCS sync...")
            local_ckpt = f"sbert_model_epoch_{epoch}.pt"
            torch.save(model.module.state_dict(), local_ckpt)
            
            gcs_dest = f"{ClusterConfig.MODEL_SAVE_PATH}/sbert_baseline/{local_ckpt}"
            upload_to_gcs(local_ckpt, gcs_dest, logger)
            
            if os.path.exists(local_ckpt):
                os.remove(local_ckpt)

    logger.info("Training finished. Cleaning up process group...")
    cleanup()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, default=16)
    parser.add_argument("--epochs",     type=int, default=5)
    parser.add_argument("--lr",         type=float, default=2e-5)
    parser.add_argument("--seed",       type=int, default=42)
    args = parser.parse_args()
    
    rank = int(os.getenv("RANK", 0))
    world_size = int(os.getenv("WORLD_SIZE", ClusterConfig.WORLD_SIZE))
    
    train(rank, world_size, args)
