import os
import logging
import torch
import torch.nn as nn
import torch.optim as optim
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import Dataset, DataLoader, DistributedSampler
from sentence_transformers import SentenceTransformer
from config.training_config import TrainingConfig
from src.models import BatchedGCN

logger = logging.getLogger("gcn_trainer")

def train_gcn(interactions_df, item_lookup):
    """
    Baseline 4: GCN Training on Graph.
    (Simplified placeholder for ETL integration)
    """
    device = TrainingConfig.DEVICE
    # Logic GCN training would go here, utilizing interactions_df for edges
    logger.info(">>> GCN Training started (Placeholder for ETL graph logic)...")
    
    model = BatchedGCN().to(device)
    model = DDP(model, device_ids=[device.index] if device.type == 'cuda' else None)
    # ... Training loop ...
    
    ckpt_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "gcn_final.pt")
    if TrainingConfig.RANK == 0: 
        torch.save(model.module.state_dict(), ckpt_path)
    return ckpt_path
