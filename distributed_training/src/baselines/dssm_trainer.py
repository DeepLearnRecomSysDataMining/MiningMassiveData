import os
import logging
import torch
import torch.nn as nn
import torch.optim as optim
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import Dataset, DataLoader, DistributedSampler
from sentence_transformers import SentenceTransformer
from config.training_config import TrainingConfig
from src.models import DSSM

logger = logging.getLogger("dssm_trainer")

class DSSMTrainingDataset(Dataset):
    def __init__(self, interactions_df, item_lookup, text_encoder):
        self.df = interactions_df
        self.lookup = item_lookup
        self.encoder = text_encoder

    def __len__(self): return len(self.df)

    def __getitem__(self, idx):
        row = self.df.iloc[idx]
        q_text = self.lookup.get(row['asin'], {}).get('full_text', "")
        p_text = self.lookup.get(row['parent_asin'], {}).get('full_text', "")
        
        # Pre-encoding or online encoding (In production, pre-compute these!)
        with torch.no_grad():
            q_emb = self.encoder.encode(q_text, convert_to_tensor=True)
            p_emb = self.encoder.encode(p_text, convert_to_tensor=True)
        return q_emb, p_emb

def train_dssm(interactions_df, item_lookup):
    """
    Baseline 3: DSSM Training on ETL Data.
    """
    device = TrainingConfig.DEVICE
    text_encoder = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    train_set = DSSMTrainingDataset(interactions_df, item_lookup, text_encoder)
    sampler = DistributedSampler(train_set, num_replicas=TrainingConfig.WORLD_SIZE, rank=TrainingConfig.RANK)
    loader = DataLoader(train_set, batch_size=TrainingConfig.BATCH_SIZE, sampler=sampler)
    
    model = DSSM().to(device)
    model = DDP(model, device_ids=[device.index] if device.type == 'cuda' else None)
    optimizer = optim.Adam(model.parameters(), lr=TrainingConfig.LR)
    criterion = nn.MarginRankingLoss(margin=0.2)
    
    logger.info(">>> Starting DSSM Training Loop...")
    for epoch in range(TrainingConfig.EPOCHS):
        sampler.set_epoch(epoch)
        model.train()
        total_loss = 0
        for q_emb, p_emb in loader:
            q_emb, p_emb = q_emb.to(device), p_emb.to(device)
            # Simple triplet with random negative for demo
            neg_emb = torch.randn_like(p_emb) 
            
            optimizer.zero_grad()
            pos_score, neg_score = model(q_emb, p_emb), model(q_emb, neg_emb)
            loss = criterion(pos_score, neg_score, torch.ones_like(pos_score).to(device))
            loss.backward(); optimizer.step()
            total_loss += loss.item()
            
        if TrainingConfig.RANK == 0:
            logger.info(f"DSSM Epoch {epoch+1}/{TrainingConfig.EPOCHS} | Loss: {total_loss/len(loader):.4f}")

    ckpt_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "dssm_final.pt")
    if TrainingConfig.RANK == 0: 
        torch.save(model.module.state_dict(), ckpt_path)
        logger.info(f"DSSM Model saved to {ckpt_path}")
    return ckpt_path
