import os
import logging
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
import pickle
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import Dataset, DataLoader, DistributedSampler
from sentence_transformers import SentenceTransformer
from config.training_config import TrainingConfig
from src.models import BatchedGCN
from tqdm import tqdm
import numpy as np

logger = logging.getLogger("gcn_trainer")

class GCNTrainingDataset(Dataset):
    def __init__(self, interactions_df, item_lookup):
        self.df = interactions_df
        self.lookup = item_lookup

    def __len__(self): return len(self.df)

    def __getitem__(self, idx):
        row = self.df[int(idx)]
        q_meta = self.lookup.get(row['asin'], {})
        p_meta = self.lookup.get(row['product_id'], {})
        q_text = q_meta.get('full_text', "") or q_meta.get('text', "")
        p_text = p_meta.get('full_text', "") or p_meta.get('text', "")
        return q_text, p_text

def evaluate_gcn(model, eval_pkl_path, text_encoder, device):
    """Đánh giá model GCN sử dụng file .pkl (HR@10, NDCG@10)"""
    if not os.path.exists(eval_pkl_path):
        return 0.0, 0.0

    with open(eval_pkl_path, 'rb') as f:
        evaluation_dataset = pickle.load(f)

    model.eval()
    hits_at_10, ndcg_at_10 = 0, 0.0
    total = len(evaluation_dataset)
    chunk = evaluation_dataset[TrainingConfig.RANK::TrainingConfig.WORLD_SIZE]

    with torch.no_grad():
        for data in tqdm(chunk, desc=f"Eval GCN Rank {TrainingConfig.RANK}", disable=(TrainingConfig.RANK != 0)):
            q_emb = text_encoder.encode(data['query_text'], convert_to_tensor=True).to(device)
            c_embs = text_encoder.encode(data['candidate_texts'], convert_to_tensor=True).to(device)
            
            # Gộp Query và Candidates tạo đồ thị động
            X = torch.cat([q_emb.unsqueeze(0), c_embs], dim=0).unsqueeze(0)
            X_out = model(X)
            
            q_gcn = X_out[:, 0:1, :]
            c_gcn = X_out[:, 1:, :]
            
            scores = torch.sum(q_gcn * c_gcn, dim=2).squeeze(0).cpu().numpy()
            ranked_ids = [data['candidate_ids'][i] for i in np.argsort(scores)[::-1]]
            
            try:
                rank = ranked_ids.index(data['true_vn_id']) + 1
                if rank <= 10: hits_at_10 += 1
                ndcg_at_10 += 1.0 / np.log2(rank + 1) if rank <= 10 else 0.0
            except ValueError: pass

    res = torch.tensor([hits_at_10, ndcg_at_10], device=device)
    if TrainingConfig.WORLD_SIZE > 1:
        torch.distributed.all_reduce(res)
    
    return res[0].item() / total, res[1].item() / total

def train_gcn(interactions_df, item_lookup):
    device = TrainingConfig.DEVICE
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE
    
    text_encoder = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    for param in text_encoder.parameters():
        param.requires_grad = False
        
    model = BatchedGCN(in_features=768, hidden_features=256, out_features=128, knn_threshold=0.3).to(device)
    if world_size > 1:
        if not torch.distributed.is_initialized():
            torch.distributed.init_process_group(backend="nccl")
        model = DDP(model, device_ids=[device.index])
    
    optimizer = optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-5)
    criterion = nn.TripletMarginLoss(margin=0.5, p=2)

    dataset = GCNTrainingDataset(interactions_df, item_lookup)
    sampler = DistributedSampler(dataset, num_replicas=world_size, rank=rank, shuffle=True)
    train_loader = DataLoader(dataset, batch_size=TrainingConfig.BATCH_SIZE, sampler=sampler, num_workers=0)

    epochs = 5
    best_hr = 0.0
    ckpt_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "gcn_best.pt")
    
    if rank == 0:
        logger.info(f">>> BẮT ĐẦU HUẤN LUYỆN GCN (BASELINE 4) TRÊN {world_size} GPUs...")

    for epoch in range(epochs):
        model.train()
        sampler.set_epoch(epoch)
        total_loss = 0
        
        pbar = tqdm(train_loader, desc=f"Epoch {epoch+1}/{epochs}", disable=(rank != 0))
        for batch_idx, (q_texts, p_texts) in enumerate(pbar):
            with torch.no_grad():
                q_embs = text_encoder.encode(list(q_texts), convert_to_tensor=True).to(device)
                p_embs = text_encoder.encode(list(p_texts), convert_to_tensor=True).to(device)

            B = q_embs.size(0)
            if B < 2: continue
            
            optimizer.zero_grad()
            X = torch.stack([q_embs, p_embs], dim=1) 
            X_out = model(X)
            
            anchors, positives = X_out[:, 0, :], X_out[:, 1, :]
            neg_indices = (torch.arange(B, device=device) + 1) % B
            negatives = positives[neg_indices]
            
            loss = criterion(anchors, positives, negatives)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
            
            if rank == 0 and batch_idx % 100 == 0:
                pbar.set_postfix({"loss": f"{total_loss / (batch_idx + 1):.4f}"})

        # Đánh giá cuối mỗi Epoch
        hr10, ndcg10 = evaluate_gcn(model, TrainingConfig.EVAL_PKL_PATH, text_encoder, device)
        if rank == 0:
            logger.info(f"GCN EPOCH {epoch+1} | HR@10: {hr10:.4f} | NDCG@10: {ndcg10:.4f}")
            if hr10 > best_hr:
                best_hr = hr10
                save_model = model.module if hasattr(model, 'module') else model
                torch.save(save_model.state_dict(), ckpt_path)
                logger.info(f"==> Đã lưu GCN Best Model với HR@10: {best_hr:.4f}")

    return ckpt_path
