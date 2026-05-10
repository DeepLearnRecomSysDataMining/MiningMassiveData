import os
import logging
import torch
import torch.nn as nn
import torch.optim as optim
import pickle
import numpy as np
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import Dataset, DataLoader, DistributedSampler
from sentence_transformers import SentenceTransformer
from config.training_config import TrainingConfig
from src.models import DSSM
from tqdm import tqdm

logger = logging.getLogger("dssm_trainer")

class DSSMTrainingDataset(Dataset):
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

def evaluate_dssm(model, eval_pkl_path, text_encoder, device):
    """Đánh giá model DSSM (HR@10, NDCG@10)"""
    if not os.path.exists(eval_pkl_path):
        logger.warning(f"File Eval PKL khong ton tai tai: {eval_pkl_path}")
        return 0.0, 0.0

    with open(eval_pkl_path, 'rb') as f:
        evaluation_dataset = pickle.load(f)

    model.eval()
    # Hỗ trợ cả DDP và Single-GPU
    base_model = model.module if hasattr(model, 'module') else model
    hits_at_10, ndcg_at_10 = 0, 0.0
    total = len(evaluation_dataset)
    
    # Chia nhỏ data theo rank để eval song song (tùy chọn)
    chunk = evaluation_dataset[TrainingConfig.RANK::TrainingConfig.WORLD_SIZE]

    with torch.no_grad():
        for data in tqdm(chunk, desc=f"Eval DSSM Rank {TrainingConfig.RANK}", disable=(TrainingConfig.RANK != 0)):
            q_emb = text_encoder.encode(data['query_text'], convert_to_tensor=True).to(device)
            c_embs = text_encoder.encode(data['candidate_texts'], convert_to_tensor=True).to(device)
            
            q_rep = torch.nn.functional.normalize(base_model.amazon_tower(q_emb.unsqueeze(0)), p=2, dim=1)
            c_reps = torch.nn.functional.normalize(base_model.vn_tower(c_embs), p=2, dim=1)
            
            scores = torch.sum(q_rep * c_reps, dim=1).cpu().numpy()
            ranked_ids = [data['candidate_ids'][i] for i in np.argsort(scores)[::-1]]
            
            try:
                rank = ranked_ids.index(data['true_vn_id']) + 1
                if rank <= 10: hits_at_10 += 1
                ndcg_at_10 += 1.0 / np.log2(rank + 1) if rank <= 10 else 0.0
            except ValueError: pass

    # Đồng bộ kết quả từ tất cả các GPU
    res = torch.tensor([hits_at_10, ndcg_at_10], device=device)
    if TrainingConfig.WORLD_SIZE > 1:
        torch.distributed.all_reduce(res)
    
    return res[0].item() / total, res[1].item() / total

def train_dssm(interactions_df, item_lookup):
    """
    Quy trình Training DSSM Phân tán (DDP) tích hợp Evaluation sau mỗi Epoch.
    """
    device = TrainingConfig.DEVICE
    text_encoder = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    # 1. Setup Data
    train_set = DSSMTrainingDataset(interactions_df, item_lookup)
    sampler = DistributedSampler(train_set, num_replicas=TrainingConfig.WORLD_SIZE, rank=TrainingConfig.RANK)
    loader = DataLoader(train_set, batch_size=TrainingConfig.BATCH_SIZE, sampler=sampler, num_workers=0)
    
    # 2. Setup Model & DDP
    model = DSSM().to(device)
    if TrainingConfig.WORLD_SIZE > 1:
        if not torch.distributed.is_initialized():
            torch.distributed.init_process_group(backend="nccl")
        model = DDP(model, device_ids=[device.index])
    
    optimizer = optim.Adam(model.parameters(), lr=TrainingConfig.LR)
    criterion = nn.MarginRankingLoss(margin=0.2)
    
    best_hr10 = 0.0
    if TrainingConfig.RANK == 0:
        logger.info(f">>> BẮT ĐẦU HUAN LUYÊN DSSM TRÊN {TrainingConfig.WORLD_SIZE} GPUs...")
    
    for epoch in range(TrainingConfig.EPOCHS):
        sampler.set_epoch(epoch)
        model.train()
        total_loss = 0
        
        pbar = tqdm(loader, desc=f"Epoch {epoch+1}", disable=(TrainingConfig.RANK != 0))
        for q_texts, p_texts in pbar:
            # Batch Encoding Optimization
            with torch.no_grad():
                q_embs = text_encoder.encode(list(q_texts), convert_to_tensor=True).to(device)
                p_embs = text_encoder.encode(list(p_texts), convert_to_tensor=True).to(device)
            
            neg_embs = p_embs[torch.randperm(p_embs.size(0))]
            
            optimizer.zero_grad()
            pos_score = model(q_embs, p_embs)
            neg_score = model(q_embs, neg_embs)
            
            loss = criterion(pos_score, neg_score, torch.ones_like(pos_score).to(device))
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            pbar.set_postfix({"loss": f"{loss.item():.4f}"})
            
        # 3. ĐÁNH GIÁ (Sau mỗi Epoch)
        hr10, ndcg10 = evaluate_dssm(model, TrainingConfig.EVAL_PKL_PATH, text_encoder, device)
        
        if TrainingConfig.RANK == 0:
            avg_loss = total_loss / len(loader)
            logger.info(f"--- EPOCH {epoch+1} DONE | Loss: {avg_loss:.4f} | HR@10: {hr10:.4f} ---")
            
            if hr10 > best_hr10:
                best_hr10 = hr10
                # Lưu model gốc (không có module.) để tương thích tốt nhất
                save_model = model.module if hasattr(model, 'module') else model
                ckpt_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "dssm_best.pt")
                torch.save(save_model.state_dict(), ckpt_path)
                logger.info(f"==> MỚI ĐẶT KỶ LỤC! Đã lưu Best Model tại: {ckpt_path}")

    return os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "dssm_best.pt")
