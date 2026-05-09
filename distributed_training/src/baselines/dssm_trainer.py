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
    def __init__(self, interactions_df, item_lookup, text_encoder):
        self.df = interactions_df
        self.lookup = item_lookup
        self.encoder = text_encoder

    def __len__(self): return len(self.df)

    def __getitem__(self, idx):
        row = self.df.iloc[idx]
        # Lấy metadata từ lookup
        q_meta = self.lookup.get(row['asin'], {})
        p_meta = self.lookup.get(row['parent_asin'], {})
        
        q_text = q_meta.get('full_text', "") or q_meta.get('text', "")
        p_text = p_meta.get('full_text', "") or p_meta.get('text', "")
        
        with torch.no_grad():
            q_emb = self.encoder.encode(q_text, convert_to_tensor=True)
            p_emb = self.encoder.encode(p_text, convert_to_tensor=True)
        return q_emb, p_emb

def evaluate_dssm(model, eval_pkl_path, text_encoder, device):
    """
    Đánh giá model DSSM sử dụng file .pkl (HR@10, NDCG@10)
    """
    if not os.path.exists(eval_pkl_path):
        logger.warning(f"File Eval PKL khong ton tai tai: {eval_pkl_path}")
        return 0.0, 0.0

    with open(eval_pkl_path, 'rb') as f:
        evaluation_dataset = pickle.load(f)

    model.eval()
    hits_at_10 = 0
    ndcg_at_10 = 0.0
    total = len(evaluation_dataset)
    
    # Chia nhỏ data theo rank để eval song song (tùy chọn)
    chunk = evaluation_dataset[TrainingConfig.RANK::TrainingConfig.WORLD_SIZE]

    with torch.no_grad():
        for data in tqdm(chunk, desc=f"Evaluating Rank {TrainingConfig.RANK}", disable=(TrainingConfig.RANK != 0)):
            q_emb = text_encoder.encode(data['query_text'], convert_to_tensor=True).to(device)
            c_embs = text_encoder.encode(data['candidate_texts'], convert_to_tensor=True).to(device)
            
            # DSSM forward: q_rep và c_reps
            # Vì model là DDP, ta gọi model.module nếu cần, hoặc gọi trực tiếp nếu DDP hỗ trợ
            q_rep = torch.nn.functional.normalize(model.module.amazon_tower(q_emb.unsqueeze(0)), p=2, dim=1)
            c_reps = torch.nn.functional.normalize(model.module.vn_tower(c_embs), p=2, dim=1)
            
            scores = torch.sum(q_rep * c_reps, dim=1).cpu().numpy()
            ranked_ids = [data['candidate_ids'][i] for i in np.argsort(scores)[::-1]]
            
            try:
                rank = ranked_ids.index(data['true_vn_id']) + 1
                if rank <= 10: hits_at_10 += 1
                ndcg_at_10 += 1.0 / np.log2(rank + 1) if rank <= 10 else 0.0
            except ValueError: pass

    # Đồng bộ kết quả từ tất cả các GPU
    res = torch.tensor([hits_at_10, ndcg_at_10], device=device)
    torch.distributed.all_reduce(res)
    
    hr10 = res[0].item() / total
    ndcg10 = res[1].item() / total
    return hr10, ndcg10

def train_dssm(interactions_df, item_lookup):
    """
    Quy trình Training DSSM Phân tán (DDP) tích hợp Evaluation sau mỗi Epoch.
    """
    device = TrainingConfig.DEVICE
    text_encoder = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    # 1. Setup Data
    train_set = DSSMTrainingDataset(interactions_df, item_lookup, text_encoder)
    
    # Khởi tạo process group cho DDP (Chỉ gọi nếu WORLD_SIZE > 1 hoặc dùng torchrun)
    if TrainingConfig.WORLD_SIZE > 1 or os.getenv("RANK"):
        if not torch.distributed.is_initialized():
            torch.distributed.init_process_group(backend="nccl" if torch.cuda.is_available() else "gloo")
            
    sampler = DistributedSampler(train_set, num_replicas=TrainingConfig.WORLD_SIZE, rank=TrainingConfig.RANK)
    loader = DataLoader(train_set, batch_size=TrainingConfig.BATCH_SIZE, sampler=sampler)
    
    # 2. Setup Model & DDP
    model = DSSM().to(device)
    if TrainingConfig.WORLD_SIZE > 1:
        model = DDP(model, device_ids=[device.index] if device.type == 'cuda' else None)
    
    optimizer = optim.Adam(model.parameters(), lr=TrainingConfig.LR)
    criterion = nn.MarginRankingLoss(margin=0.2)
    
    best_hr10 = 0.0
    logger.info(f">>> BAT DAU HUAN LUYEN DSSM TRÊN {TrainingConfig.WORLD_SIZE} GPUs...")
    
    for epoch in range(TrainingConfig.EPOCHS):
        sampler.set_epoch(epoch)
        model.train()
        total_loss = 0
        
        pbar = tqdm(loader, desc=f"Epoch {epoch+1}", disable=(TrainingConfig.RANK != 0))
        for q_emb, p_emb in pbar:
            q_emb, p_emb = q_emb.to(device), p_emb.to(device)
            neg_emb = p_emb[torch.randperm(p_emb.size(0))] 
            
            optimizer.zero_grad()
            
            # Forward
            if TrainingConfig.WORLD_SIZE > 1:
                pos_score = model(q_emb, p_emb)
                neg_score = model(q_emb, neg_emb)
            else:
                pos_score = model(q_emb, p_emb)
                neg_score = model(q_emb, neg_emb)
            
            loss = criterion(pos_score, neg_score, torch.ones_like(pos_score).to(device))
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            pbar.set_postfix({"loss": f"{loss.item():.4f}"})
            
        # 3. ĐÁNH GIÁ (EVALUATION)
        # Truyền model.module nếu dùng DDP, ngược lại truyền model
        eval_model = model.module if TrainingConfig.WORLD_SIZE > 1 else model
        hr10, ndcg10 = evaluate_dssm(eval_model, TrainingConfig.EVAL_PKL_PATH, text_encoder, device)
        
        if TrainingConfig.RANK == 0:
            avg_loss = total_loss / len(loader)
            logger.info(f"--- EPOCH {epoch+1} DONE ---")
            logger.info(f"Loss: {avg_loss:.4f} | HR@10: {hr10:.4f} | NDCG@10: {ndcg10:.4f}")
            
            if hr10 > best_hr10:
                best_hr10 = hr10
                ckpt_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "dssm_best.pt")
                torch.save(eval_model.state_dict(), ckpt_path)
                logger.info(f"==> MOI DAT KY LUC! Da luu Best Model tai: {ckpt_path}")

    if torch.distributed.is_initialized():
        torch.distributed.destroy_process_group()
        
    return os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "dssm_best.pt")

if __name__ == "__main__":
    from src.data_utils import load_item_nodes_lookup, load_interactions_df
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    logger.info("--- [KHOI DONG DSSM TRAINER] ---")
    
    # 1. Load Item Metadata bằng hàm dùng chung (Tối ưu RAM)
    item_lookup = load_item_nodes_lookup()
    
    # 2. Load Interactions bằng hàm dùng chung
    interactions_df = load_interactions_df()
    
    # 3. Chạy Training
    train_dssm(interactions_df, item_lookup)
