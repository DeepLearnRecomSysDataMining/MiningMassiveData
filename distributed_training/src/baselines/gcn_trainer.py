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
    def __init__(self, interactions_df, item_lookup, text_encoder):
        self.df = interactions_df
        self.lookup = item_lookup
        self.encoder = text_encoder

    def __len__(self): return len(self.df)

    def __getitem__(self, idx):
        # Trích xuất dòng từ HF dataset (memory-mapped)
        row = self.df[int(idx)]
        
        # Lấy metadata từ lookup dựa trên asin và product_id (đã đồng bộ với Spark)
        q_meta = self.lookup.get(row['asin'], {})
        p_meta = self.lookup.get(row['product_id'], {})
        
        # Ưu tiên lấy full_text đã được chuẩn bị từ ETL v2
        q_text = q_meta.get('full_text', "") or q_meta.get('text', "")
        p_text = p_meta.get('full_text', "") or p_meta.get('text', "")
        
        # Encode on-the-fly để tiết kiệm RAM (tránh pre-computing 12GB vector)
        with torch.no_grad():
            q_emb = self.encoder.encode(q_text, convert_to_tensor=True)
            p_emb = self.encoder.encode(p_text, convert_to_tensor=True)
        
        return q_emb, p_emb

def evaluate_gcn(model, eval_pkl_path, text_encoder, device):
    """
    Đánh giá model GCN sử dụng file .pkl (HR@10, NDCG@10).
    GCN cần gộp Query và Candidates vào cùng một ma trận X để tạo đồ thị batch.
    """
    if not os.path.exists(eval_pkl_path):
        logger.warning(f"File Eval PKL không tồn tại: {eval_pkl_path}")
        return 0.0, 0.0

    with open(eval_pkl_path, 'rb') as f:
        evaluation_dataset = pickle.load(f)

    model.eval()
    hits_at_10 = 0
    ndcg_at_10 = 0.0
    total = len(evaluation_dataset)
    chunk = evaluation_dataset[TrainingConfig.RANK::TrainingConfig.WORLD_SIZE]

    with torch.no_grad():
        for data in tqdm(chunk, desc=f"Eval GCN Rank {TrainingConfig.RANK}", disable=(TrainingConfig.RANK != 0)):
            q_emb = text_encoder.encode(data['query_text'], convert_to_tensor=True).to(device) # (768)
            c_embs = text_encoder.encode(data['candidate_texts'], convert_to_tensor=True).to(device) # (100, 768)
            
            # Gộp Query (index 0) và 100 Candidates (index 1-100) -> X shape (1, 101, 768)
            X = torch.cat([q_emb.unsqueeze(0), c_embs], dim=0).unsqueeze(0)
            
            # Forward qua GCN (sẽ tạo đồ thị kề 101x101)
            X_out = model(X) # (1, 101, 128)
            
            q_gcn = X_out[:, 0:1, :] # (1, 1, 128)
            c_gcn = X_out[:, 1:, :]  # (1, 100, 128)
            
            # Tính điểm tương đồng Cosine
            scores = torch.sum(q_gcn * c_gcn, dim=2).squeeze(0).cpu().numpy() # (100,)
            ranked_ids = [data['candidate_ids'][i] for i in np.argsort(scores)[::-1]]
            
            try:
                rank = ranked_ids.index(data['true_vn_id']) + 1
                if rank <= 10: hits_at_10 += 1
                ndcg_at_10 += 1.0 / np.log2(rank + 1) if rank <= 10 else 0.0
            except ValueError: pass

    res = torch.tensor([hits_at_10, ndcg_at_10], device=device)
    if TrainingConfig.WORLD_SIZE > 1:
        torch.distributed.all_reduce(res)
    
    hr10 = res[0].item() / total
    ndcg10 = res[1].item() / total
    return hr10, ndcg10

def train_gcn(interactions_df, item_lookup):
    """
    Baseline 4: GCN Training using Batched Dynamic Graphs.
    Sử dụng kỹ thuật In-batch Negatives để tối ưu hóa I/O và RAM.
    """
    device = TrainingConfig.DEVICE
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE
    
    # 1. Khởi tạo Model & Encoder
    text_encoder = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    # Khóa trọng số encoder để giảm tải tính toán gradient
    for param in text_encoder.parameters():
        param.requires_grad = False
        
    model = BatchedGCN(in_features=768, hidden_features=256, out_features=128, knn_threshold=0.3).to(device)
    
    if TrainingConfig.WORLD_SIZE > 1:
        model = DDP(model, device_ids=[device.index] if device.type == 'cuda' else None)
    
    optimizer = optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-5)
    # Triplet Loss giúp tối ưu khoảng cách trên đồ thị
    criterion = nn.TripletMarginLoss(margin=0.5, p=2)

    # 2. Chuẩn bị DataLoader
    dataset = GCNTrainingDataset(interactions_df, item_lookup, text_encoder)
    sampler = DistributedSampler(dataset, num_replicas=world_size, rank=rank, shuffle=True)
    
    # CẢNH BÁO RAM: num_workers=0 để tránh tràn RAM do nhân bản metadata dictionary
    train_loader = DataLoader(
        dataset, 
        batch_size=TrainingConfig.BATCH_SIZE, 
        sampler=sampler, 
        num_workers=0,
        pin_memory=True
    )

    epochs = 5 # GCN hội tụ khá nhanh
    best_hr = 0.0
    ckpt_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "gcn_best.pt")
    
    if rank == 0:
        logger.info(f"Bắt đầu huấn luyện GCN (Baseline 4) trên {len(dataset):,} tương tác...")

    for epoch in range(epochs):
        model.train()
        sampler.set_epoch(epoch)
        total_loss = 0
        
        pbar = tqdm(train_loader, desc=f"Epoch {epoch+1}/{epochs}", disable=(rank != 0))
        for batch_idx, (q_embs, p_embs) in enumerate(pbar):
            q_embs, p_embs = q_embs.to(device), p_embs.to(device)
            B = q_embs.size(0)
            if B < 2: continue # Bỏ qua batch quá nhỏ để làm Triplet
            
            optimizer.zero_grad()
            
            # --- XÂY DỰNG BATCH GRAPH ---
            # Gộp Query và Positive vào một cấu trúc (B, 2, 768)
            X = torch.stack([q_embs, p_embs], dim=1) 
            X_out = model(X) # Kết quả sau lan truyền tin nhắn GCN (B, 2, 128)
            
            anchors = X_out[:, 0, :]
            positives = X_out[:, 1, :]
            
            # --- MEMORY-EFFICIENT IN-BATCH NEGATIVES ---
            # Lấy Positive của dòng khác làm Negative (Dịch chuyển vòng tròn 1 đơn vị)
            neg_indices = (torch.arange(B, device=device) + 1) % B
            negatives = positives[neg_indices]
            
            loss = criterion(anchors, positives, negatives)
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            if rank == 0 and batch_idx % 100 == 0:
                pbar.set_postfix({"loss": total_loss / (batch_idx + 1)})

        # 3. Đánh giá cuối mỗi Epoch
        if rank == 0:
            logger.info(f"--- Đang đánh giá GCN Rank 0 (Epoch {epoch+1}) ---")
            hr10, ndcg10 = evaluate_gcn(model, TrainingConfig.EVAL_PKL_PATH, text_encoder, device)
            logger.info(f"GCN Kết quả | HR@10: {hr10:.4f} | NDCG@10: {ndcg10:.4f}")
            
            if hr10 > best_hr:
                best_hr = hr10
                torch.save(model.module.state_dict() if hasattr(model, 'module') else model.state_dict(), ckpt_path)
                logger.info(f"Đã lưu mô hình GCN tốt nhất với HR@10: {best_hr:.4f}")

    # Đồng bộ hóa trước khi kết thúc
    if world_size > 1:
        torch.distributed.barrier()
        
    return ckpt_path
