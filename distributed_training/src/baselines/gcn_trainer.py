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
from src.metrics_utils import write_metrics_csv
from src.checkpoint_utils import download_resume_checkpoint, load_resume_checkpoint, save_resume_checkpoint, save_best_model, upload_file_to_gcs

logger = logging.getLogger("gcn_trainer")

class GCNTrainingDataset(Dataset):
    def __init__(self, interactions_df, embedding_lookup):
        self.asins = interactions_df["asin"]
        self.product_ids = interactions_df["product_id"]
        self.lookup = embedding_lookup

    def __len__(self): 
        return len(self.asins)

    def __getitem__(self, idx):
        idx = int(idx)

        q_emb = self.lookup.get_embedding(f"amz_{self.asins[idx]}")
        p_emb = self.lookup.get_embedding(f"vn_{self.product_ids[idx]}")
        
        return (
            torch.from_numpy(q_emb.copy()).float(),
            torch.from_numpy(p_emb.copy()).float()
        )

def evaluate_gcn(model, eval_pkl_path, text_encoder, device):
    """Đánh giá model GCN sử dụng file .pkl"""
    if not os.path.exists(eval_pkl_path):
        return 0.0, 0.0

    with open(eval_pkl_path, 'rb') as f:
        evaluation_dataset = pickle.load(f)

    model.eval()
    hits_at_10, ndcg_at_10 = 0, 0.0
    total = len(evaluation_dataset)
    # chunk = evaluation_dataset[TrainingConfig.RANK::TrainingConfig.WORLD_SIZE]
    chunk = evaluation_dataset

    with torch.no_grad():
        for data in tqdm(chunk, desc=f"Eval GCN Rank {TrainingConfig.RANK}", disable=(TrainingConfig.RANK != 0)):
            q_emb = text_encoder.encode(data['query_text'], convert_to_tensor=True).to(device)
            c_embs = text_encoder.encode(data['candidate_texts'], convert_to_tensor=True).to(device)
            
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
            
    return hits_at_10 / total, ndcg_at_10 / total

def train_gcn(interactions_df, embedding_lookup):
    device = TrainingConfig.DEVICE
    logger.info(f"\n\n\nTraining GCN on device {device}\n\n\n")
    
    # Chỉ dùng encoder cho Evaluation
    text_encoder = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    train_set = GCNTrainingDataset(interactions_df, embedding_lookup)
    loader = DataLoader(train_set, batch_size=TrainingConfig.BATCH_SIZE, shuffle=False, num_workers=1, pin_memory=True, drop_last=True, persistent_workers=True)
    
    model = BatchedGCN(in_features=768, hidden_features=256, out_features=128, knn_threshold=0.3).to(device)

    if TrainingConfig.WORLD_SIZE > 1:
        if not torch.distributed.is_initialized(): 
            torch.distributed.init_process_group(backend="nccl")
        model = DDP(model, device_ids=[device.index])
    
    local_batches = torch.tensor([len(loader)], device=device)

    if torch.distributed.is_initialized():
        torch.distributed.all_reduce( local_batches, op=torch.distributed.ReduceOp.MIN)

    sync_train_batches = int(local_batches.item())

    logger.info(
        f"Rank {TrainingConfig.RANK}: local_batches={len(loader)}, "
        f"sync_train_batches={sync_train_batches}"
    )

    optimizer = optim.Adam(model.parameters(), lr=1e-4, weight_decay=1e-5) # giảm LR thay vì chọn TrainingConfig.LEARNing..
    criterion = nn.TripletMarginLoss(margin=0.2, p=2)  # giảm margin

    if TrainingConfig.RANK == 0:
        download_resume_checkpoint("gcn")

    if torch.distributed.is_initialized():
        torch.distributed.barrier()

    resume = load_resume_checkpoint( "gcn", model, optimizer, device=device )

    start_epoch = resume["start_epoch"]
    best_hr = resume["best_metric"]
    metrics_rows = resume["history"]
    
    if TrainingConfig.RANK == 0:
        logger.info(">>> BẮT ĐẦU HUẤN LUYỆN GCN (PRECOMPUTED)...")

    for epoch in range(start_epoch, TrainingConfig.EPOCHS):
        # sampler.set_epoch(epoch)
        model.train()
        total_loss = 0
        total_batches = min(len(loader), sync_train_batches)
        
        # pbar = tqdm(train_loader, desc=f"Epoch {epoch+1}/{TrainingConfig.EPOCHS}", disable=(rank != 0))
        for batch_idx, (q_embs, p_embs) in enumerate(loader, start=1):
            if batch_idx > sync_train_batches:
                break

            q_embs, p_embs = q_embs.to(device, non_blocking=True), p_embs.to(device, non_blocking=True)

            B = q_embs.size(0)
            if B < 2:
                continue
            num_neg = min(8, B - 1)
            # 1. Lấy negative candidates từ các positive khác trong cùng batch
            rand_idx = torch.randint( 0, B, (B, num_neg), device=device )
            row_idx = torch.arange(B, device=device).unsqueeze(1)

            # Tránh chọn đúng positive của chính sample đó
            rand_idx = torch.where( rand_idx == row_idx, (rand_idx + 1) % B, rand_idx )
            neg_embs = p_embs[rand_idx]  # (B, num_neg, 768)

            # 2. Tạo graph context thật: query + positive + nhiều negatives
            X = torch.cat(
                [
                    q_embs.unsqueeze(1),  # (B, 1, 768)
                    p_embs.unsqueeze(1),  # (B, 1, 768)
                    neg_embs              # (B, num_neg, 768)
                ],
                dim=1
            )

            optimizer.zero_grad(set_to_none=True)

            # 3. GCN message passing trên graph nhiều node
            X_out = model(X)

            anchors = X_out[:, 0, :]          # (B, 128)
            positives = X_out[:, 1, :]        # (B, 128)
            neg_candidates = X_out[:, 2:, :]  # (B, num_neg, 128)

            # 4. Chọn negative
            if epoch <= 3:
                # Warm-up: chọn random negative trong graph
                neg_choice = torch.randint( 0, num_neg, (B,), device=device )
            else:
                # Semi-hard: chọn negative giống anchor nhất trong graph candidates
                with torch.no_grad():
                    a_norm = F.normalize(anchors, p=2, dim=1)
                    n_norm = F.normalize(neg_candidates, p=2, dim=2)
                    sim = torch.sum( a_norm.unsqueeze(1) * n_norm, dim=2 )  # (B, num_neg)
                    k = min(5, sim.size(1))
                    topk_idx = torch.topk( sim, k=k, dim=1 ).indices  # (B, k)
                    rand_pos = torch.randint( 0, k, (B,), device=device )
                    neg_choice = topk_idx[ torch.arange(B, device=device), rand_pos ]

            negatives = neg_candidates[ torch.arange(B, device=device), neg_choice ]  # (B, 128)

            # 5. Triplet loss
            loss = criterion(anchors, positives, negatives)
            loss.backward()
            optimizer.step()

            total_loss += loss.item()
            
            if TrainingConfig.RANK == 0 and (batch_idx % 200 == 0 or batch_idx == total_batches):
                # pbar.set_postfix({"loss": f"{total_loss / (batch_idx + 1):.4f}"})
                logger.info(
                    f"Epoch {epoch+1}/{TrainingConfig.EPOCHS} | "
                    f"Batch {batch_idx}/{total_batches} | "
                    f"Loss={loss.item():.4f}"
                )

        if torch.distributed.is_initialized():
            torch.distributed.barrier()

        loss_tensor = torch.tensor([total_loss, total_batches], dtype=torch.float32, device=device)
        if torch.distributed.is_initialized():
            torch.distributed.all_reduce(loss_tensor, op=torch.distributed.ReduceOp.SUM)
        global_avg_loss = (loss_tensor[0] / loss_tensor[1].clamp_min(1)).item()

        if TrainingConfig.RANK == 0:
            hr10, ndcg10 = evaluate_gcn(model, TrainingConfig.EVAL_PKL_PATH, text_encoder, device)
            logger.info(f"GCN EPOCH {epoch+1} DONE | HR@10: {hr10:.4f}")
            
            # avg_loss = total_loss / max(total_batches, 1)

            # trong mỗi epoch sau eval:
            current_metrics = {
                "baseline": "gcn",
                "epoch": epoch + 1,
                "hr10": hr10,
                "ndcg10": ndcg10,
                "loss": global_avg_loss,
                "data_fraction": getattr(TrainingConfig, "DATA_FRACTION", "")
            }

            metrics_rows.append(current_metrics)
            metrics_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "gcn_metrics.csv")    
            write_metrics_csv(metrics_path, metrics_rows)
            upload_file_to_gcs(metrics_path)
            
            if hr10 > best_hr:
                best_hr = hr10
                save_best_model( model_name="gcn", model=model, epoch=epoch, metrics=current_metrics )           

            save_resume_checkpoint(model_name="gcn", model=model, optimizer=optimizer, epoch=epoch, best_metric=best_hr, history=metrics_rows)
        
        if torch.distributed.is_initialized():
            torch.distributed.barrier()

    return os.path.join( TrainingConfig.LOCAL_MODELS_DIR, "gcn_best.pt" )
