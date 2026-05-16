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
from src.metrics_utils import write_metrics_csv
from src.checkpoint_utils import download_resume_checkpoint, load_resume_checkpoint, save_resume_checkpoint, save_best_model, upload_file_to_gcs

logger = logging.getLogger("dssm_trainer")

class DSSMTrainingDataset(Dataset):
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

def evaluate_dssm(model, eval_pkl_path, text_encoder, device):
    """Đánh giá model DSSM (Vẫn cần encoder cho tập Eval vì nó nhỏ)"""
    if not os.path.exists(eval_pkl_path):
        return 0.0, 0.0

    with open(eval_pkl_path, 'rb') as f:
        evaluation_dataset = pickle.load(f)

    model.eval()
    base_model = model.module if hasattr(model, 'module') else model
    hits_at_10, ndcg_at_10 = 0, 0.0
    total = len(evaluation_dataset)
    # chunk = evaluation_dataset[TrainingConfig.RANK::TrainingConfig.WORLD_SIZE]
    chunk = evaluation_dataset

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

    # res = torch.tensor([hits_at_10, ndcg_at_10], device=device)
    # if TrainingConfig.WORLD_SIZE > 1:
    #     if torch.distributed.is_initialized():
    #         torch.distributed.all_reduce(res, op=torch.distributed.ReduceOp.SUM)
    # return res[0].item() / total, res[1].item() / total
    return hits_at_10 / total, ndcg_at_10 / total

def train_dssm(interactions_df, embedding_lookup):
    device = TrainingConfig.DEVICE
    logger.info(f"\n\n\nTraining DSSM on device {device}\n\n\n")
    # Chỉ dùng encoder cho phần Evaluation
    text_encoder = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    # 1. Setup Data (Chế độ Precomputed)
    train_set = DSSMTrainingDataset(interactions_df, embedding_lookup)
    loader = DataLoader(train_set, batch_size=TrainingConfig.BATCH_SIZE, shuffle=False, num_workers=1, pin_memory=True, drop_last=True, persistent_workers=True)

    # 2. Setup Model & DDP
    model = DSSM().to(device)

    if TrainingConfig.WORLD_SIZE > 1:
        if not torch.distributed.is_initialized():
            torch.distributed.init_process_group(backend="nccl")
        model = DDP(model, device_ids=[device.index])

    local_batches = torch.tensor([len(loader)], device=device)

    if torch.distributed.is_initialized():
        torch.distributed.all_reduce(local_batches, op=torch.distributed.ReduceOp.MIN)

    sync_train_batches = int(local_batches.item())
    
    logger.info(
        f"Rank {TrainingConfig.RANK}: local_batches={len(loader)}, "
        f"sync_train_batches={sync_train_batches}"
    )
    
    optimizer = optim.Adam(model.parameters(), lr=TrainingConfig.LR)
    criterion = nn.MarginRankingLoss(margin=0.2)
    
    if TrainingConfig.RANK == 0:
        download_resume_checkpoint("dssm")

    if torch.distributed.is_initialized():
        torch.distributed.barrier()
    
    resume = load_resume_checkpoint( "dssm", model, optimizer, device=device )

    start_epoch = resume["start_epoch"]
    best_hr10 = resume["best_metric"]
    metrics_rows = resume["history"]

    if TrainingConfig.RANK == 0:
        logger.info(">>> BẮT ĐẦU HUẤN LUYỆN DSSM (PRECOMPUTED)...")  

    for epoch in range(start_epoch, TrainingConfig.EPOCHS):
        # sampler.set_epoch(epoch)
        model.train()
        total_loss = 0
        total_batches = min(len(loader), sync_train_batches)

        for batch_idx, (q_embs, p_embs) in enumerate(loader, start=1):
            if batch_idx > sync_train_batches:
                break

            q_embs, p_embs = q_embs.to(device, non_blocking=True), p_embs.to(device, non_blocking=True)
            # neg_embs = p_embs[torch.randperm(p_embs.size(0),device=device)] # device=device: Tránh tạo index CPU rồi dùng với tensor GPU
            with torch.no_grad():
                q_norm = torch.nn.functional.normalize(q_embs, p=2, dim=1)
                p_norm = torch.nn.functional.normalize(p_embs, p=2, dim=1)
                sim = torch.matmul(q_norm, p_norm.T)
                sim.fill_diagonal_(-1e9)

                k = min(10, sim.size(1) - 1)
                topk_idx = torch.topk(sim, k=k, dim=1).indices

                rand_pos = torch.randint(0, k, (sim.size(0),), device=device)
                hard_neg_idx = topk_idx[
                    torch.arange(sim.size(0), device=device),
                    rand_pos
                ]

            neg_embs = p_embs[hard_neg_idx]

            # 4. Training Step
            optimizer.zero_grad(set_to_none=True) # set_to_none=True giúp giải phóng bộ nhớ nhanh hơn
            pos_score = model(q_embs, p_embs)
            neg_score = model(q_embs, neg_embs)
            
            # loss = criterion(pos_score, neg_score, torch.ones_like(pos_score).to(device))
            target = torch.ones_like(pos_score, device=device)
            loss = criterion(pos_score, neg_score, target)
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()

            if TrainingConfig.RANK == 0 and (batch_idx % 200 == 0 or batch_idx == total_batches):
                logger.info(f"Epoch {epoch+1}/{TrainingConfig.EPOCHS} | Batch {batch_idx}/{total_batches} | Loss={loss.item():.4f}")

        if torch.distributed.is_initialized():
            torch.distributed.barrier()

        # Eval chỉ chạy trên Rank 0 để tránh NCCL timeout do các rank lệch nhịp khi encode text
        if TrainingConfig.RANK == 0:
            hr10, ndcg10 = evaluate_dssm( model, TrainingConfig.EVAL_PKL_PATH, text_encoder, device )
            logger.info(f"--- EPOCH {epoch+1} DONE | HR@10: {hr10:.4f} ---")
            
            avg_loss = total_loss / max(total_batches, 1)

            current_metrics = {
                "baseline": "dssm",
                "epoch": epoch + 1,
                "hr10": hr10,
                "ndcg10": ndcg10,
                "loss": avg_loss,
                "data_fraction": getattr(TrainingConfig, "DATA_FRACTION", "")
            }

            metrics_rows.append(current_metrics)
            metrics_path = os.path.join( TrainingConfig.LOCAL_MODELS_DIR, "dssm_metrics.csv" )
            write_metrics_csv(metrics_path, metrics_rows)
            upload_file_to_gcs(metrics_path)

            if hr10 > best_hr10:
                best_hr10 = hr10
                save_best_model( model_name="dssm", model=model, epoch=epoch, metrics=current_metrics)

            save_resume_checkpoint( model_name="dssm", model=model, optimizer=optimizer, epoch=epoch, best_metric=best_hr10, history=metrics_rows)

        # Sync sau eval + save checkpoint
        if torch.distributed.is_initialized():
            torch.distributed.barrier()
        
    return os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "dssm_best.pt")
