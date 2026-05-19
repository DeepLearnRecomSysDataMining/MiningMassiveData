import os
import json
import logging
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from tqdm import tqdm
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import Dataset, DataLoader
from sentence_transformers import SentenceTransformer

from config.training_config import TrainingConfig
from src.models import LLM_CHGNN
from src.metrics_utils import write_metrics_csv
from src.checkpoint_utils import download_resume_checkpoint, load_resume_checkpoint, save_resume_checkpoint, save_best_model, upload_file_to_gcs
from src.data_utils import load_eval_dataset

logger = logging.getLogger("llm_chgnn_trainer")


def _get_env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def build_attr_vocab(dataset, max_attrs: int = 5000):
    """
    Build a global attribute vocabulary from query_specs + candidate_specs.
    The vocab is capped to avoid exploding H matrix RAM/VRAM.
    """
    attr_counts = {}

    for data in dataset:
        specs_list = [data.get("query_specs", {})] + data.get("candidate_specs", [])
        for specs in specs_list:
            if not specs:
                continue
            for k, v in specs.items():
                if k is None or v is None:
                    continue
                attr = f"{str(k).strip().lower()}:{str(v).strip().lower()}"
                if attr == ":":
                    continue
                attr_counts[attr] = attr_counts.get(attr, 0) + 1

    sorted_attrs = sorted(attr_counts.items(), key=lambda x: (-x[1], x[0]))
    return [attr for attr, _ in sorted_attrs[:max_attrs]]


def save_attr_vocab(model_name: str, attr_vocab):
    if TrainingConfig.RANK != 0:
        return

    os.makedirs(TrainingConfig.LOCAL_MODELS_DIR, exist_ok=True)
    local_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, f"{model_name}_attr_vocab.json")

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(attr_vocab, f, ensure_ascii=False, indent=2)

    upload_file_to_gcs(local_path)


def build_incidence_matrix(item_specs_list, attr_to_idx, device):
    """
    Build dense incidence matrix H for one graph.
    Return shape: (N, E)
    """
    N = len(item_specs_list)
    E = len(attr_to_idx)
    H = torch.zeros((N, E), dtype=torch.float32, device=device)

    if E == 0:
        return H

    for node_idx, specs in enumerate(item_specs_list):
        if not specs:
            continue
        for k, v in specs.items():
            if k is None or v is None:
                continue
            attr = f"{str(k).strip().lower()}:{str(v).strip().lower()}"
            attr_idx = attr_to_idx.get(attr)
            if attr_idx is not None:
                H[node_idx, attr_idx] = 1.0

    return H


class LLMCHGNNDataset(Dataset):
    def __init__(self, records):
        self.records = list(records)

    def __len__(self):
        return len(self.records)

    def __getitem__(self, idx):
        return self.records[int(idx)]


def collate_graph_records(batch):
    # Keep records as list because candidate count can vary.
    return batch


def encode_graph_record(data, model_sbert, attr_to_idx, device):
    """
    Encode one query-candidate graph.

    Returns:
        X: (1, N, 768)
        H: (1, N, E)
        target_idx: index among candidates, or None
    """
    q_text = data["query_text"]
    candidate_texts = data["candidate_texts"]
    candidate_ids = data["candidate_ids"]
    true_vn_id = data["true_vn_id"]

    q_emb = model_sbert.encode(q_text, convert_to_tensor=True, device=device)
    c_embs = model_sbert.encode(candidate_texts, convert_to_tensor=True, device=device)

    X = torch.cat([q_emb.unsqueeze(0), c_embs], dim=0).unsqueeze(0).to(device)

    item_specs_list = [data.get("query_specs", {})] + data.get("candidate_specs", [])
    H = build_incidence_matrix(item_specs_list, attr_to_idx, device).unsqueeze(0)

    try:
        target_idx = candidate_ids.index(true_vn_id)
    except ValueError:
        target_idx = None

    return X, H, target_idx


def evaluate_llm_chgnn(model, dataset, model_sbert, attr_to_idx, device):
    """
    Rank candidates with trained LLM-CHGNN and compute HR@10/NDCG@10.
    Only Rank 0 should call this.
    """
    if not dataset:
        return 0.0, 0.0

    model.eval()
    base_model = model.module if hasattr(model, "module") else model

    hits_at_10 = 0
    ndcg_at_10 = 0.0
    total = len(dataset)

    with torch.no_grad():
        for data in tqdm(dataset, desc="LLM-CHGNN Evaluation", disable=(TrainingConfig.RANK != 0)):
            X, H, target_idx = encode_graph_record(data, model_sbert, attr_to_idx, device)
            X_out = base_model(X, H).squeeze(0)

            q_vec = X_out[0]
            c_vecs = X_out[1:]

            scores = torch.sum(q_vec.unsqueeze(0) * c_vecs, dim=1).detach().cpu().numpy()
            ranked_indices = list(np.argsort(scores)[::-1])

            if target_idx is None:
                continue

            try:
                rank = ranked_indices.index(target_idx) + 1
                if rank <= 10:
                    hits_at_10 += 1
                    ndcg_at_10 += 1.0 / np.log2(rank + 1)
            except ValueError:
                pass

    return hits_at_10 / total, ndcg_at_10 / total


def train_llm_chgnn(train_dataset, eval_dataset=None):

    device = TrainingConfig.DEVICE
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE

    logger.info(f"\n\n\nTraining LLM-CHGNN on device {device}\n\n\n")

    if eval_dataset is None:
        raise ValueError("LLM-CHGNN cần eval_dataset riêng, không dùng train_dataset để evaluate.")
    eval_dataset = list(eval_dataset)

    max_attrs = TrainingConfig.LLM_CHGNN_MAX_ATTRS
    # attr_vocab = build_attr_vocab(train_dataset, max_attrs=max_attrs)
    attr_vocab = build_attr_vocab(eval_dataset, max_attrs=max_attrs)
    attr_to_idx = {attr: i for i, attr in enumerate(attr_vocab)}

    if rank == 0:
        logger.info(f"LLM-CHGNN Attribute Vocab Size: {len(attr_vocab)}")
        save_attr_vocab("llm_chgnn", attr_vocab)

    graph_batch_size = TrainingConfig.LLM_CHGNN_BATCH_SIZE
    loader = DataLoader( train_dataset, batch_size=graph_batch_size, shuffle=False, num_workers=1, 
                        pin_memory=True, drop_last=True, collate_fn=collate_graph_records, 
                        persistent_workers=True)

    model_sbert = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2", device=device)
    model = LLM_CHGNN(in_features=768).to(device)

    if world_size > 1:
        if not torch.distributed.is_initialized():
            torch.distributed.init_process_group(backend="nccl")
        model = DDP(model, device_ids=[device.index])

    local_batches = torch.tensor([max(1, len(loader))], device=device, dtype=torch.float32)

    if torch.distributed.is_initialized():
        torch.distributed.all_reduce(local_batches, op=torch.distributed.ReduceOp.MIN)

    sync_train_batches = int(local_batches.item())

    logger.info(
        f"Rank {rank}: train_records≈{len(train_dataset):,}, "
        f"local_batches={len(loader)}"
    )

    optimizer = optim.Adam( model.parameters(),
                            lr=TrainingConfig.LLM_CHGNN_LR,
                            weight_decay=TrainingConfig.LLM_CHGNN_WEIGHT_DECAY)

    criterion = nn.CrossEntropyLoss()

    if rank == 0:
        download_resume_checkpoint("llm_chgnn")

    if torch.distributed.is_initialized():
        torch.distributed.barrier()

    resume = load_resume_checkpoint("llm_chgnn", model, optimizer, device=device)

    start_epoch = resume["start_epoch"]
    best_hr10 = resume["best_metric"]
    metrics_rows = resume["history"]

    if rank == 0:
        logger.info(">>> BẮT ĐẦU HUẤN LUYỆN LLM-CHGNN...")

    for epoch in range(start_epoch, TrainingConfig.EPOCHS):
        model.train()

        total_loss = 0.0
        total_batches = min(len(loader), sync_train_batches)

        for batch_idx, batch_records in enumerate(loader, start=1):
            if batch_idx > sync_train_batches:
                break

            batch_losses = []
            optimizer.zero_grad(set_to_none=True)

            for data in batch_records:
                X, H, target_idx = encode_graph_record(data, model_sbert, attr_to_idx, device)

                if target_idx is None:
                    continue

                X_out = model(X, H).squeeze(0)

                q_vec = X_out[0]
                c_vecs = X_out[1:]

                logits = torch.sum(q_vec.unsqueeze(0) * c_vecs, dim=1).unsqueeze(0)

                target = torch.tensor([target_idx], dtype=torch.long, device=device)

                loss = criterion(logits, target)
                batch_losses.append(loss)

            if not batch_losses:
                continue

            loss = torch.stack(batch_losses).mean()
            loss.backward()
            optimizer.step()

            total_loss += float(loss.item())

            if rank == 0 and (batch_idx % 20 == 0 or batch_idx == total_batches):
                logger.info(
                    f"Epoch {epoch+1}/{TrainingConfig.EPOCHS} | "
                    f"Batch {batch_idx}/{total_batches} | "
                    f"Loss={loss.item():.4f}"
                )

        if torch.distributed.is_initialized():
            torch.distributed.barrier()

        loss_tensor = torch.tensor(
            [total_loss, max(total_batches, 1)],
            dtype=torch.float32,
            device=device
        )

        if torch.distributed.is_initialized():
            torch.distributed.all_reduce(loss_tensor, op=torch.distributed.ReduceOp.SUM)

        global_avg_loss = (loss_tensor[0] / loss_tensor[1].clamp_min(1)).item()

        if rank == 0:
            hr10, ndcg10 = evaluate_llm_chgnn( model=model, dataset=eval_dataset, model_sbert=model_sbert, attr_to_idx=attr_to_idx, device=device )

            logger.info(
                f"LLM-CHGNN EPOCH {epoch+1} DONE | "
                f"HR@10={hr10:.4f} | NDCG@10={ndcg10:.4f}"
            )

            current_metrics = {
                "baseline": "llm_chgnn",
                "epoch": epoch + 1,
                "hr10": hr10,
                "ndcg10": ndcg10,
                "loss": global_avg_loss,
                "data_fraction": getattr(TrainingConfig, "DATA_FRACTION", "")
            }

            metrics_rows.append(current_metrics)

            metrics_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "llm_chgnn_metrics.csv")
            write_metrics_csv(metrics_path, metrics_rows)
            upload_file_to_gcs(metrics_path)

            best_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "llm_chgnn_best.pt")

            if hr10 > best_hr10 or not os.path.exists(best_path):
                best_hr10 = hr10
                save_best_model(
                    model_name="llm_chgnn",
                    model=model,
                    epoch=epoch,
                    metrics=current_metrics
                )

            save_resume_checkpoint(
                model_name="llm_chgnn",
                model=model,
                optimizer=optimizer,
                epoch=epoch,
                best_metric=best_hr10,
                history=metrics_rows
            )

        if torch.distributed.is_initialized():
            torch.distributed.barrier()

    return os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "llm_chgnn_best.pt")


def run_llm_chgnn(train_dataset, eval_dataset=None):
    """
    Compatibility wrapper for existing main.py.

    Old behavior:
        run_llm_chgnn(dataset) only evaluated zero-shot.

    New behavior:
        run_llm_chgnn(dataset) trains LLM-CHGNN with DDP/checkpointing.
    """
    return train_llm_chgnn(train_dataset, eval_dataset)

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    try:
        eval_data = load_eval_dataset()
        run_llm_chgnn(eval_data)
    except Exception as e:
        logger.exception(f"Error running LLM-CHGNN: {e}")
