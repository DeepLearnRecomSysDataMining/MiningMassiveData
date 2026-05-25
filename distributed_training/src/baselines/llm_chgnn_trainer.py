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
import pyarrow.parquet as pq
import gcsfs
from collections import Counter
import hashlib

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
    

def make_context_key(prefix, item_id, text):
    item_id = str(item_id or "").strip()
    text = str(text or "").strip()

    if not item_id or not text:
        return None

    h = hashlib.md5(text.encode("utf-8")).hexdigest()[:12]

    return f"{prefix}_{item_id}_{h}"

def _normalize_attr(k, v):
    if k is None or v is None:
        return None
    attr = f"{str(k).strip().lower()}:{str(v).strip().lower()}"
    if attr == ":":
        return None
    return attr


def normalize_specs(specs):
    if specs is None:
        return {}

    if isinstance(specs, dict):
        return {
            str(k): str(v)
            for k, v in specs.items()
            if k is not None and v is not None
        }

    if isinstance(specs, list):
        out = {}

        for item in specs:
            if isinstance(item, tuple) and len(item) == 2:
                k, v = item
                if k is not None and v is not None:
                    out[str(k)] = str(v)

            elif isinstance(item, dict):
                if "key" in item and "value" in item:
                    k, v = item.get("key"), item.get("value")
                    if k is not None and v is not None:
                        out[str(k)] = str(v)
                else:
                    for k, v in item.items():
                        if k is not None and v is not None:
                            out[str(k)] = str(v)

        return out

    return {}


def build_attr_vocab_from_train_parquet(max_attrs: int = 5000, arrow_batch_size: int = 1024):
    """
    Build attr_vocab từ toàn bộ TRAIN parquet.
    Chỉ scan query_specs + candidate_specs, không load toàn bộ train vào RAM.
    Chỉ rank 0 nên gọi hàm này.
    """
    path = (
        TrainingConfig.GCS_LLM_CHGNN_TRAIN
        if TrainingConfig.IS_CLOUD
        else "data/llm_chgnn_train_dataset"
    )

    fs = gcsfs.GCSFileSystem() if TrainingConfig.IS_CLOUD else None
    arrow_path = path.replace("gs://", "") if TrainingConfig.IS_CLOUD else path

    dataset = pq.ParquetDataset(arrow_path, filesystem=fs)
    attr_counts = Counter()

    cols = ["query_specs", "candidate_specs"]

    for frag_idx, frag in enumerate(dataset.fragments, start=1):
        for record_batch in frag.to_batches(columns=cols, batch_size=arrow_batch_size):
            for row in record_batch.to_pylist():
                specs_list = [row.get("query_specs", {})] + (row.get("candidate_specs", []) or [])

                for specs in specs_list:
                    specs = normalize_specs(specs)

                    if not specs:
                        continue

                    for k, v in specs.items():
                        attr = _normalize_attr(k, v)
                        if attr is not None:
                            attr_counts[attr] += 1

            del record_batch

        logger.info(
            f"Rank {TrainingConfig.RANK}: scanned train fragment "
            f"{frag_idx}/{len(dataset.fragments)} for attr_vocab"
        )

    sorted_attrs = sorted(attr_counts.items(), key=lambda x: (-x[1], x[0]))
    return [attr for attr, _ in sorted_attrs[:max_attrs]]


def load_or_build_train_attr_vocab(max_attrs: int = 5000):
    """
    Single-node multi-GPU:
    - Rank 0 build attr_vocab từ toàn bộ train parquet.
    - Các rank khác đợi barrier rồi load cùng file local.
    - Ghi file atomic để tránh rank khác đọc file đang ghi dở.
    """
    os.makedirs(TrainingConfig.LOCAL_DATA_DIR, exist_ok=True)

    local_path = os.path.join(
        TrainingConfig.LOCAL_DATA_DIR,
        "llm_chgnn_attr_vocab.json"
    )
    tmp_path = local_path + ".tmp"

    is_dist = torch.distributed.is_available() and torch.distributed.is_initialized()

    if TrainingConfig.RANK == 0:
        rebuild = True

        if os.path.exists(local_path):
            try:
                logger.info(f"Loading existing train attr_vocab from {local_path}")
                with open(local_path, "r", encoding="utf-8") as f:
                    attr_vocab = json.load(f)

                if attr_vocab:
                    rebuild = False
                else:
                    logger.warning("Existing attr_vocab is empty. Rebuilding...")
            except Exception as e:
                logger.warning(f"Failed to load existing attr_vocab: {e}. Rebuilding...")

        if rebuild:
            logger.info("Building attr_vocab from TRAIN parquet, not eval dataset...")
            attr_vocab = build_attr_vocab_from_train_parquet(max_attrs=max_attrs)

            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(attr_vocab, f, ensure_ascii=False, indent=2)

            os.replace(tmp_path, local_path)

            logger.info(f"Saved train attr_vocab to {local_path}")

        save_attr_vocab("llm_chgnn", attr_vocab)

    if is_dist:
        torch.distributed.barrier()

    if not os.path.exists(local_path):
        raise FileNotFoundError(
            f"Rank {TrainingConfig.RANK}: missing attr_vocab after barrier: {local_path}"
        )

    with open(local_path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_attr_vocab(dataset, max_attrs: int = 5000):
    """
    Build a global attribute vocabulary from query_specs + candidate_specs.
    The vocab is capped to avoid exploding H matrix RAM/VRAM.
    """
    attr_counts = {}

    for data in dataset:
        specs_list = [data.get("query_specs", {})] + data.get("candidate_specs", [])
        # for specs in specs_list:
        #     if not specs:
        #         continue
        #     for k, v in specs.items():
        for specs in specs_list:
            specs = normalize_specs(specs)

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

    # for node_idx, specs in enumerate(item_specs_list):
    #     if not specs:
    #         continue
    #     for k, v in specs.items():
    for node_idx, specs in enumerate(item_specs_list):
        specs = normalize_specs(specs)

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

def encode_graph_record(data, embedding_lookup, attr_to_idx, device):
    query_asin = (
        data.get("query_asin")
        or data.get("query_id")
        or data.get("asin")
    )
    query_text = data.get("query_text", "") or ""

    q_key = make_context_key("amz", query_asin, query_text)

    candidate_ids = data.get("candidate_ids", []) or []
    candidate_texts = data.get("candidate_texts", []) or []
    candidate_specs = data.get("candidate_specs", []) or []

    candidate_ids = [str(x) for x in candidate_ids]
    candidate_texts = [str(x) for x in candidate_texts]
    candidate_specs = list(candidate_specs)

    true_vn_id = str(data.get("true_vn_id", ""))

    if not q_key or not candidate_ids or not candidate_texts:
        return None, None, None

    # n0 = min(len(candidate_ids), len(candidate_texts), len(candidate_specs))
    n0 = min(len(candidate_ids), len(candidate_texts))

    candidate_ids = candidate_ids[:n0]
    candidate_texts = candidate_texts[:n0]
    candidate_specs = candidate_specs[:n0]

    if len(candidate_specs) < n0:
        candidate_specs.extend([{} for _ in range(n0 - len(candidate_specs))])

    candidate_ids = candidate_ids[:n0]
    candidate_texts = candidate_texts[:n0]
    candidate_specs = candidate_specs[:n0]

    if n0 == 0 or true_vn_id not in candidate_ids:
        return None, None, None

    q_emb = embedding_lookup.get_embedding(q_key)

    c_embs = []
    valid_candidate_ids = []
    valid_candidate_specs = []

    for cid, ctext, specs in zip(candidate_ids, candidate_texts, candidate_specs):
        key = make_context_key("vn", cid, ctext)
        if not key:
            continue

        emb = embedding_lookup.get_embedding(key)

        c_embs.append(emb)
        valid_candidate_ids.append(cid)
        valid_candidate_specs.append(specs)

    if not c_embs or true_vn_id not in valid_candidate_ids:
        return None, None, None

    target_idx = valid_candidate_ids.index(true_vn_id)

    q_tensor = torch.from_numpy(np.asarray(q_emb, dtype=np.float32)).to(device)
    c_tensor = torch.from_numpy(np.stack(c_embs).astype(np.float32)).to(device)

    X = torch.cat([q_tensor.unsqueeze(0), c_tensor], dim=0).unsqueeze(0)

    item_specs_list = [data.get("query_specs", {}) or {}] + valid_candidate_specs
    H = build_incidence_matrix(item_specs_list, attr_to_idx, device).unsqueeze(0)

    if H.size(1) != X.size(1):
        return None, None, None

    return X, H, target_idx

def evaluate_llm_chgnn(model, dataset, embedding_lookup, attr_to_idx, device):
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
    valid_total = 0
    # total = len(dataset)

    with torch.no_grad():
        for data in tqdm(dataset, desc="LLM-CHGNN Evaluation", disable=(TrainingConfig.RANK != 0)):
            X, H, target_idx = encode_graph_record(data, embedding_lookup, attr_to_idx, device)

            if X is None or H is None or target_idx is None:
                continue

            valid_total += 1

            X_out = base_model(X, H).squeeze(0)

            q_vec = X_out[0]
            c_vecs = X_out[1:]

            scores = torch.sum(q_vec.unsqueeze(0) * c_vecs, dim=1).detach().cpu().numpy()
            ranked_indices = list(np.argsort(scores)[::-1])

            try:
                rank = ranked_indices.index(target_idx) + 1
                if rank <= 10:
                    hits_at_10 += 1
                    ndcg_at_10 += 1.0 / np.log2(rank + 1)
            except ValueError:
                pass

    if valid_total == 0:
        return 0.0, 0.0

    return hits_at_10 / valid_total, ndcg_at_10 / valid_total


def train_llm_chgnn(train_dataset, eval_dataset=None, embedding_lookup=None):

    device = TrainingConfig.DEVICE
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE

    is_dist = world_size > 1

    if is_dist and not torch.distributed.is_initialized():
        torch.distributed.init_process_group(backend="nccl")

    if torch.cuda.is_available():
        torch.cuda.set_device(TrainingConfig.LOCAL_RANK)

    logger.info(f"\n\n\nTraining LLM-CHGNN on device {device}\n\n\n")

    if embedding_lookup is None:
        raise ValueError("LLM-CHGNN cần precomputed embedding_lookup.")

    if eval_dataset is None:
        raise ValueError("LLM-CHGNN cần eval_dataset riêng, không dùng train_dataset để evaluate.")
    eval_dataset = list(eval_dataset)

    max_attrs = TrainingConfig.LLM_CHGNN_MAX_ATTRS

    # 1. Tất cả rank load cùng attr_vocab
    attr_vocab = load_or_build_train_attr_vocab(max_attrs=max_attrs)
    attr_to_idx = {attr: i for i, attr in enumerate(attr_vocab)}

    # 2. Rank 0 log/upload
    if rank == 0:
        logger.info(f"LLM-CHGNN Attribute Vocab Size: {len(attr_vocab)}")
        save_attr_vocab("llm_chgnn", attr_vocab)

    # 3. Bắt buộc chờ sau khi vocab/upload xong
    if torch.distributed.is_available() and torch.distributed.is_initialized():
        torch.distributed.barrier()

    # 4. Tạo model sau barrier để mọi rank vào cùng nhịp
    # model = LLM_CHGNN(in_features=768).to(device)

    # if torch.distributed.is_available() and torch.distributed.is_initialized():
    #     model = DDP(model, device_ids=[device.index])

    model = LLM_CHGNN(in_features=768).to(device)

    n_params = sum(p.numel() for p in model.parameters())
    logger.warning(f"Rank {rank}: LLM_CHGNN params before DDP = {n_params}")

    if torch.distributed.is_available() and torch.distributed.is_initialized():
        torch.distributed.barrier()
        model = DDP(model, device_ids=[TrainingConfig.LOCAL_RANK])

    graph_batch_size = TrainingConfig.LLM_CHGNN_BATCH_SIZE
    loader = DataLoader( train_dataset, batch_size=graph_batch_size, shuffle=False, num_workers=1, 
                        pin_memory=True, drop_last=True, collate_fn=collate_graph_records, 
                        persistent_workers=True, prefetch_factor=2 )

    local_batches = torch.tensor([max(1, len(loader))], device=device, dtype=torch.float32)

    if torch.distributed.is_initialized():
        torch.distributed.all_reduce(local_batches, op=torch.distributed.ReduceOp.MIN)

    sync_train_batches = int(local_batches.item())

    logger.info(
        f"Rank {rank}: train_records≈{len(train_dataset):,}, "
        f"local_batches={len(loader)}"
    )

    optimizer = optim.Adam( model.parameters(), lr=TrainingConfig.LLM_CHGNN_LR, weight_decay=TrainingConfig.LLM_CHGNN_WEIGHT_DECAY)

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

        valid_steps = 0

        for batch_idx, batch_records in enumerate(loader, start=1):
            if batch_idx > sync_train_batches:
                break

            batch_losses = []
            optimizer.zero_grad(set_to_none=True)

            for data in batch_records:
                X, H, target_idx = encode_graph_record(data, embedding_lookup, attr_to_idx, device)

                if X is None or H is None or target_idx is None:
                    continue

                X_out = model(X, H).squeeze(0)

                q_vec = X_out[0]
                c_vecs = X_out[1:]

                logits = torch.sum(q_vec.unsqueeze(0) * c_vecs, dim=1).unsqueeze(0)
                target = torch.tensor([target_idx], dtype=torch.long, device=device)

                loss = criterion(logits, target)
                batch_losses.append(loss)

            if batch_losses:
                loss = torch.stack(batch_losses).mean()
                loss.backward()
                optimizer.step()
                total_loss += float(loss.item())
                valid_steps += 1
                log_loss = loss.item()
            else:
                E = len(attr_to_idx)
                dummy_X = torch.zeros((1, 2, 768), dtype=torch.float32, device=device)
                dummy_H = torch.zeros((1, 2, E), dtype=torch.float32, device=device)
                dummy_out = model(dummy_X, dummy_H)
                dummy_loss = dummy_out.sum() * 0.0
                dummy_loss.backward()
                optimizer.step()
                log_loss = 0.0

            if rank == 0 and (batch_idx % 20 == 0 or batch_idx == total_batches):
                logger.info(
                    f"Epoch {epoch+1}/{TrainingConfig.EPOCHS} | "
                    f"Batch {batch_idx}/{total_batches} | "
                    f"Loss={log_loss:.4f}"
                )

        if torch.distributed.is_initialized():
            torch.distributed.barrier()

        loss_tensor = torch.tensor( [total_loss, max(valid_steps, 1)], dtype=torch.float32, device=device )

        if torch.distributed.is_initialized():
            torch.distributed.all_reduce(loss_tensor, op=torch.distributed.ReduceOp.SUM)

        global_avg_loss = (loss_tensor[0] / loss_tensor[1].clamp_min(1)).item()

        if rank == 0:
            hr10, ndcg10 = evaluate_llm_chgnn( model=model, dataset=eval_dataset, embedding_lookup=embedding_lookup, attr_to_idx=attr_to_idx, device=device )

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
                save_best_model( model_name="llm_chgnn", model=model, epoch=epoch, metrics=current_metrics )
            save_resume_checkpoint( model_name="llm_chgnn", model=model, optimizer=optimizer, epoch=epoch, 
                                   best_metric=best_hr10, history=metrics_rows )

        if torch.distributed.is_initialized():
            torch.distributed.barrier()

    return os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "llm_chgnn_best.pt")


def run_llm_chgnn(train_dataset, eval_dataset=None, embedding_lookup=None):
    return train_llm_chgnn(train_dataset, eval_dataset, embedding_lookup)
