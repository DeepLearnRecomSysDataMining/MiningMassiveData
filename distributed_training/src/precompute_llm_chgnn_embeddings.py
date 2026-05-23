import os
import gc
import time
import pickle
import logging
import subprocess
from datetime import timedelta

import numpy as np
import torch.distributed as dist
import pyarrow.parquet as pq
import gcsfs
from sentence_transformers import SentenceTransformer

from config.training_config import TrainingConfig
from src.data_utils import load_eval_dataset

logger = logging.getLogger("precompute_llm_chgnn")


def check_gcs_file_exists(gcs_path: str) -> bool:
    result = subprocess.run(["gsutil", "-q", "stat", gcs_path], capture_output=True)
    return result.returncode == 0


def _key_amz(x):
    x = str(x or "").strip()
    return f"amz_{x}" if x else None


def _key_vn(x):
    x = str(x or "").strip()
    return f"vn_{x}" if x else None


def _collect_train_pairs_from_batch(record_batch):
    """
    Return list[(embedding_id, text)] từ batch parquet.
    Không load toàn bộ dataset.
    """
    pairs = []

    for row in record_batch.to_pylist():
        query_asin = row.get("query_asin") or row.get("query_id")
        query_text = row.get("query_text") or ""
        q_key = _key_amz(query_asin)

        if q_key and query_text:
            pairs.append((q_key, str(query_text)))

        candidate_ids = row.get("candidate_ids") or []
        candidate_texts = row.get("candidate_texts") or []

        n = min(len(candidate_ids), len(candidate_texts))

        for cid, ctext in zip(candidate_ids[:n], candidate_texts[:n]):
            c_key = _key_vn(cid)
            if c_key and ctext:
                pairs.append((c_key, str(ctext)))

    return pairs


def _collect_eval_pairs(eval_dataset):
    pairs = []

    for row in eval_dataset:
        query_asin = (
            row.get("query_asin")
            or row.get("query_id")
            or row.get("asin")
        )
        query_text = row.get("query_text") or ""
        q_key = _key_amz(query_asin)

        if q_key and query_text:
            pairs.append((q_key, str(query_text)))

        candidate_ids = row.get("candidate_ids") or []
        candidate_texts = row.get("candidate_texts") or []

        n = min(len(candidate_ids), len(candidate_texts))

        for cid, ctext in zip(candidate_ids[:n], candidate_texts[:n]):
            c_key = _key_vn(cid)
            if c_key and ctext:
                pairs.append((c_key, str(ctext)))

    return pairs


def _dedup_pairs(pairs):
    seen = set()
    ids = []
    texts = []

    for pid, text in pairs:
        if not pid or not text:
            continue
        if pid in seen:
            continue
        seen.add(pid)
        ids.append(pid)
        texts.append(text)

    return ids, texts


def _save_chunk(chunk_name, ids, embs):
    local_dir = TrainingConfig.LOCAL_DATA_DIR
    gcs_dir = f"{TrainingConfig.GCS_LLM_CHGNN_EMBEDDINGS}/chunks"

    os.makedirs(local_dir, exist_ok=True)

    local_npy = os.path.join(local_dir, f"{chunk_name}.npy")
    local_pkl = os.path.join(local_dir, f"{chunk_name}.pkl")
    done_flag = f"{gcs_dir}/{chunk_name}_done.txt"

    index = {pid: i for i, pid in enumerate(ids)}

    np.save(local_npy, embs.astype("float32"))

    with open(local_pkl, "wb") as f:
        pickle.dump(index, f)

    subprocess.run(["gsutil", "cp", local_npy, f"{gcs_dir}/{chunk_name}.npy"], check=True)
    subprocess.run(["gsutil", "cp", local_pkl, f"{gcs_dir}/{chunk_name}.pkl"], check=True)
    subprocess.run(["gsutil", "cp", "/dev/null", done_flag], check=True)

    os.remove(local_npy)
    os.remove(local_pkl)


def merge_llm_chgnn_embedding_chunks():
    local_dir = TrainingConfig.LOCAL_DATA_DIR
    gcs_dir = f"{TrainingConfig.GCS_LLM_CHGNN_EMBEDDINGS}/chunks"

    final_npy_path = TrainingConfig.LLM_CHGNN_EMBEDDINGS_PATH
    final_pkl_path = TrainingConfig.LLM_CHGNN_INDEX_PATH

    os.makedirs(local_dir, exist_ok=True)

    subprocess.run(["gsutil", "-m", "cp", f"{gcs_dir}/*.npy", local_dir], check=True)
    subprocess.run(["gsutil", "-m", "cp", f"{gcs_dir}/*.pkl", local_dir], check=True)

    npy_files = sorted([
        f for f in os.listdir(local_dir)
        if f.startswith("llm_chgnn_") and f.endswith(".npy")
    ])

    seen = set()
    file_info = []
    total_rows = 0

    for npy_f in npy_files:
        npy_path = os.path.join(local_dir, npy_f)
        pkl_path = os.path.join(local_dir, npy_f.replace(".npy", ".pkl"))

        if not os.path.exists(pkl_path):
            raise FileNotFoundError(f"Missing index for {npy_f}")

        arr = np.load(npy_path, mmap_mode="r")

        with open(pkl_path, "rb") as f:
            chunk_index = pickle.load(f)

        unique_items = []

        for key, local_idx in chunk_index.items():
            if key in seen:
                continue
            if local_idx < 0 or local_idx >= arr.shape[0]:
                raise ValueError(f"Index out of range: {key} -> {local_idx} in {npy_f}")

            seen.add(key)
            unique_items.append((key, local_idx))

        file_info.append((npy_f, unique_items))
        total_rows += len(unique_items)

        logger.info(
            f"[CHECK] {npy_f}: vectors={arr.shape[0]:,}, "
            f"index={len(chunk_index):,}, kept_unique={len(unique_items):,}"
        )

    if total_rows == 0:
        raise ValueError("No LLM-CHGNN embeddings to merge.")

    fp = np.lib.format.open_memmap(
        final_npy_path,
        dtype="float32",
        mode="w+",
        shape=(total_rows, 768)
    )

    final_index = {}
    offset = 0

    for npy_f, unique_items in file_info:
        arr = np.load(os.path.join(local_dir, npy_f), mmap_mode="r")

        for key, local_idx in unique_items:
            fp[offset] = arr[local_idx]
            final_index[key] = offset
            offset += 1

        os.remove(os.path.join(local_dir, npy_f))
        os.remove(os.path.join(local_dir, npy_f.replace(".npy", ".pkl")))

    fp.flush()
    del fp

    with open(final_pkl_path, "wb") as f:
        pickle.dump(final_index, f)

    gcs_out = TrainingConfig.GCS_LLM_CHGNN_EMBEDDINGS
    subprocess.run(["gsutil", "cp", final_npy_path, f"{gcs_out}/llm_chgnn_embeddings.npy"], check=True)
    subprocess.run(["gsutil", "cp", final_pkl_path, f"{gcs_out}/llm_chgnn_index.pkl"], check=True)
    subprocess.run(["gsutil", "cp", "/dev/null", f"{gcs_out}/_final_done.txt"], check=True)

    logger.info(f"LLM-CHGNN embedding merge done: {offset:,} vectors")


def precompute_llm_chgnn_embeddings():
    rank = TrainingConfig.RANK
    world_size = TrainingConfig.WORLD_SIZE
    device = TrainingConfig.DEVICE

    if world_size > 1 and not dist.is_initialized():
        dist.init_process_group(backend="nccl", timeout=timedelta(minutes=180))

    gcs_out = TrainingConfig.GCS_LLM_CHGNN_EMBEDDINGS

    final_npy_gcs = f"{gcs_out}/llm_chgnn_embeddings.npy"
    final_pkl_gcs = f"{gcs_out}/llm_chgnn_index.pkl"
    final_done = f"{gcs_out}/_final_done.txt"

    final_ready = ( check_gcs_file_exists(final_done) and check_gcs_file_exists(final_npy_gcs) 
                   and check_gcs_file_exists(final_pkl_gcs)
                   )

    if final_ready:
        logger.info(
            "LLM-CHGNN final embeddings already exist on GCS "
            "(.npy + .pkl + _final_done). Skip precompute."
        )
        return

    if check_gcs_file_exists(final_done) and not final_ready:
        logger.warning(
            "Found _final_done.txt but missing llm_chgnn_embeddings.npy "
            "or llm_chgnn_index.pkl. Rebuilding LLM-CHGNN embeddings."
        )

    path = (
        TrainingConfig.GCS_LLM_CHGNN_TRAIN
        if TrainingConfig.IS_CLOUD
        else "data/llm_chgnn_train_dataset"
    )

    fs = gcsfs.GCSFileSystem() if TrainingConfig.IS_CLOUD else None
    arrow_path = path.replace("gs://", "") if TrainingConfig.IS_CLOUD else path

    dataset = pq.ParquetDataset(arrow_path, filesystem=fs)
    fragments = list(dataset.fragments)
    my_fragments = fragments[rank::world_size] if world_size > 1 else fragments

    model = SentenceTransformer("paraphrase-multilingual-mpnet-base-v2", device=device)

    cols = ["query_asin", "query_text", "candidate_ids", "candidate_texts"]

    for frag_idx, frag in enumerate(my_fragments):
        chunk_name = f"llm_chgnn_train_rank{rank}_frag{frag_idx}"
        done_flag = f"{gcs_out}/chunks/{chunk_name}_done.txt"

        if check_gcs_file_exists(done_flag):
            logger.info(f"[SKIP] {chunk_name} already done.")
            continue

        all_pairs = []

        for record_batch in frag.to_batches(columns=cols, batch_size=1024):
            all_pairs.extend(_collect_train_pairs_from_batch(record_batch))
            del record_batch

        ids, texts = _dedup_pairs(all_pairs)

        if not ids:
            subprocess.run(["gsutil", "cp", "/dev/null", done_flag], check=True)
            continue

        logger.info(f"[START] Encoding {chunk_name}: {len(ids):,} unique texts")

        embs = model.encode(
            texts,
            batch_size=512,
            convert_to_numpy=True,
            show_progress_bar=False
        )

        if len(ids) != embs.shape[0]:
            raise ValueError(f"Mismatch ids/vectors in {chunk_name}")

        _save_chunk(chunk_name, ids, embs)

        del all_pairs, ids, texts, embs
        gc.collect()

    # Eval embeddings: rank 0 làm riêng để evaluate không cần SBERT encode lại.
    if rank == 0:
        eval_chunk = "llm_chgnn_eval"
        eval_done = f"{gcs_out}/chunks/{eval_chunk}_done.txt"

        if not check_gcs_file_exists(eval_done):
            eval_dataset = load_eval_dataset()
            ids, texts = _dedup_pairs(_collect_eval_pairs(eval_dataset))

            if ids:
                logger.info(f"[START] Encoding eval embeddings: {len(ids):,} unique texts")
                embs = model.encode(
                    texts,
                    batch_size=512,
                    convert_to_numpy=True,
                    show_progress_bar=False
                )
                _save_chunk(eval_chunk, ids, embs)
                del ids, texts, embs
                gc.collect()
            else:
                subprocess.run(["gsutil", "cp", "/dev/null", eval_done], check=True)

    if world_size > 1:
        dist.barrier()

    if rank == 0:
        logger.info("Merging LLM-CHGNN embedding chunks...")
        merge_llm_chgnn_embedding_chunks()

    if world_size > 1:
        if rank != 0:
            max_wait = 7200
            elapsed = 0
            while not check_gcs_file_exists(final_done) and elapsed < max_wait:
                time.sleep(60)
                elapsed += 60
            if not check_gcs_file_exists(final_done):
                raise TimeoutError("Timeout waiting for LLM-CHGNN final embeddings.")
        dist.barrier()


if __name__ == "__main__":
    precompute_llm_chgnn_embeddings()