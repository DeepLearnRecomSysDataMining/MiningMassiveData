import logging
import torch
import torch.distributed as dist
import numpy as np
from tqdm import tqdm
from sentence_transformers import SentenceTransformer
from config.training_config import TrainingConfig

logger = logging.getLogger("sbert_ranker")

def get_ndcg_at_k(rank, k=10):
    return 1.0 / np.log2(rank + 1) if rank <= k else 0.0

def run_sbert(dataset):
    """
    Baseline 2: SBERT Inference (Pre-trained).
    Evaluates on evaluation_dataset.pkl.
    """
    device = TrainingConfig.DEVICE
    model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2', device=device)
    
    # Distributed Chunking
    chunk = dataset[TrainingConfig.RANK::TrainingConfig.WORLD_SIZE]
    
    # Pre-compute VN unique embeddings
    unique_vn = {vid: vtext for d in dataset for vid, vtext in zip(d['candidate_ids'], d['candidate_texts'])}
    vn_ids, vn_texts = list(unique_vn.keys()), list(unique_vn.values())
    vn_embs = model.encode(vn_texts, batch_size=128, convert_to_tensor=True)
    vn_emb_dict = {vid: vn_embs[i] for i, vid in enumerate(vn_ids)}
    
    local_hits, local_ndcg = 0, 0.0
    with torch.no_grad():
        for data in tqdm(chunk, desc=f"SBERT Rank {TrainingConfig.RANK}"):
            query_emb = model.encode(data['query_text'], convert_to_tensor=True)
            candidate_embs = torch.stack([vn_emb_dict[vid] for vid in data['candidate_ids']])
            
            cos_scores = torch.nn.functional.cosine_similarity(query_emb.unsqueeze(0), candidate_embs).cpu().numpy()
            cat_scores = np.array([1.0 if cat == data['query_category'] else 0.0 for cat in data['candidate_categories']])
            combined = 0.7 * cos_scores + 0.3 * cat_scores * np.max(cos_scores)
            
            ranked_ids = [data['candidate_ids'][i] for i in np.argsort(combined)[::-1]]
            try:
                rank = ranked_ids.index(data['true_vn_id']) + 1
                if rank <= 10: local_hits += 1
                local_ndcg += get_ndcg_at_k(rank)
            except ValueError: pass

    # Sync results
    res = torch.tensor([local_hits, local_ndcg], device=device)
    dist.all_reduce(res, op=dist.ReduceOp.SUM)
    
    if TrainingConfig.RANK == 0:
        logger.info(f"SBERT Result -> HR@10: {res[0].item()/len(dataset):.4f} | NDCG@10: {res[1].item()/len(dataset):.4f}")
