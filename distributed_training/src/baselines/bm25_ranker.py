import logging
import numpy as np
from tqdm import tqdm
from rank_bm25 import BM25Okapi
from config.training_config import TrainingConfig

logger = logging.getLogger("bm25_ranker")

def get_ndcg_at_k(rank, k=10):
    return 1.0 / np.log2(rank + 1) if rank <= k else 0.0

def run_bm25(dataset):
    """
    Baseline 1: BM25 + Category Filter.
    Deterministic algorithm, no training required.
    """
    if TrainingConfig.RANK != 0: return 
    
    hits_at_10, ndcg_at_10 = 0, 0.0
    logger.info(">>> Starting BM25 Evaluation...")
    
    for data in tqdm(dataset, desc="BM25"):
        query_tokens = data['query_text'].split()
        candidate_tokens = [text.split() for text in data['candidate_texts']]
        
        bm25 = BM25Okapi(candidate_tokens)
        bm25_scores = bm25.get_scores(query_tokens)
        
        # Category bonus
        cat_scores = np.array([1.0 if cat == data['query_category'] else 0.0 for cat in data['candidate_categories']])
        combined = 0.7 * bm25_scores + 0.3 * cat_scores * (np.max(bm25_scores) if np.max(bm25_scores) > 0 else 1.0)
        
        ranked_ids = [data['candidate_ids'][i] for i in np.argsort(combined)[::-1]]
        
        try:
            rank = ranked_ids.index(data['true_vn_id']) + 1
            if rank <= 10: hits_at_10 += 1
            ndcg_at_10 += get_ndcg_at_k(rank)
        except ValueError: pass
        
    logger.info(f"BM25 Result -> HR@10: {hits_at_10/len(dataset):.4f} | NDCG@10: {ndcg_at_10/len(dataset):.4f}")
