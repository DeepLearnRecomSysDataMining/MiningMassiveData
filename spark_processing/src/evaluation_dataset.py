# ============================================================
# src/evaluation_dataset.py
# New module to generate the matching dataset (1 true + 99 negatives)
# ============================================================

import os
import logging
import random
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("evaluation_dataset")

def run_evaluation_generator(spark: SparkSession, item_nodes_path: str, output_path: str, num_candidates: int = 100):
    """
    Generates evaluation dataset: for each Amazon item, find its VN counterpart (true) 
    and pick 99 negatives (70% same category, 30% others).
    """
    logger.info("Bắt đầu tạo bộ dữ liệu Evaluation...")

    # 1. Đọc item_nodes
    df_items = spark.read.parquet(item_nodes_path)

    # 2. Tách Amazon và VN
    df_amz = df_items.filter(col("domain") == "amazon").alias("amz")
    df_vn = df_items.filter(col("domain") == "vn").alias("vn")

    # 3. Join để tìm Ground Truth (khớp theo ASIN)
    df_ground_truth = df_amz.join(
        df_vn.select(col("asin").alias("vn_asin"), col("product_id").alias("true_vn_id"), col("category").alias("vn_cat")),
        col("amz.asin") == col("vn_asin"),
        "inner"
    )

    if df_ground_truth.count() == 0:
        logger.warning("Không tìm thấy cặp Amazon-VN nào khớp ASIN!")
        return 0

    # 4. Negative Mining Logic
    # Lấy danh sách tất cả VN IDs theo category
    vn_catalog = df_vn.select("product_id", "category", "full_text", "parsed_specs").collect()
    vn_cat_map = {}
    all_vn_ids = []
    vn_data_map = {}

    for row in vn_catalog:
        vid = row['product_id']
        cat = row['category']
        all_vn_ids.append(vid)
        vn_cat_map.setdefault(cat, []).append(vid)
        vn_data_map[vid] = {
            "full_text": row['full_text'],
            "category": row['category'],
            "parsed_specs": row['parsed_specs']
        }

    # 5. Xây dựng dataset (sử dụng broadcast hoặc collect nếu data nhỏ, hoặc dùng Spark logic)
    # Vì logic negative mining khá phức tạp (random sample), ta sẽ dùng UDF hoặc xử lý qua list nếu tập query không quá lớn.
    # Tuy nhiên để đúng chất Spark, ta sẽ cố gắng dùng join/window hoặc xử lý theo partition.
    
    queries = df_ground_truth.select(
        col("asin").alias("query_id"),
        col("full_text").alias("query_text"),
        col("product_name").alias("query_name"),
        col("parsed_specs").alias("query_specs"),
        col("category").alias("query_category"),
        col("true_vn_id")
    ).collect()

    evaluation_data = []
    
    for q in queries:
        true_id = q['true_vn_id']
        q_cat = q['query_category']
        
        same_cat_ids = [vid for vid in vn_cat_map.get(q_cat, []) if vid != true_id]
        other_cat_ids = [vid for vid in all_vn_ids if vid != true_id and vid not in same_cat_ids]
        
        n_hard = min(int(num_candidates * 0.7), len(same_cat_ids))
        n_easy = (num_candidates - 1) - n_hard
        
        if n_easy > len(other_cat_ids):
            n_easy = len(other_cat_ids)
            n_hard = (num_candidates - 1) - n_easy
            
        if len(same_cat_ids) < n_hard or len(other_cat_ids) < n_easy:
            continue # Không đủ candidate
            
        negatives = random.sample(same_cat_ids, n_hard) + random.sample(other_cat_ids, n_easy)
        candidate_ids = [true_id] + negatives
        random.shuffle(candidate_ids)
        
        evaluation_data.append({
            "query_id": q['query_id'],
            "query_text": q['query_text'],
            "query_name": q['query_name'],
            "query_specs": q['query_specs'],
            "query_category": q_cat,
            "true_vn_id": true_id,
            "candidate_ids": candidate_ids,
            "candidate_texts": [vn_data_map[vid]['full_text'] for vid in candidate_ids],
            "candidate_categories": [vn_data_map[vid]['category'] for vid in candidate_ids],
            "candidate_specs": [vn_data_map[vid]['parsed_specs'] for vid in candidate_ids],
        })

    if not evaluation_data:
        logger.warning("Không tạo được bộ dataset nào đủ 100 ứng viên!")
        return 0

    # Chuyển về Spark DataFrame để lưu Parquet
    df_eval = spark.createDataFrame(evaluation_data)
    df_eval.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Đã tạo {len(evaluation_data)} bộ kiểm thử.")
    return len(evaluation_data)
from pyspark.sql.functions import col
