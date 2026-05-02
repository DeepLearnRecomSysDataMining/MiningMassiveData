# ============================================================
# src/evaluation_dataset.py
# Native Spark implementation of Negative Mining (1 True + 99 Negatives)
# ============================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("evaluation_dataset")

def run_evaluation_generator(spark: SparkSession, item_nodes_path: str, output_path: str, num_candidates: int = 100):
    """
    Generates evaluation dataset using 100% Native Spark logic.
    For each Amazon item, find its VN counterpart (true) and pick 99 negatives.
    """
    logger.info("Bat dau tao bo du lieu Evaluation (Native Mode)...")

    # 1. Đọc dữ liệu
    df_items = spark.read.parquet(item_nodes_path)

    # Tách Amazon (Query) và VN (Candidates)
    # Chỉ lấy các cột cần thiết để nhẹ bộ nhớ
    df_amz = df_items.filter(F.col("domain") == "amazon").select(
        F.col("asin").alias("query_id"),
        F.col("product_name").alias("query_name"),
        F.col("full_text").alias("query_text"),
        F.col("category").alias("query_category"),
        F.col("parsed_specs").alias("query_specs")
    )
    
    df_vn = df_items.filter(F.col("domain") == "vn").select(
        F.col("product_id").alias("cand_id"),
        F.col("asin").alias("cand_asin"),
        F.col("product_name").alias("cand_name"),
        F.col("full_text").alias("cand_text"),
        F.col("category").alias("cand_category"),
        F.col("parsed_specs").alias("cand_specs")
    )

    # 2. Tìm cặp Positive (Ground Truth) dựa trên ASIN
    df_pos = df_amz.join(df_vn, df_amz.query_id == df_vn.cand_asin, "inner") \
                   .withColumn("label", F.lit(1))
                   
    # Đếm số lượng query có cặp positive
    query_count = df_pos.select("query_id").distinct().count()
    if query_count == 0:
        logger.warning("Khong tim thay cap Amazon-VN nao khop ASIN!")
        return 0

    # 3. Tạo Negative Candidates bằng phương pháp "Hard Negative Mining" (Join theo Category)
    logger.info("Dang thuc hien Hard Negative Mining theo Category...")
    
    # Lấy danh sách các Query (Amazon) duy nhất đã tìm thấy cặp Positive
    df_queries = df_pos.select("query_id", "query_name", "query_text", "query_category", "query_specs").distinct()
    
    # Thực hiện Inner Join trên cột Category
    df_negatives = df_queries.join(df_vn, df_queries.query_category == df_vn.cand_category, "inner") \
                             .filter(F.col("query_id") != F.col("cand_asin"))
    
    # Lấy 99 đối thủ "khó" nhất (cùng category) cho mỗi query
    window_spec = Window.partitionBy("query_id").orderBy(F.rand())
    
    df_negatives = df_negatives.withColumn("rank", F.row_number().over(window_spec)) \
                               .filter(F.col("rank") <= (num_candidates - 1)) \
                               .withColumn("label", F.lit(0))

    # Trường hợp dự phòng: Nếu một số query không đủ 99 negatives từ cùng category (rất hiếm)
    # Ta có thể bổ sung thêm random, nhưng với 4M items thì cùng category thường là đủ.

    # 4. Gộp Positive và Negative
    common_cols = ["query_id", "query_name", "query_text", "query_category", "query_specs", 
                   "cand_id", "cand_name", "cand_text", "cand_category", "cand_specs", "label"]
    
    df_final = df_pos.select(*common_cols).unionByName(df_negatives.select(*common_cols))

    # 5. Group lại thành dạng 1 dòng cho mỗi query
    df_eval = df_final.groupBy("query_id", "query_name", "query_text", "query_category").agg(
        F.first("query_specs").alias("query_specs"),
        F.collect_list("cand_id").alias("candidate_ids"),
        F.collect_list("cand_text").alias("candidate_texts"),
        F.collect_list("cand_category").alias("candidate_categories"),
        F.collect_list("cand_specs").alias("candidate_specs"),
        F.collect_list("label").alias("labels")
    )

    # 6. Lưu xuống Parquet
    logger.info(f"Dang ghi {query_count} queries (Hard Negatives) xuong GCS...")
    df_eval.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"V HOAN TAT: Da tao bo kiem thu Hard Negatives cho {query_count} queries.")
    return query_count
