# ============================================================
# src/evaluation_dataset.py
# Native Spark implementation of Negative Mining (1 True + 99 Negatives)
# ============================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("evaluation_dataset")

def run_evaluation_generator(spark: SparkSession, items_path: str, output_path: str, num_candidates: int = 100):
    """
    Generates evaluation dataset using 100% Native Spark logic.
    For each Amazon item, find its VN counterpart (true) and pick 99 negatives.
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    logger.info("Bat dau tao bo du lieu Evaluation (Native Mode - Lean Architecture)...")

    # 1. Đọc dữ liệu
    df_items = spark.read.parquet(items_path)

    # Tách Amazon (Query) và VN (Candidates)
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

    # 3. Tạo Negative Candidates bằng phương pháp "Extreme Lean Mining"
    logger.info(f"Dang thuc hien Mining cho {query_count} queries...")
    
    # Điều chỉnh partition về mức tối ưu cho cụm nhỏ
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    
    # Bước then chốt: Chỉ giữ lại Metadata của những Query thực sự cần thiết (Broadcast sau này)
    df_amz_meta_lean = df_amz.join(F.broadcast(df_pos.select("query_id").distinct()), "query_id", "inner")
    
    # Lấy mẫu 2% VN items làm ứng viên (Cực nhẹ)
    df_vn_reduced = df_vn.select("cand_id", "cand_asin", "cand_category").sample(False, 0.02, 42)
    
    # Join Query với tập ứng viên (Broadcast Join)
    df_negatives = df_vn_reduced.join(F.broadcast(df_pos.select("query_id", "query_category").distinct()), 
                                     df_vn_reduced.cand_category == df_pos.query_category, 
                                     "inner") \
                                .filter(F.col("query_id") != F.col("cand_asin"))
    
    # Lấy 99 negatives
    window_neg = Window.partitionBy("query_id").orderBy(F.rand())
    df_negatives = df_negatives.withColumn("rank", F.row_number().over(window_neg)) \
                               .filter(F.col("rank") <= (num_candidates - 1)) \
                               .withColumn("label", F.lit(0))

    # 4. Gộp Positive và Negative (Chỉ dùng ID để Shuffle nhẹ)
    df_final_ids = df_pos.select("query_id", "cand_id", "cand_category", "label") \
                         .unionByName(df_negatives.select("query_id", "cand_id", "cand_category", "label"))

    # 5. Gom nhóm (Aggregation)
    logger.info("Dang gom nhom ID (Shuffle-light)...")
    df_grouped = df_final_ids.groupBy("query_id").agg(
        F.collect_list("cand_id").alias("candidate_ids"),
        F.collect_list("cand_category").alias("candidate_categories"),
        F.collect_list("label").alias("labels")
    )

    # 6. Join ngược lại Metadata (Sử dụng Broadcast để chốt hạ OOM)
    df_eval = df_grouped.join(F.broadcast(df_amz_meta_lean), "query_id", "inner")

    # 7. Lưu xuống Parquet
    logger.info(f"Dang ghi {query_count} queries xuong GCS: {output_path}")
    df_eval.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"V HOAN TAT: Da tao bo du lieu Evaluation tai {output_path}")
    return query_count
