# ============================================================
# src/evaluation_dataset.py
# Native Spark implementation of Negative Mining (1 True + 99 Negatives)
# ============================================================

import logging
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger("evaluation_dataset")

def run_evaluation_generator(spark: SparkSession, items_path: str, output_path: str, num_candidates: int = 100):
    """
    Generates evaluation dataset using 100% Native Spark logic.
    Optimized for stability by converting MAP to STRING.
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    logger.info("Bat dau tao bo du lieu Evaluation (Ultra-Stable Architecture)...")

    # 1. Đọc dữ liệu
    df_items = spark.read.parquet(items_path)

    # Tách Amazon (Query) và VN (Candidates)
    df_amz = df_items.filter(F.col("domain") == "amazon").select(
        F.col("asin").alias("query_id"),
        F.col("product_name").alias("query_name"),
        F.col("full_text").alias("query_text"),
        F.col("category").alias("query_category"),
        # Chuyển Map sang JSON String ngay từ đầu để ổn định tuyệt đối
        F.to_json(F.col("parsed_specs")).alias("query_specs")
    )
    
    df_vn = df_items.filter(F.col("domain") == "vn").select(
        F.col("product_id").alias("cand_id"),
        F.col("asin").alias("cand_asin"),
        F.col("product_name").alias("cand_name"),
        F.col("full_text").alias("cand_text"),
        F.col("category").alias("cand_category"),
        F.to_json(F.col("parsed_specs")).alias("cand_specs")
    )

    # 2. Tìm cặp Positive (Ground Truth) dựa trên ASIN
    df_pos = df_amz.join(df_vn, df_amz.query_id == df_vn.cand_asin, "inner") \
                   .withColumn("label", F.lit(1))
                   
    # Đếm số lượng query có cặp positive
    query_count = df_pos.select("query_id").distinct().count()
    if query_count == 0:
        logger.warning("Khong tim thay cap Amazon-VN nao khop ASIN!")
        return 0

    # 3. Tạo Negative Candidates
    logger.info(f"Dang thuc hien Mining cho {query_count} queries...")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    
    # Lấy mẫu 2% VN items làm ứng viên
    df_vn_reduced = df_vn.select("cand_id", "cand_asin", "cand_category").sample(False, 0.05, 42)
    
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

    # 4. Gộp Positive và Negative
    df_final_ids = df_pos.select("query_id", "cand_id", "label") \
                         .unionByName(df_negatives.select("query_id", "cand_id", "label"))

    # 5. Gom nhóm
    df_grouped = df_final_ids.groupBy("query_id").agg(
        F.collect_list("cand_id").alias("candidate_ids"),
        F.collect_list("label").alias("labels")
    )

    # 6. Join Metadata cuối cùng
    # Lấy thêm cả text và specs của query
    df_amz_meta = df_amz.select("query_id", "query_name", "query_text", "query_category", "query_specs")
    df_eval = df_grouped.join(F.broadcast(df_amz_meta), "query_id", "inner")

    # 7. Ghi dữ liệu (Dùng coalesce(1) để gom thành 1 file duy nhất cho gọn vì dữ liệu nhỏ)
    logger.info(f"Dang ghi {query_count} queries xuong GCS...")
    df_eval.coalesce(1).write.mode("overwrite").parquet(output_path)
    
    logger.info(f"V HOAN TAT: Da tao bo du lieu Evaluation tai {output_path}")
    return query_count
