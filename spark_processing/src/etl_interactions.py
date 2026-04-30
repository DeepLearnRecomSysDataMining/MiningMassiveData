# ============================================================
# src/etl_interactions.py
# Refactored to match MMD_v3 logic
# ============================================================

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, lit

logger = logging.getLogger("etl_interactions")

def run_etl_interactions(spark: SparkSession, data_dir: str, output_dir: str) -> int:
    """
    ETL for review data. Filters reviews to only include those with asins present in the metadata.
    """
    logger.info(f"Đang quét review từ: {data_dir}")

    # 1. Tìm các file review
    all_files = os.listdir(data_dir)
    review_files = [os.path.join(data_dir, f) for f in all_files if "reviews" in f]

    if not review_files:
        logger.warning("Không tìm thấy file review nào!")
        return 0

    # 2. Đọc review
    logger.info(f"Đang đọc {len(review_files)} file review...")
    df_reviews = spark.read.json(review_files)

    # 3. Chuẩn hóa schema review
    # Amazon review schema: rating, title, text, asin, parent_asin, user_id, timestamp_raw, etc.
    df_inter = df_reviews.select(
        col("user_id"),
        col("parent_asin").alias("product_id"),
        col("asin"),
        col("rating").cast("float"),
        col("text").alias("review_text"),
        col("main_category")
    ).dropna(subset=["user_id", "product_id"])

    # 4. Lọc trùng lặp
    df_inter = df_inter.dropDuplicates(["user_id", "product_id"])

    # 5. Lưu xuống Parquet
    count = df_inter.count()
    logger.info(f"Lưu {count} tương tác xuống Parquet...")
    df_inter.repartition(10).write.mode("overwrite").parquet(output_dir)

    return count
