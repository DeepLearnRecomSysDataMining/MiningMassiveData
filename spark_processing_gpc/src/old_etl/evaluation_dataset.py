# ============================================================
# src/evaluation_dataset.py
# Native Spark implementation of Negative Mining (1 True + 99 Negatives)
# ============================================================

import logging
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel
from src.debug_utils import log_df_size

logger = logging.getLogger("evaluation_dataset")

def run_evaluation_generator(spark: SparkSession, items_path: str, output_path: str, num_candidates: int = 100):
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    logger.info("Bat dau tao bo du lieu Evaluation (Ultra-Stable Architecture)...")

    # 1. FIX LỖI GCS: Đọc và Cache luôn vào RAM/Disk của 3 Worker (Tốn 0 request GCS về sau)
    df_items = spark.read.parquet(items_path).persist(StorageLevel.MEMORY_AND_DISK)
    log_df_size(df_items, "df_items (Toàn bộ Data gốc cache ở Local)")

    # Ép Spark thực thi cache ngay lập tức
    total_items = df_items.count()
    logger.info(f"Da load {total_items} items vao Local Cache.")

    df_amz_light = df_items.filter(F.col("domain") == "amazon").select(
        F.col("asin").alias("query_id"),
        F.col("category").alias("query_category")
    )
    
    df_vn_light = df_items.filter(F.col("domain") == "vn").select(
        F.col("product_id").alias("cand_id"),
        F.col("asin").alias("cand_asin"),
        F.col("category").alias("cand_category")
    )

    log_df_size(df_amz_light, "df_amz_light (Chỉ chứa ID Amazon)")
    log_df_size(df_vn_light, "df_vn_light (Chỉ chứa ID VN)")

    df_pos = df_amz_light.join(F.broadcast(df_vn_light), df_amz_light.query_id == df_vn_light.cand_asin, "inner") \
                         .withColumn("label", F.lit(1))
    log_df_size(df_pos, "df_pos (Tập Positive Pairs)")
                   
    query_count = df_pos.select("query_id").distinct().count()
    if query_count == 0:
        logger.warning("Khong tim thay cap Amazon-VN nao khop ASIN!")
        return 0

    logger.info(f"Dang thuc hien Mining cho {query_count} queries...")
    
    df_negatives = df_vn_light.join(F.broadcast(df_pos.select("query_id", "query_category").distinct()), 
                                     df_vn_light.cand_category == df_pos.query_category, 
                                     "inner") \
                                .filter(F.col("query_id") != F.col("cand_asin"))
    log_df_size(df_negatives, "df_negatives (Trước khi Window - Chỗ này dễ phình to)")

    # 2. FIX LỖI 143 TRONG WINDOW: 
    # Vẫn dùng Window nhưng kết hợp Adaptive Query Execution đã bật ở trên.
    window_neg = Window.partitionBy("query_id").orderBy(F.rand())
    df_negatives = df_negatives.withColumn("rank", F.row_number().over(window_neg)) \
                               .filter(F.col("rank") <= (num_candidates - 1)) \
                               .withColumn("label", F.lit(0))

    df_final_ids = df_pos.select("query_id", "cand_id", "label") \
                         .unionByName(df_negatives.select("query_id", "cand_id", "label"))

    df_grouped = df_final_ids.groupBy("query_id").agg(
        F.collect_list("cand_id").alias("candidate_ids"),
        F.collect_list("label").alias("labels")
    )

    df_amz_heavy = df_items.filter(F.col("domain") == "amazon").select(
        F.col("asin").alias("query_id"),
        F.col("product_name").alias("query_name"),
        F.col("full_text").alias("query_text"),
        F.col("category").alias("query_category"),
        F.col("parsed_specs")
    )
    
    df_eval_raw = df_grouped.join(df_amz_heavy, "query_id", "inner")
    log_df_size(df_eval_raw, "df_eval_raw (Dữ liệu chốt sổ chờ ghi File)")
    
    df_eval = df_eval_raw.withColumn("query_specs", F.to_json(F.col("parsed_specs"))).drop("parsed_specs")

    logger.info(f"Dang ghi {query_count} queries xuong GCS...")
    
    # 3. FIX LỖI OOM KHI GHI FILE:
    # Bỏ hẳn repartition(1) hoặc coalesce(1). Hãy để Spark tự ghi song song ra nhiều file parquet nhỏ. 
    # GCS hỗ trợ đọc folder parquet cực tốt, không bắt buộc phải gom vào 1 file.
    df_eval.write.mode("overwrite").parquet(output_path)
    
    # Giải phóng Cache
    df_items.unpersist()
    
    logger.info(f"V HOAN TAT: Da tao bo du lieu Evaluation tai {output_path}")
    return query_count