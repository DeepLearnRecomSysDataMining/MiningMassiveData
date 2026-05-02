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
        F.col("parsed_specs") # Giữ nguyên MapType, CHƯA convert vội
    )
    
    df_vn = df_items.filter(F.col("domain") == "vn").select(
        F.col("product_id").alias("cand_id"),
        F.col("asin").alias("cand_asin"),
        F.col("category").alias("cand_category")
        # Không cần parse các cột text của VN ở đây vì file eval chỉ lưu cand_id
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
    
    # Lấy toàn bộ VN items làm ứng viên (Vì VN chỉ có vài nghìn SP)
    df_vn_reduced = df_vn.select("cand_id", "cand_asin", "cand_category")
    
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
    df_amz_meta = df_amz.select("query_id", "query_name", "query_text", "query_category", "parsed_specs")
    
    # Ép Spark broadcast bảng df_grouped (bảng này rất nhỏ, chỉ vài chục/vài trăm dòng)
    df_eval_raw = F.broadcast(df_grouped).join(df_amz_meta, "query_id", "inner")
    
    # BÂY GIỜ MỚI CONVERT SANG JSON: Vì lúc này bảng df_eval_raw chỉ còn ĐÚNG 11 DÒNG!
    # Nó chạy nhanh bằng vận tốc ánh sáng và tốn 0 MB RAM.
    df_eval = df_eval_raw.withColumn("query_specs", F.to_json(F.col("parsed_specs"))).drop("parsed_specs")

    # 7. Ghi dữ liệu
    logger.info(f"Dang ghi {query_count} queries xuong GCS...")
    
    # SỬ DỤNG repartition(1) THAY VÌ coalesce(1)
    # coalesce(1) là thủ phạm ép Spark dồn toàn bộ 50GB data vào 1 task duy nhất để đọc, gây OOM!
    # repartition(1) sẽ cho phép Spark đọc và Join trên hàng trăm task song song, sau đó mới gom 11 kết quả cuối cùng lại.
    df_eval.repartition(1).write.mode("overwrite").parquet(output_path)
    
    logger.info(f"V HOAN TAT: Da tao bo du lieu Evaluation tai {output_path}")
    return query_count
