# ============================================================
# src/evaluation_dataset_v2.py (OPTIMIZED VERSION)
# Native Spark implementation of Negative Mining (Ultra-Stable)
# ============================================================

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel

logger = logging.getLogger("evaluation_dataset_v2")

def run_evaluation_generator(spark: SparkSession, items_path: str, output_path: str, num_candidates: int = 100):
    """
    Tạo bộ dữ liệu Evaluation (1 Positive + 99 Negatives) 
    Sử dụng kỹ thuật 'Safe Mining' để tránh bùng nổ dữ liệu khi Join Category.
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    logger.info("[EVAL-V2] Bat dau tao bo du lieu Evaluation...")

    # 1. Đọc dữ liệu sản phẩm (Khoảng 4-5 triệu dòng)
    df_items = spark.read.parquet(items_path).select(
        "product_id", "asin", "product_name", "category", "full_text", "parsed_specs", "domain"
    ).persist(StorageLevel.MEMORY_AND_DISK)

    # 2. Tạo tập Positive (Dựa trên ASIN khớp nhau giữa Amazon và VN)
    df_amz = df_items.filter(F.col("domain") == "amazon").select(
        F.col("asin").alias("query_id"),
        F.col("category").alias("query_category")
    )
    
    df_vn = df_items.filter(F.col("domain") == "vn").select(
        F.col("product_id").alias("cand_id"),
        F.col("asin").alias("cand_asin"),
        F.col("category").alias("cand_category")
    )

    # Positive pairs: Những cặp có cùng ASIN
    df_pos = df_amz.join(F.broadcast(df_vn), df_amz.query_id == df_vn.cand_asin, "inner") \
                   .select("query_id", "cand_id", "query_category") \
                   .withColumn("label", F.lit(1))
    
    query_ids_df = df_pos.select("query_id", "query_category").distinct()
    query_count = query_ids_df.count()

    if query_count == 0:
        logger.warning("Khong tim thay cap Amazon-VN nao khop ASIN!")
        return 0

    logger.info(f"Mining cho {query_count} queries...")

    # 3. TỐI ƯU NEGATIVE MINING (Tránh Join Cartesian Product)
    # Thay vì Join mọi Amazon với mọi VN trong cùng Category (dễ sập), 
    # Ta lấy mẫu ngẫu nhiên khoảng 500 candidates cho mỗi Category trước.
    
    window_limit = Window.partitionBy("cand_category").orderBy(F.rand())
    df_vn_sampled = df_vn.withColumn("rn", F.row_number().over(window_limit)) \
                         .filter(F.col("rn") <= 500) # Chỉ lấy tối đa 500 ứng viên mỗi Category để Mining
    
    # Mining: Join Query với tập Candidate đã được thu gọn (Sampled)
    df_neg_candidates = query_ids_df.join(F.broadcast(df_vn_sampled), 
                                          query_ids_df.query_category == df_vn_sampled.cand_category, 
                                          "inner") \
                                    .filter(F.col("query_id") != F.col("cand_asin"))
    
    # Chọn ra 99 Negative cho mỗi Query từ tập ứng viên
    window_neg = Window.partitionBy("query_id").orderBy(F.rand())
    df_negatives = df_neg_candidates.withColumn("rank", F.row_number().over(window_neg)) \
                                    .filter(F.col("rank") <= (num_candidates - 1)) \
                                    .select("query_id", "cand_id") \
                                    .withColumn("label", F.lit(0))

    # 4. Gom tập Positive và Negative
    df_final_ids = df_pos.select("query_id", "cand_id", "label") \
                         .unionByName(df_negatives)

    # Group IDs lại thành List (Format chuẩn cho Training)
    df_grouped = df_final_ids.groupBy("query_id").agg(
        F.collect_list("cand_id").alias("candidate_ids"),
        F.collect_list("label").alias("labels")
    )

    # 5. Join lại với Metadata Amazon (Heavy)
    df_amz_metadata = df_items.filter(F.col("domain") == "amazon").select(
        F.col("asin").alias("query_id"),
        F.col("product_name").alias("query_name"),
        F.col("full_text").alias("query_text"),
        F.col("category").alias("query_category"),
        F.to_json(F.col("parsed_specs")).alias("query_specs")
    )
    
    df_eval = df_grouped.join(F.broadcast(df_amz_metadata), "query_id", "inner")

    # Ghi kết quả (TỐI ƯU: Coalesce để giảm phí Class A trên GCS)
    logger.info(f"Ghi ket qua Evaluation (Coalesce 16) xuong: {output_path}")
    df_eval.coalesce(16).write.mode("overwrite").parquet(output_path)
    
    df_items.unpersist()
    
    # TỐI ƯU: Đếm số lượng từ metadata của output (Cực nhanh)
    final_query_count = spark.read.parquet(output_path).count()
    logger.info(f"Hoan tat! Da tao {final_query_count} evaluation queries.")
    return final_query_count

