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
        Sử dụng kỹ thuật 'Safe Mining' và Sampling để tránh bùng nổ dữ liệu.
        Chỉ lưu ID-Only, bỏ qua Join Metadata để tối ưu RAM và thời gian chạy.
        """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    logger.info("[EVAL-V2] Bat dau tao bo du lieu Evaluation (ID-Only)...")

    # 1. Đọc dữ liệu sản phẩm (Chỉ load các cột cần thiết cho việc xử lý ID)
    df_items = spark.read.parquet(items_path).select(
        "product_id", "asin", "category", "domain"
    ).persist(StorageLevel.MEMORY_AND_DISK)

    # 2. Tạo tập Positive (Dựa trên ASIN khớp nhau giữa Amazon và VN)
    # TỐI ƯU: Thêm F.trim() để tránh lỗi khoảng trắng làm lệch kết quả
    df_amz = df_items.filter(
        (F.col("domain") == "amazon") & 
        (F.col("asin") != "") & 
        (F.col("asin").isNotNull()) & 
        (F.col("asin") != "none")
    ).select(
        F.trim(F.col("asin")).alias("query_id"),
        F.trim(F.col("product_id")).alias("query_parent_id"), 
        F.col("category").alias("query_category")
    )

    df_vn = df_items.filter(
        (F.col("domain") == "vn") & 
        (F.col("asin") != "") & 
        (F.col("asin").isNotNull()) &
        (F.col("asin") != "none")
    ).select(
        F.col("product_id").alias("cand_id"),
        F.trim(F.col("asin")).alias("cand_asin"),
        F.col("category").alias("cand_category")
    )

    # DEBUG: Kiểm tra số lượng item thô
    amz_count = df_amz.count()
    vn_count = df_vn.count()
    logger.info(f"[DEBUG] So luong Amazon hop le: {amz_count:,}")
    logger.info(f"[DEBUG] So luong VN hop le: {vn_count:,}")

    # Positive pairs: Khớp linh hoạt với cả ASIN con HOẶC Parent ASIN
    df_pos_raw = df_amz.join(
        F.broadcast(df_vn), 
        (F.lower(df_amz.query_id) == F.lower(df_vn.cand_asin)) | 
        (F.lower(df_amz.query_parent_id) == F.lower(df_vn.cand_asin)), 
        "inner"
    ).select("query_id", "cand_id", "query_category") \
     .dropDuplicates(["query_id", "cand_id"]) \
     .withColumn("label", F.lit(1))

    pos_count = df_pos_raw.count()
    logger.info(f"[DEBUG] So luong cap Positive tim thay: {pos_count:,}")

    df_pos = df_pos_raw

    query_ids_df = df_pos.select("query_id", "query_category").distinct()
    query_count = query_ids_df.count()

    if query_count == 0:
        logger.warning("Khong tim thay cap Amazon-VN nao khop ASIN!")
        return 0

    logger.info(f"Mining cho {query_count} queries...")

    # 3. TỐI ƯU NEGATIVE MINING (Đảm bảo luôn đủ 100 candidates bằng cách Fallback)
    # Lấy mẫu Hard Negatives (cùng Category) - tối đa 500 mẫu mỗi loại
    window_limit = Window.partitionBy("cand_category").orderBy(F.rand(seed=42))
    df_vn_hard_pool = df_vn.withColumn("rn", F.row_number().over(window_limit)) \
        .filter(F.col("rn") <= 500)

    # Lấy mẫu Easy Negatives (Ngẫu nhiên từ toàn bộ kho) để làm dự phòng
    df_vn_easy_pool = df_vn.orderBy(F.rand(seed=42)).limit(2000)

    # Bước A: Tìm ứng viên cùng Category (Hard)
    df_neg_hard = query_ids_df.join(F.broadcast(df_vn_hard_pool),
                                    query_ids_df.query_category == df_vn_hard_pool.cand_category,
                                    "inner") \
        .filter(F.col("query_id") != F.col("cand_asin")) \
        .select("query_id", "cand_id") \
        .withColumn("priority", F.lit(1))

    # Bước B: Tìm ứng viên ngẫu nhiên (Easy) - Cross join với tập đã thu gọn là cực nhanh
    df_neg_easy = query_ids_df.crossJoin(F.broadcast(df_vn_easy_pool)) \
        .filter(F.col("query_id") != F.col("cand_asin")) \
        .select("query_id", "cand_id") \
        .withColumn("priority", F.lit(0))

    # Bước C: Gộp và lấy Top 99 cho mỗi Query (Ưu tiên Hard trước)
    df_neg_all = df_neg_hard.unionByName(df_neg_easy)
    
    window_neg = Window.partitionBy("query_id").orderBy(F.col("priority").desc(), F.rand(seed=42))
    
    df_negatives = df_neg_all.withColumn("rank", F.row_number().over(window_neg)) \
        .filter(F.col("rank") <= (num_candidates - 1)) \
        .select("query_id", "cand_id") \
        .withColumn("label", F.lit(0))

    # 4. Gom tập Positive và Negative (Chỉ lấy ID)
    df_final_ids = df_pos.select("query_id", "cand_id", "label") \
        .unionByName(df_negatives)

    # 5. Group IDs lại thành List (Format chuẩn cho Training nhưng siêu nhẹ)
    df_eval = df_final_ids.groupBy("query_id").agg(
        F.collect_list("cand_id").alias("candidate_ids"),
        F.collect_list("label").alias("labels")
    )

    # --- KHÔNG JOIN METADATA Ở ĐÂY NỮA ---
    # Toàn bộ dữ liệu text khổng lồ sẽ được xử lý độc lập bên ngoài Spark

    # 6. Ghi kết quả (Bây giờ chỉ còn vài MB, chạy cực nhanh)
    logger.info(f"Ghi ket qua Evaluation ID-ONLY (Coalesce 1) xuong: {output_path}")
    df_eval.coalesce(1).write.mode("overwrite").parquet(output_path)

    df_items.unpersist()

    # Đếm số lượng từ metadata của output
    final_query_count = spark.read.parquet(output_path).count()
    logger.info(f"Hoan tat! Da tao {final_query_count} evaluation queries (ID-Only).")
    return final_query_count
