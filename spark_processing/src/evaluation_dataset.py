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

    # 3. Tạo Negative Candidates
    # Để tối ưu, ta không Cross Join toàn bộ (4k x 4k = 16M). 
    # Ta sẽ lấy ngẫu nhiên một tập các ứng viên VN cho mỗi Query.
    
    # Lấy 150 ứng viên ngẫu nhiên cho mỗi query để lọc lấy 99 cái
    # Dùng hàm rand() và Window để chọn ngẫu nhiên
    window_spec = Window.partitionBy("query_id").orderBy(F.rand())
    
    # Join Query với toàn bộ VN Catalog nhưng loại bỏ cặp Positive
    df_all_pairs = df_amz.join(df_vn, df_amz.query_id != df_vn.cand_asin, "inner")
    
    # Phân loại Negative: Hard (cùng category) và Easy (khác category)
    df_negatives = df_all_pairs.withColumn("is_hard", F.when(F.col("query_category") == F.col("cand_category"), 1).otherwise(0)) \
                               .withColumn("rank", F.row_number().over(window_spec)) \
                               .filter(F.col("rank") <= (num_candidates - 1)) \
                               .withColumn("label", F.lit(0))

    # 4. Gộp Positive và Negative
    # Đưa về cùng schema
    common_cols = ["query_id", "query_name", "query_text", "query_category", "query_specs", 
                   "cand_id", "cand_name", "cand_text", "cand_category", "cand_specs", "label"]
    
    df_final = df_pos.select(*common_cols).unionByName(df_negatives.select(*common_cols))

    # 5. Group lại thành dạng 1 dòng cho mỗi query (để AI Model dễ đọc)
    # query_specs là kiểu Map nên không groupBy được, ta dùng first() trong agg
    df_eval = df_final.groupBy("query_id", "query_name", "query_text", "query_category").agg(
        F.first("query_specs").alias("query_specs"),
        F.collect_list("cand_id").alias("candidate_ids"),
        F.collect_list("cand_text").alias("candidate_texts"),
        F.collect_list("cand_category").alias("candidate_categories"),
        F.collect_list("cand_specs").alias("candidate_specs"),
        F.collect_list("label").alias("labels")
    )

    # 6. Lưu xuống Parquet (Native hoàn toàn)
    df_eval.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Da tao bo kiem thu Native cho {query_count} queries.")
    return query_count
