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
    logger.info("Bat dau tao bo du lieu Evaluation (Native Mode)...")

    # 1. Đọc dữ liệu
    df_items = spark.read.parquet(items_path)

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

    # 3. Tạo Negative Candidates bằng phương pháp "Sampling Broadcast Join"
    logger.info(f"Dang thuc hien Negative Mining cho {query_count} queries...")
    
    # Ép Spark chia nhỏ các tác vụ ghi để không dồn cục bộ
    spark.conf.set("spark.sql.shuffle.partitions", "1000")
    
    # Lấy mẫu 2% VN items làm ứng viên (Map-only, cực nhẹ)
    df_vn_reduced = df_vn.select("cand_id", "cand_asin", "cand_category").sample(False, 0.02, 42)
    
    # Chỉ lấy ID và Category của Query để Join
    df_queries_min = df_pos.select("query_id", "query_category").dropDuplicates(["query_id"])
    
    # Join Query với tập ứng viên (Broadcast Join)
    df_negatives = df_vn_reduced.join(F.broadcast(df_queries_min), 
                                     df_vn_reduced.cand_category == df_queries_min.query_category, 
                                     "inner") \
                                .filter(F.col("query_id") != F.col("cand_asin"))
    
    # Lấy 99 negatives bằng Window (Chỉ làm việc với ID nên rất nhẹ)
    window_neg = Window.partitionBy("query_id").orderBy(F.rand())
    df_negatives = df_negatives.withColumn("rank", F.row_number().over(window_neg)) \
                               .filter(F.col("rank") <= (num_candidates - 1)) \
                               .withColumn("label", F.lit(0))

    # 4. Gộp Positive và Negative (Chỉ gộp ID và Label)
    df_pos_min = df_pos.select("query_id", "cand_id", "cand_category", "label")
    df_neg_min = df_negatives.select("query_id", "cand_id", "cand_category", "label")
    
    df_final_ids = df_pos_min.unionByName(df_neg_min)

    # 5. Gom nhóm theo ID (Shuffle cực nhẹ vì không có văn bản)
    logger.info("Dang gom nhom ID (Shuffle-light)...")
    df_grouped = df_final_ids.groupBy("query_id").agg(
        F.collect_list("cand_id").alias("candidate_ids"),
        F.collect_list("cand_category").alias("candidate_categories"),
        F.collect_list("label").alias("labels")
    )

    # 6. Join ngược lại để lấy Metadata của Query (Bước cuối cùng)
    # Lúc này ta mới đưa Query_Text và Query_Specs vào
    df_amz_meta = df_amz.select("query_id", "query_name", "query_text", "query_category", "query_specs")
    
    df_eval = df_grouped.join(df_amz_meta, "query_id", "inner")

    # 7. Lưu xuống Parquet
    logger.info(f"Dang ghi du lieu cuoi cung xuong GCS: {output_path}")
    df_eval.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"V HOAN TAT: Da tao bo du lieu Evaluation tai {output_path}")
    return query_count
