# ============================================================
# src/etl_interactions_v2.py (OPTIMIZED VERSION)
# Standardizes Amazon and VN reviews into a common schema.
# ============================================================

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, lit, coalesce, concat_ws, regexp_replace, trim
from pyspark.sql.types import StringType
from src.file_utils import detect_jsonl_type, list_files

logger = logging.getLogger("etl_interactions_v2")

def safe_col(df, col_name, default_val=None):
    if col_name in df.columns:
        return col(col_name)
    else:
        return lit(default_val)

def spark_standardize(c):
    c = coalesce(concat_ws(" ", c), lit(""))
    c = regexp_replace(c, r"\s+", " ")
    return lower(trim(c))

def spark_clean_text(c):
    c = coalesce(concat_ws(" ", c), lit(""))
    c = regexp_replace(c, r"<[^>]*>", " ")
    c = regexp_replace(c, r"[^a-zA-Z0-9\s.,!?àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ]", " ")
    c = regexp_replace(c, r"\s+", " ")
    return lower(trim(c))

def run_etl_interactions(spark: SparkSession, data_dir: str, output_dir: str) -> int:
    logger.info(f"[V2-OPTIMIZED] Dang quet review tu: {data_dir}")
    
    all_files = list_files(data_dir)
    vn_review_files = []
    amz_review_files = []

    for f_path in all_files:
        if not f_path.endswith(".jsonl"): continue
        f_type = detect_jsonl_type(f_path)
        if f_type == "vn_review": vn_review_files.append(f_path)
        elif f_type == "amz_review": amz_review_files.append(f_path)

    df_final = None

    # 1. Xử lý Review Việt Nam
    if vn_review_files:
        logger.info(f"Dang xu ly {len(vn_review_files)} file VN reviews...")
        # TỐI ƯU: Chỉ select những cột cần thiết ngay khi đọc
        vn_cols = ["fullName", "productId", "asin", "rating", "content", "breadcrumb"]
        df_vn = spark.read.json(vn_review_files).select([c for c in vn_cols if c in vn_cols])
        
        df_vn_std = df_vn.select(
            spark_standardize(safe_col(df_vn, "fullName")).alias("user_id"),
            spark_standardize(safe_col(df_vn, "productId")).alias("product_id"),
            spark_standardize(safe_col(df_vn, "asin")).alias("asin"),
            coalesce(safe_col(df_vn, "rating").cast("float"), lit(0.0)).alias("rating"),
            spark_clean_text(safe_col(df_vn, "content")).alias("review_text"),
            spark_standardize(safe_col(df_vn, "breadcrumb")).alias("main_category")
        ).withColumn("domain", lit("vn"))
        df_final = df_vn_std

    # 2. Xử lý Review Amazon
    if amz_review_files:
        logger.info(f"Dang xu ly {len(amz_review_files)} file Amazon reviews...")
        # TỐI ƯU: Chỉ select những cột cần thiết ngay khi đọc
        amz_cols = ["user_id", "parent_asin", "asin", "rating", "text", "main_category"]
        df_amz = spark.read.json(amz_review_files).select([c for c in amz_cols if c in amz_cols])
        
        df_amz_std = df_amz.select(
            spark_standardize(safe_col(df_amz, "user_id")).alias("user_id"),
            spark_standardize(safe_col(df_amz, "parent_asin")).alias("product_id"),
            spark_standardize(safe_col(df_amz, "asin")).alias("asin"),
            coalesce(safe_col(df_amz, "rating").cast("float"), lit(0.0)).alias("rating"),
            spark_clean_text(safe_col(df_amz, "text")).alias("review_text"),
            spark_standardize(safe_col(df_amz, "main_category")).alias("main_category")
        ).withColumn("domain", lit("amazon"))

        if df_final is None: df_final = df_amz_std
        else: df_final = df_final.unionByName(df_amz_std)

    if df_final is None:
        logger.warning("Khong tim thay file review nao!")
        return 0

    # TỐI ƯU: Ghi thẳng ra GCS dưới dạng Pipeline, gom thành 32 file
    logger.info(f"Saving to Parquet (V2-Coalesce) -> {output_dir}")
    df_final.filter((col("user_id") != "") & (col("product_id") != "")) \
            .dropDuplicates(["user_id", "product_id"]) \
            .coalesce(32) \
            .write.mode("overwrite").parquet(output_dir)

    return -1
