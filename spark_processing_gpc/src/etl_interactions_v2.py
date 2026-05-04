# ============================================================
# src/etl_interactions_v2.py (OPTIMIZED VERSION)
# Standardizes Amazon and VN reviews into a common schema.
# ============================================================

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, lit, coalesce, concat_ws, regexp_replace, trim
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from src.file_utils import detect_jsonl_type, list_files

logger = logging.getLogger("etl_interactions_v2")

# --- ĐỊNH NGHĨA SCHEMA TƯỜNG MINH (Giúp Spark không cần scan dữ liệu để đoán schema) ---
VN_REVIEW_SCHEMA = StructType([
    StructField("fullName", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("content", StringType(), True),
    StructField("breadcrumb", StringType(), True)
])

AMZ_REVIEW_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("parent_asin", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("rating", FloatType(), True),
    StructField("text", StringType(), True),
    StructField("main_category", StringType(), True)
])

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

def run_etl_interactions(spark: SparkSession, data_dir: str, output_dir: str, file_groups: dict = None) -> int:
    logger.info(f"[V2-OPTIMIZED] Dang xu ly ETL Interactions...")
    
    if file_groups:
        # TỐI ƯU: Tận dụng kết quả từ Schema Scanner
        vn_review_files = file_groups.get("vn_review", [])
        amz_review_files = file_groups.get("amz_review", [])
    else:
        # Fallback nếu không có file_groups
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
        logger.info(f"Dang xu ly {len(vn_review_files)} file VN reviews (VỚI SCHEMA TƯỜNG MINH)")
        # TỐI ƯU: Dùng schema để tránh request đoán kiểu
        df_vn = spark.read.schema(VN_REVIEW_SCHEMA).json(vn_review_files)

        logger.info(f"Dang Mapping data cua {len(vn_review_files)} file VN reviews sang Formal Schema")

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
        logger.info(f"Dang xu ly {len(amz_review_files)} file Amazon reviews (VỚI SCHEMA TƯỜNG MINH)")
        # TỐI ƯU: Dùng schema để tránh request đoán kiểu
        df_amz = spark.read.schema(AMZ_REVIEW_SCHEMA).json(amz_review_files)

        logger.info(f"Dang Mapping data cua {len(amz_review_files)} file Amazon reviews sang Formal Schema")

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

    # TỐI ƯU: Đếm số lượng từ metadata của file đã ghi (Cực nhanh vì chỉ đọc footer Parquet)
    final_count = spark.read.parquet(output_dir).count()
    return final_count
