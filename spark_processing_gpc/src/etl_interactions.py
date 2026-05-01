# ============================================================
# src/etl_interactions.py
# Standardizes Amazon and VN reviews into a common schema.
# ============================================================

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, lit, coalesce, concat_ws, regexp_replace, trim
from pyspark.sql.types import StringType
import ast
from .file_utils import detect_jsonl_type, list_files

logger = logging.getLogger("etl_interactions")

# --- Helper for missing columns ---
def safe_col(df, col_name, default_val=None):
    """Returns the column if it exists in df, otherwise returns a literal default value."""
    if col_name in df.columns:
        return col(col_name)
    else:
        return lit(default_val)

def spark_standardize(c):
    """Light cleaning: Space normalization, Trim, Lower. Safe for IDs and Names."""
    c = coalesce(concat_ws(" ", c), lit(""))
    c = regexp_replace(c, r"\s+", " ")
    return lower(trim(c))

def spark_clean_text(c):
    """Aggressive cleaning: HTML removal, Regex strip, Space normalization, Trim, Lower."""
    c = coalesce(concat_ws(" ", c), lit(""))
    # 1. Bo HTML
    c = regexp_replace(c, r"<[^>]*>", " ")
    # 2. Bo ky tu dac biet, chi giu chu, so, dau cau (Simplified regex for stability)
    c = regexp_replace(c, r"[^a-zA-Z0-9\s.,!?àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ]", " ")
    # 3. Chuan hoa khoang trang
    c = regexp_replace(c, r"\s+", " ")
    return lower(trim(c))

def run_etl_interactions(spark: SparkSession, data_dir: str, output_dir: str) -> int:
    """ ETL for review data. Standardizes different review formats into a common schema."""
    logger.info(f"Dang quet review tu: {data_dir}")
    
    # 1. Tìm và phân loại các file review
    all_files = list_files(data_dir)
    vn_review_files = []
    amz_review_files = []

    for f_path in all_files:
        if not f_path.endswith(".jsonl"):
            continue
            
        f_type = detect_jsonl_type(f_path)
        if f_type == "vn_review":
            vn_review_files.append(f_path)
        elif f_type == "amz_review":
            amz_review_files.append(f_path)

    df_final = None

    # 2. Xử lý Review Việt Nam
    if vn_review_files:
        logger.info(f"Dang xu ly {len(vn_review_files)} file VN reviews:")
        for f in vn_review_files: logger.info(f"  -> {os.path.basename(f)}")
        df_vn = spark.read.json(vn_review_files)
        
        # Mapping with safety and null handling for EVERY column
        df_vn_std = df_vn.select(
            spark_standardize(safe_col(df_vn, "fullName")).alias("user_id"),
            spark_standardize(safe_col(df_vn, "productId")).alias("product_id"),
            spark_standardize(safe_col(df_vn, "asin")).alias("asin"),
            coalesce(safe_col(df_vn, "rating").cast("float"), lit(0.0)).alias("rating"),
            spark_clean_text(safe_col(df_vn, "content")).alias("review_text"),
            spark_standardize(safe_col(df_vn, "breadcrumb")).alias("main_category")
        ).withColumn("domain", lit("vn"))
        
        df_final = df_vn_std

    # 3. Xử lý Review Amazon
    if amz_review_files:
        logger.info(f"Dang xu ly {len(amz_review_files)} file Amazon reviews:")
        for f in amz_review_files: logger.info(f"  -> {os.path.basename(f)}")
        df_amz = spark.read.json(amz_review_files)
        
        # Mapping with safety and null handling for EVERY column
        df_amz_std = df_amz.select(
            spark_standardize(safe_col(df_amz, "user_id")).alias("user_id"),
            spark_standardize(safe_col(df_amz, "parent_asin")).alias("product_id"),
            spark_standardize(safe_col(df_amz, "asin")).alias("asin"),
            coalesce(safe_col(df_amz, "rating").cast("float"), lit(0.0)).alias("rating"),
            spark_clean_text(safe_col(df_amz, "text")).alias("review_text"),
            spark_standardize(safe_col(df_amz, "main_category")).alias("main_category")
        ).withColumn("domain", lit("amazon"))

        if df_final is None:
            df_final = df_amz_std
        else:
            df_final = df_final.unionByName(df_amz_std)

    if df_final is None:
        logger.warning("Khong tim thay file review nao!")
        return 0

    # 4. Làm sạch: Xóa dòng thiếu thông tin định danh cốt lõi
    df_final = df_final.filter((col("user_id") != "") & (col("product_id") != "")) \
                       .dropDuplicates(["user_id", "product_id"])

    # 5. Lưu xuống Parquet
    count = df_final.count()
    logger.info(f"Luu {count} tuong tac chuan hoa xuong Parquet...")
    df_final.repartition(32).write.mode("overwrite").parquet(output_dir)

    return count
