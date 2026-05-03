# ============================================================
# src/etl_item_nodes.py
# Standardizes Amazon and VN metadata into a common schema.
# ============================================================

import os
import json
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, lit, lower, regexp_replace, udf, when, coalesce, array_join, trim, from_json
from pyspark.sql.types import StringType, MapType, ArrayType
from file_utils import detect_jsonl_type, list_files
from debug_utils import log_df_size

logger = logging.getLogger("etl_item_nodes")

# --- Helpers ---
def safe_col(df, col_name, default_val=None):
    """Returns the column if it exists in df, otherwise returns a literal default value."""
    if col_name in df.columns:
        return col(col_name)
    else:
        return lit(default_val)

def spark_standardize(c):
    """Light cleaning: Space normalization, Trim, Lower. Safe for IDs and Names."""
    c = coalesce(c.cast("string"), lit(""))
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

def get_category_expr(breadcrumb_col, product_name_col):
    """Native Spark expression to classify products."""
    text = lower(concat_ws(" ", breadcrumb_col, product_name_col))
    return when(text.rlike("laptop|macbook|máy tính xách tay"), "laptop") \
          .when(text.rlike("điện thoại|smartphone|iphone|dtdd"), "smartphone") \
          .when(text.rlike("tivi|tv|television"), "television") \
          .when(text.rlike("tai nghe|headphone|earphone|airpods"), "headphone") \
          .when(text.rlike("màn hình|monitor"), "monitor") \
          .when(text.rlike("để bàn|desktop|pc|máy tính bộ"), "desktop") \
          .when(text.rlike("tablet|máy tính bảng|ipad"), "tablet") \
          .otherwise("other")

def run_etl_item_nodes(spark, data_dir, output_dir):
    """Main ETL for Metadata files."""
    logger.info(f"Dang quet metadata tu: {data_dir}")
    
    all_files = list_files(data_dir)
    vn_files = []
    amz_files = []
    
    for f in all_files:
        if not f.endswith(".jsonl"):
            continue
        ftype = detect_jsonl_type(f)
        if ftype == "vn_item": vn_files.append(f)
        elif ftype == "amz_item": amz_files.append(f)

    df_final = None

    # 1. Xử lý VN Metadata
    if vn_files:
        logger.info(f"Dang xu ly {len(vn_files)} file VN metadata:")
        for f in vn_files: logger.info(f"  -> {os.path.basename(f)}")
        # Mode DROPMALFORMED de tranh treo may do loi format JSON
        df_vn = spark.read.option("mode", "DROPMALFORMED").json(vn_files)
        
        df_vn_std = df_vn.select(
            spark_standardize(safe_col(df_vn, "product_id")).alias("product_id"),
            spark_standardize(safe_col(df_vn, "asin")).alias("asin"),
            spark_standardize(safe_col(df_vn, "productName")).alias("product_name"),
            spark_clean_text(safe_col(df_vn, "specifications")).alias("specs_text"),
            spark_clean_text(safe_col(df_vn, "description")).alias("desc_text"),
            spark_standardize(safe_col(df_vn, "breadcrumb")).alias("breadcrumb")
        ).withColumn(
            "category", get_category_expr(col("breadcrumb"), col("product_name"))
        ).withColumn(
            "full_text", concat_ws(" ", col("product_name"), col("specs_text"), col("desc_text"))
        ).withColumn("domain", lit("vn"))

        df_final = df_vn_std.select("product_id", "asin", "product_name", "category", "full_text", "specs_text", "domain")

    # 2. Xử lý Amazon Metadata
    if amz_files:
        logger.info(f"Dang xu ly {len(amz_files)} file Amazon metadata:")
        for f in amz_files: logger.info(f"  -> {os.path.basename(f)}")
        df_amz = spark.read.option("mode", "DROPMALFORMED").json(amz_files)
        
        df_amz_std = df_amz.select(
            spark_standardize(safe_col(df_amz, "parent_asin")).alias("product_id"),
            spark_standardize(safe_col(df_amz, "asin")).alias("asin"),
            spark_standardize(safe_col(df_amz, "title")).alias("product_name"),
            spark_clean_text(safe_col(df_amz, "features")).alias("specs_text"),
            spark_clean_text(safe_col(df_amz, "description")).alias("desc_text"),
            spark_standardize(safe_col(df_amz, "main_category")).alias("breadcrumb")
        ).withColumn(
            "category", get_category_expr(col("breadcrumb"), col("product_name"))
        ).withColumn(
            "full_text", concat_ws(" ", col("product_name"), col("specs_text"), col("desc_text"))
        ).withColumn("domain", lit("amazon"))

        df_amz_final = df_amz_std.select("product_id", "asin", "product_name", "category", "full_text", "specs_text", "domain")
        
        if df_final is None:
            df_final = df_amz_final
        else:
            df_final = df_final.unionByName(df_amz_final)

    if df_final is None:
        logger.warning("Khong tim thay file metadata nao!")
        return 0

    logger.info("Dang thuc hien Native Parse Specs...")
    # Lọc và Parse JSON Native
    map_schema = "MAP<STRING, STRING>"
    df_final = df_final.filter(col("product_id") != "").dropDuplicates(["product_id"]) \
                       .withColumn("parsed_specs", 
                           when(col("specs_text").startswith("{"), from_json(col("specs_text"), map_schema))
                           .otherwise(None)
                       ).drop("specs_text")

    # 1. Ép vào Cache để tránh đọc lại GCS
    df_final = df_final.persist(StorageLevel.MEMORY_AND_DISK)

    # 2. Soi dung lượng
    log_df_size(df_final, "df_final_items (Chuẩn bị ghi file)")
    count = df_final.count()

    # 3. Ghi file
    logger.info(f"Luu {count} san pham chuan hoa xuong Parquet...")
    df_final.write.mode("overwrite").parquet(output_dir)
    
    # 4. Giải phóng
    df_final.unpersist()

    return count
