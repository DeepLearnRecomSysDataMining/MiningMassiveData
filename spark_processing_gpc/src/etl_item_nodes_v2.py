# ============================================================
# src/etl_item_nodes_v2.py (OPTIMIZED VERSION)
# Standardizes Amazon and VN metadata into a common schema.
# ============================================================

import os
import json
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, lit, lower, regexp_replace, udf, when, coalesce, array_join, trim, from_json
from pyspark.sql.types import StructType, StructField, StringType
from src.file_utils import detect_jsonl_type, list_files

logger = logging.getLogger("etl_item_nodes_v2")

# --- ĐỊNH NGHĨA SCHEMA TƯỜNG MINH ---
VN_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("productName", StringType(), True),
    StructField("specifications", StringType(), True),
    StructField("description", StringType(), True),
    StructField("breadcrumb", StringType(), True)
])

AMZ_ITEM_SCHEMA = StructType([
    StructField("parent_asin", StringType(), True),
    StructField("asin", StringType(), True),
    StructField("title", StringType(), True),
    StructField("features", StringType(), True),
    StructField("description", StringType(), True),
    StructField("main_category", StringType(), True)
])

def safe_col(df, col_name, default_val=None):
    if col_name in df.columns:
        return col(col_name)
    else:
        return lit(default_val)

def spark_standardize(c):
    c = coalesce(c.cast("string"), lit(""))
    c = regexp_replace(c, r"\s+", " ")
    return lower(trim(c))

def spark_clean_text(c):
    c = coalesce(concat_ws(" ", c), lit(""))
    c = regexp_replace(c, r"<[^>]*>", " ")
    c = regexp_replace(c, r"[^a-zA-Z0-9\s.,!?àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ]", " ")
    c = regexp_replace(c, r"\s+", " ")
    return lower(trim(c))

def get_category_expr(breadcrumb_col, product_name_col):
    text = lower(concat_ws(" ", breadcrumb_col, product_name_col))
    return when(text.rlike("laptop|macbook|máy tính xách tay"), "laptop") \
          .when(text.rlike("điện thoại|smartphone|iphone|dtdd"), "smartphone") \
          .when(text.rlike("tivi|tv|television"), "television") \
          .when(text.rlike("tai nghe|headphone|earphone|airpods"), "headphone") \
          .when(text.rlike("màn hình|monitor"), "monitor") \
          .when(text.rlike("để bàn|desktop|pc|máy tính bộ"), "desktop") \
          .when(text.rlike("tablet|máy tính bảng|ipad"), "tablet") \
          .otherwise("other")

def run_etl_item_nodes(spark, data_dir, output_dir, file_groups: dict = None):
    logger.info(f"[V2-OPTIMIZED] Dang xu ly ETL Item Nodes...")
    
    if file_groups:
        vn_files = file_groups.get("vn_item", [])
        amz_files = file_groups.get("amz_item", [])
    else:
        all_files = list_files(data_dir)
        vn_files = []
        amz_files = []
        for f in all_files:
            if not f.endswith(".jsonl"): continue
            ftype = detect_jsonl_type(f)
            if ftype == "vn_item": vn_files.append(f)
            elif ftype == "amz_item": amz_files.append(f)

    df_final = None

    # 1. Xử lý VN Metadata
    if vn_files:
        logger.info(f"Dang xu ly {len(vn_files)} file VN metadata (VỚI SCHEMA TƯỜNG MINH)")
        df_vn = spark.read.option("mode", "DROPMALFORMED").schema(VN_ITEM_SCHEMA).json(vn_files)
        
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
        logger.info(f"Dang xu ly {len(amz_files)} file Amazon metadata (VỚI SCHEMA TƯỜNG MINH)")
        df_amz = spark.read.option("mode", "DROPMALFORMED").schema(AMZ_ITEM_SCHEMA).json(amz_files)
        
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
        
        if df_final is None: df_final = df_amz_final
        else: df_final = df_final.unionByName(df_amz_final)

    if df_final is None:
        logger.warning("Khong tim thay file metadata nao!")
        return 0

    # Lọc và Parse JSON Native
    map_schema = "MAP<STRING, STRING>"
    df_final = df_final.filter(col("product_id") != "").dropDuplicates(["product_id"]) \
                       .withColumn("parsed_specs", 
                           when(col("specs_text").startswith("{"), from_json(col("specs_text"), map_schema))
                           .otherwise(None)
                       ).drop("specs_text")

    # TỐI ƯU: Ghi trực tiếp
    logger.info(f"Saving to Parquet (V2-Coalesce) -> {output_dir}")
    df_final.coalesce(16).write.mode("overwrite").parquet(output_dir)
    
    # TỐI ƯU: Đếm số lượng từ metadata của file đã ghi (Cực nhanh vì chỉ đọc footer Parquet)
    final_count = spark.read.parquet(output_dir).count()
    return final_count


