# ============================================================
# src/etl_item_nodes.py
# Refactored to match MMD_v3 logic
# ============================================================

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat_ws, lit, lower, regexp_replace, udf, when
from pyspark.sql.types import StringType, MapType, ArrayType
import ast

logger = logging.getLogger("etl_item_nodes")

# --- UDFs for Text Cleaning and Category Extraction ---

@udf(returnType=StringType())
def clean_text_udf(val):
    if val is None: return ""
    if isinstance(val, list): return " ".join([str(x) for x in val])
    if isinstance(val, str):
        if val.startswith('['):
            try:
                val_list = ast.literal_eval(val)
                if isinstance(val_list, list): return " ".join([str(x) for x in val_list])
            except: pass
        return val
    return str(val)

@udf(returnType=MapType(StringType(), StringType()))
def parse_specs_udf(spec_text):
    specs = {}
    if isinstance(spec_text, list):
        for item in spec_text:
            if '::' in str(item):
                parts = str(item).split('::', 1)
                if len(parts) == 2: specs[parts[0].strip().lower()] = parts[1].strip().lower()
    elif isinstance(spec_text, str) and spec_text.startswith('['):
        try:
            items = ast.literal_eval(spec_text)
            for item in items:
                if '::' in str(item):
                    parts = str(item).split('::', 1)
                    if len(parts) == 2: specs[parts[0].strip().lower()] = parts[1].strip().lower()
        except: pass
    elif isinstance(spec_text, dict):
        for k, v in spec_text.items():
            specs[str(k).lower()] = str(v).lower()
    return specs

@udf(returnType=StringType())
def get_category_udf(breadcrumb, product_name):
    text = f"{str(breadcrumb)} {str(product_name)}".lower()
    if any(w in text for w in ['laptop', 'macbook', 'máy tính xách tay']): return 'laptop'
    elif any(w in text for w in ['điện thoại', 'smartphone', 'iphone', 'dtdd']): return 'smartphone'
    elif any(w in text for w in ['tivi', 'tv', 'television']): return 'television'
    elif any(w in text for w in ['tai nghe', 'headphone', 'earphone', 'airpods']): return 'headphone'
    elif any(w in text for w in ['màn hình', 'monitor']): return 'monitor'
    elif any(w in text for w in ['để bàn', 'desktop', 'pc', 'máy tính bộ']): return 'desktop'
    elif any(w in text for w in ['tablet', 'máy tính bảng', 'ipad']): return 'tablet'
    return 'other'

def run_etl_item_nodes(spark: SparkSession, data_dir: str, output_dir: str) -> int:
    """
    Standardizes Amazon and VN metadata into a common schema.
    """
    logger.info(f"Đang quét metadata từ: {data_dir}")

    # 1. Tìm các file metadata
    all_files = os.listdir(data_dir)
    vn_files = [os.path.join(data_dir, f) for f in all_files if ("metadatas" in f and "vn" in f) or ("dmx_metadatas" in f) or ("tgdd_metadatas" in f)]
    amz_files = [os.path.join(data_dir, f) for f in all_files if ("metadatas" in f and "amazon" in f) or (f == "sample_metadatas.jsonl")]

    df_final = None

    # 2. Xử lý dữ liệu Việt Nam
    if vn_files:
        logger.info(f"Đang xử lý {len(vn_files)} file VN metadata...")
        df_vn = spark.read.json(vn_files)
        
        # Mapping fields to standard schema
        df_vn_std = df_vn.select(
            col("product_id"),
            col("asin"),
            clean_text_udf(col("product_name")).alias("product_name"),
            clean_text_udf(col("specifications")).alias("specs_text"),
            clean_text_udf(col("description")).alias("desc_text"),
            lit("").alias("breadcrumb") # VN schema doesn't have breadcrumb in sample
        ).withColumn(
            "category", get_category_udf(col("breadcrumb"), col("product_name"))
        ).withColumn(
            "parsed_specs", parse_specs_udf(col("specs_text"))
        ).withColumn(
            "full_text", lower(concat_ws(" ", col("product_name"), col("specs_text"), col("desc_text")))
        ).withColumn("domain", lit("vn"))

        df_final = df_vn_std.select("product_id", "asin", "product_name", "category", "full_text", "parsed_specs", "domain")

    # 3. Xử lý dữ liệu Amazon
    if amz_files:
        logger.info(f"Đang xử lý {len(amz_files)} file Amazon metadata...")
        df_amz = spark.read.json(amz_files)
        
        # Amazon schema has different names
        df_amz_std = df_amz.select(
            col("parent_asin").alias("product_id"), # Use parent_asin as ID
            col("asin"),
            clean_text_udf(col("title")).alias("product_name"),
            clean_text_udf(col("features")).alias("specs_text"),
            clean_text_udf(col("description")).alias("desc_text"),
            clean_text_udf(col("main_category")).alias("breadcrumb")
        ).withColumn(
            "category", get_category_udf(col("breadcrumb"), col("product_name"))
        ).withColumn(
            "parsed_specs", parse_specs_udf(col("specs_text"))
        ).withColumn(
            "full_text", lower(concat_ws(" ", col("product_name"), col("specs_text"), col("desc_text")))
        ).withColumn("domain", lit("amazon"))

        df_amz_final = df_amz_std.select("product_id", "asin", "product_name", "category", "full_text", "parsed_specs", "domain")
        
        if df_final is None:
            df_final = df_amz_final
        else:
            df_final = df_final.unionByName(df_amz_final)

    if df_final is None:
        logger.warning("Không tìm thấy file metadata nào!")
        return 0

    # 4. Làm sạch: Xóa dòng lỗi, lọc trùng
    df_final = df_final.dropna(subset=["product_id", "asin"]) \
                       .dropDuplicates(["product_id"])

    # 5. Lưu xuống Parquet
    count = df_final.count()
    logger.info(f"Lưu {count} sản phẩm chuẩn hóa xuống Parquet...")
    df_final.repartition(10).write.mode("overwrite").parquet(output_dir)
    
    return count
