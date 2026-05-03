# ============================================================
# src/data_validator_v2.py (OPTIMIZED VERSION)
# Kiểm tra chất lượng dữ liệu với cơ chế Caching một lần duy nhất.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
import logging

logger = logging.getLogger("data_validator_v2")

def validate_interactions(spark: SparkSession, parquet_dir: str) -> dict:
    """Kiểm tra output của ETL 2.1 với tối ưu Caching và Single Action"""
    print(f"\n[VALIDATE-V2] Dang kiem tra Interactions tai: {parquet_dir}")
    
    # Đọc và Cache luôn (Tránh đọc GCS nhiều lần cho mỗi lệnh count)
    df = spark.read.parquet(parquet_dir).persist(StorageLevel.MEMORY_AND_DISK)
    
    # TỐI ƯU: Gom tất cả các thống kê vào 1 Action duy nhất (collect)
    # Giảm từ 8 jobs xuống còn 1 job duy nhất.
    res = df.agg(
        F.count("*").alias("total"),
        F.countDistinct("user_id").alias("u_users"),
        F.countDistinct("product_id").alias("u_items"),
        F.sum(F.when((F.col("user_id").isNull()) | (F.col("user_id") == ""), 1).otherwise(0)).alias("null_u"),
        F.sum(F.when((F.col("product_id").isNull()) | (F.col("product_id") == ""), 1).otherwise(0)).alias("null_i"),
        F.min("rating").alias("r_min"),
        F.max("rating").alias("r_max"),
        F.avg("rating").alias("r_avg")
    ).collect()[0]
    
    stats = {
        "total_rows":       res["total"],
        "unique_users":     res["u_users"],
        "unique_items":     res["u_items"],
        "null_user_id":     res["null_u"],
        "null_item_id":     res["null_i"],
        "rating_min":       res["r_min"],
        "rating_max":       res["r_max"],
        "rating_avg":       round(res["r_avg"] or 0.0, 3),
    }

    print(f"  Tong tuong tac : {stats['total_rows']:,}")
    print(f"  Unique users   : {stats['unique_users']:,}")
    print(f"  Unique products: {stats['unique_items']:,}")
    print(f"  Null user_id   : {stats['null_user_id']}")
    print(f"  Null product_id: {stats['null_item_id']}")
    print(f"  Rating         : {stats['rating_min']} - {stats['rating_max']} (avg {stats['rating_avg']})")

    print("\n  Phan phoi Rating:")
    df.groupBy("rating").count().orderBy("rating").show()

    df.unpersist()
    return stats


def validate_item_nodes(spark: SparkSession, parquet_dir: str) -> dict:
    """Kiểm tra output của ETL 2.2 với tối ưu Caching và Single Action"""
    print(f"\n[VALIDATE-V2] Dang kiem tra Item Nodes tai: {parquet_dir}")
    
    df = spark.read.parquet(parquet_dir).persist(StorageLevel.MEMORY_AND_DISK)
    
    # TỐI ƯU: Gom các lệnh đếm vào 1 Action
    res = df.agg(
        F.count("*").alias("total"),
        F.countDistinct("product_id").alias("u_ids"),
        F.sum(F.when((F.col("full_text").isNull()) | (F.col("full_text") == ""), 1).otherwise(0)).alias("null_txt"),
        F.avg(F.length("full_text")).alias("avg_len")
    ).collect()[0]
    
    stats = {
        "total_items":      res["total"],
        "unique_item_ids":  res["u_ids"],
        "null_combined":    res["null_txt"],
        "avg_text_len":     round(res["avg_len"] or 0.0, 1),
    }

    print(f"  Tong san pham        : {stats['total_items']:,}")
    print(f"  Unique product_id    : {stats['unique_item_ids']:,}")
    print(f"  Null full_text       : {stats['null_combined']}")
    print(f"  Do dai text TB       : {stats['avg_text_len']} ky tu")

    print("\n  Phan bo theo Domain:")
    df.groupBy("domain").count().show()

    df.unpersist()
    return stats

