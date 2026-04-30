# ============================================================
# src/data_validator.py
# Kiểm tra chất lượng dữ liệu sau ETL trước khi sang Embedding
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def validate_interactions(spark: SparkSession, parquet_dir: str) -> dict:
    """Kiểm tra output của ETL 2.1"""
    print("\n[VALIDATE] Kiem tra Interactions Parquet...")
    df = spark.read.parquet(parquet_dir)

    stats = {
        "total_rows":       df.count(),
        "unique_users":     df.select("user_id").distinct().count(),
        "unique_items":     df.select("product_id").distinct().count(),
        "null_user_id":     df.filter(F.col("user_id").isNull()).count(),
        "null_item_id":     df.filter(F.col("product_id").isNull()).count(),
        "rating_min":       df.agg(F.min("rating")).first()[0],
        "rating_max":       df.agg(F.max("rating")).first()[0],
        "rating_avg":       round(df.agg(F.avg("rating")).first()[0] or 0.0, 3),
    }

    print(f"  Tổng tương tác : {stats['total_rows']:,}")
    print(f"  Unique users   : {stats['unique_users']:,}")
    print(f"  Unique products: {stats['unique_items']:,}")
    print(f"  Null user_id   : {stats['null_user_id']}")
    print(f"  Null product_id: {stats['null_item_id']}")
    print(f"  Rating         : {stats['rating_min']} – {stats['rating_max']} (avg {stats['rating_avg']})")

    # Phân phối rating
    print("\n  Phân phối Rating:")
    df.groupBy("rating").count().orderBy("rating").show()

    return stats


def validate_item_nodes(spark: SparkSession, parquet_dir: str) -> dict:
    """Kiểm tra output của ETL 2.2"""
    print("\n[VALIDATE] Kiem tra Item Nodes Parquet...")
    df = spark.read.parquet(parquet_dir)

    stats = {
        "total_items":      df.count(),
        "unique_item_ids":  df.select("product_id").distinct().count(),
        "null_combined":    df.filter(F.col("full_text").isNull()).count(),
        "avg_text_len":     round(df.agg(F.avg(F.length("full_text"))).first()[0] or 0.0, 1),
    }

    print(f"  Tổng sản phẩm        : {stats['total_items']:,}")
    print(f"  Unique product_id    : {stats['unique_item_ids']:,}")
    print(f"  Null full_text       : {stats['null_combined']}")
    print(f"  Độ dài text TB       : {stats['avg_text_len']} ký tự")

    print("\n  Phân bổ theo Domain:")
    df.groupBy("domain").count().show()

    return stats
