# ============================================================
# src/etl_item_nodes.py
# Giai đoạn 2.2 – ETL hàng loạt file Meta (sản phẩm)
#
# Hỗ trợ cả file Amazon (.jsonl.gz / .json.gz) lẫn file
# Việt Nam (.jsonl) – gộp bằng unionByName.
# ============================================================

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

PARTITION_COUNT = 10


def _collect_meta_files(data_dir: str) -> tuple[list[str], list[str]]:
    """
    Phân loại file trong thư mục thành:
      - meta_files  : file Amazon (bắt đầu bằng 'meta_')
      - detail_files: file Việt Nam (chứa 'details')
    """
    all_files = sorted(os.listdir(data_dir))
    meta_files, detail_files = [], []

    for fname in all_files:
        fpath = os.path.join(data_dir, fname)
        is_gz    = fname.endswith(".jsonl.gz") or fname.endswith(".json.gz")
        is_jsonl = fname.endswith(".jsonl")

        if fname.startswith("meta_") and (is_gz or is_jsonl):
            meta_files.append(fpath)
        elif "details" in fname.lower() and (is_gz or is_jsonl):
            detail_files.append(fpath)

    return meta_files, detail_files


def _build_item_df(
    spark: SparkSession,
    file_paths: list[str],
    domain_label: str,
) -> DataFrame | None:
    """
    Đọc song song nhiều file meta, chuẩn hóa sang schema chung:
        item_id | title | categories_str | description_str | features_str | domain
    """
    if not file_paths:
        return None

    print(f"    Đọc {len(file_paths)} file [{domain_label}] song song...")

    df_raw = (
        spark.read
        .option("mode",          "DROPMALFORMED")
        .option("samplingRatio", "0.005")
        .json(file_paths)
    )

    # Xác định cột ID: ưu tiên parent_asin, fallback sang asin
    id_col = (
        F.col("parent_asin")
        if "parent_asin" in df_raw.columns
        else F.col("asin")
    )

    # ── Xử lý cột description ────────────────────────────────
    # Có thể là array<string> hoặc string tùy file → chuẩn hóa
    if "description" in df_raw.columns:
        desc_col = F.when(
            F.col("description").cast("string").startswith("["),
            F.concat_ws(". ", F.col("description"))
        ).otherwise(F.col("description").cast("string"))
    else:
        desc_col = F.lit("")

    # ── Xử lý categories ─────────────────────────────────────
    if "categories" in df_raw.columns:
        cat_col = F.concat_ws(", ", F.col("categories"))
    else:
        cat_col = F.lit("")

    # ── Xử lý features ───────────────────────────────────────
    if "features" in df_raw.columns:
        feat_col = F.concat_ws(". ", F.col("features"))
    else:
        feat_col = F.lit("")

    df_items = (
        df_raw
        .select(
            id_col.alias("item_id"),
            F.col("title").cast("string").alias("title"),
            cat_col.alias("categories_str"),
            desc_col.alias("description_str"),
            feat_col.alias("features_str"),
        )
        .withColumn("domain", F.lit(domain_label))
    )

    return df_items


def run_etl_item_nodes(
    spark: SparkSession,
    data_dir: str,
    output_dir: str,
) -> int:
    """
    Đọc song song tất cả file meta/details →
    gộp Amazon + VN → gom text → lưu Parquet.

    Returns: Số lượng sản phẩm unique sau làm sạch.
    """
    print("\n" + "="*60)
    print("  GIAI ĐOẠN 2.2 – ETL SẢN PHẨM (Item Nodes)")
    print("="*60)

    # ── Bước 1: Phân loại file ───────────────────────────────
    meta_files, detail_files = _collect_meta_files(data_dir)
    print(f"\n  Amazon meta files  : {len(meta_files)}")
    print(f"  VN detail files    : {len(detail_files)}")

    if not meta_files and not detail_files:
        raise FileNotFoundError("Không tìm thấy file meta hoặc details nào!")

    # ── Bước 2: Đọc song song từng nhóm ─────────────────────
    df_amazon = _build_item_df(spark, meta_files,   domain_label="amazon")
    df_vn     = _build_item_df(spark, detail_files, domain_label="vn")

    # ── Bước 3: Gộp 2 domain ────────────────────────────────
    if df_amazon is not None and df_vn is not None:
        print("  Đang gộp Amazon + VN bằng unionByName...")
        # allowMissingColumns=True: bỏ qua cột thiếu, điền null
        df_items = df_amazon.unionByName(df_vn, allowMissingColumns=True)
    elif df_amazon is not None:
        df_items = df_amazon
    else:
        df_items = df_vn

    # ── Bước 4: Tạo combined_text ────────────────────────────
    # Gộp tất cả trường text thành 1 chuỗi duy nhất → dùng cho embedding
    print("  Đang tạo cột combined_text...")
    df_items = (
        df_items
        .withColumn(
            "combined_text",
            F.concat_ws(
                " | ",
                F.col("title"),
                F.col("categories_str"),
                F.col("description_str"),
                F.col("features_str"),
            )
        )
        # Loại bỏ combined_text rỗng hoặc quá ngắn
        .filter(F.length(F.col("combined_text")) > 10)
    )

    # ── Bước 5: Chỉ giữ 3 cột quan trọng, loại trùng ────────
    df_item_nodes = (
        df_items
        .select("item_id", "combined_text", "domain")
        .dropna(subset=["item_id"])
        .dropDuplicates(subset=["item_id"])
    )

    # ── Bước 6: Thống kê theo domain ────────────────────────
    print("\n  Thống kê theo domain:")
    df_item_nodes.groupBy("domain").count().show()

    total = df_item_nodes.count()
    print(f"  ✔ Tổng sản phẩm unique: {total:,}")

    # ── Bước 7: Lưu Parquet phân vùng theo domain ───────────
    os.makedirs(output_dir, exist_ok=True)
    print(f"  Đang lưu Parquet (partition by domain) → {output_dir}")

    (
        df_item_nodes
        .repartition(PARTITION_COUNT, F.col("domain"))  # Partition theo domain
        .write
        .mode("overwrite")
        .partitionBy("domain")   # ← Tạo thư mục domain=amazon/ và domain=vn/
        .parquet(output_dir)     #   Giúp filter theo domain nhanh hơn 10x
    )

    print(f"  ✔ Đã lưu tại: {output_dir}\n")
    return total
