# ============================================================
# src/etl_interactions.py
# Giai đoạn 2.1 – ETL hàng loạt toàn bộ file Review/Tương tác
#
# Xử lý SONG SONG: Spark tự động chia file thành nhiều partition
# và xử lý đồng thời trên tất cả CPU core (hoặc nhiều node).
# ============================================================

import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, LongType

logger = logging.getLogger(__name__)


# ── Cột bắt buộc phải có trong file review ──────────────────
REQUIRED_COLS   = {"user_id", "parent_asin", "rating", "timestamp"}
PARTITION_COUNT = 20   # Số partition Parquet output – tăng nếu data lớn hơn


def _collect_review_files(data_dir: str) -> list[str]:
    """
    Quét thư mục, lấy tất cả file review:
      - .jsonl, .jsonl.gz, .json.gz
      - Loại bỏ file bắt đầu bằng 'meta_' hoặc chứa 'details'
    """
    all_files = os.listdir(data_dir)
    review_files = []

    for fname in sorted(all_files):
        is_valid_ext = (
            fname.endswith(".jsonl")
            or fname.endswith(".jsonl.gz")
            or fname.endswith(".json.gz")
        )
        is_excluded = fname.startswith("meta_") or "details" in fname.lower()

        if is_valid_ext and not is_excluded:
            review_files.append(os.path.join(data_dir, fname))

    return review_files


def _validate_columns(df: DataFrame, file_path: str) -> bool:
    """Kiểm tra file có đủ cột bắt buộc không."""
    existing = set(df.columns)
    missing  = REQUIRED_COLS - existing
    if missing:
        logger.warning(f"[SKIP] {os.path.basename(file_path)} thiếu cột: {missing}")
        return False
    return True


def run_etl_interactions(
    spark: SparkSession,
    data_dir: str,
    output_dir: str,
) -> int:
    """
    Đọc song song tất cả file review → làm sạch → lưu Parquet.

    Cơ chế phân tán:
    ─────────────────────────────────────────────────────────
    Spark chia mỗi file .gz thành các SPLIT (partition) có kích
    thước ~128MB (cấu hình ở spark_config). Mỗi partition được
    giao cho 1 task chạy độc lập trên 1 CPU core (hoặc 1 node).
    Toàn bộ quá trình filter, cast, dedup xảy ra song song.
    ─────────────────────────────────────────────────────────

    Returns: Số lượng tương tác sau khi làm sạch.
    """
    print("\n" + "="*60)
    print("  GIAI ĐOẠN 2.1 – ETL TƯƠNG TÁC (Interactions)")
    print("="*60)

    # ── Bước 1: Thu thập danh sách file ─────────────────────
    review_files = _collect_review_files(data_dir)
    if not review_files:
        raise FileNotFoundError(f"Không tìm thấy file review nào trong: {data_dir}")

    print(f"\n  Số file review tìm được: {len(review_files)}")
    for f in review_files:
        print(f"    - {os.path.basename(f)}")

    # ── Bước 2: Đọc song song TẤT CẢ file cùng lúc ──────────
    # spark.read.json([list]) → Spark tạo 1 job duy nhất,
    # phân phối từng file/partition ra các core/node đồng thời.
    print("\n  Đang đọc song song tất cả file...")
    df_raw = (
        spark.read
        .option("mode",          "DROPMALFORMED")  # Bỏ qua dòng lỗi JSON
        .option("samplingRatio", "0.01")           # Sample 1% để suy luận schema
        .json(review_files)                        # ← Điểm then chốt: list nhiều file
    )

    print(f"  Schema đọc được: {df_raw.columns}")

    # ── Bước 3: Kiểm tra cột bắt buộc ───────────────────────
    if not _validate_columns(df_raw, "combined"):
        raise ValueError("Các file review thiếu cột bắt buộc. Kiểm tra lại data.")

    # ── Bước 4: Transform & Làm sạch (chạy phân tán) ────────
    print("  Đang làm sạch và chuẩn hóa dữ liệu...")
    df_interactions = (
        df_raw
        # Chỉ giữ 4 cột cần thiết, đổi tên parent_asin → item_id
        .select(
            F.col("user_id"),
            F.col("parent_asin").alias("item_id"),
            F.col("rating").cast(FloatType()),
            F.col("timestamp").cast(LongType()),
        )
        # Loại bỏ dòng thiếu user hoặc item
        .dropna(subset=["user_id", "item_id"])

        # Loại bỏ rating không hợp lệ (ngoài 1-5)
        .filter(F.col("rating").between(1.0, 5.0))

        # Loại bỏ trùng lặp: mỗi cặp (user, item) chỉ giữ lại
        # tương tác MỚI NHẤT (timestamp lớn nhất)
        .orderBy(F.col("timestamp").desc())
        .dropDuplicates(subset=["user_id", "item_id"])
    )

    # ── Bước 5: Thêm cột partition theo category (nếu có) ───
    # Giúp các bước sau query nhanh hơn theo category
    if "main_category" in df_raw.columns:
        df_interactions = df_interactions.withColumn(
            "category",
            F.coalesce(F.col("main_category"), F.lit("unknown"))
        )

    # ── Bước 6: Đếm và lưu ──────────────────────────────────
    # .count() trigger 1 Spark Action → Spark thực thi toàn bộ
    # DAG song song trên các core/node lúc này
    total = df_interactions.count()
    print(f"\n  ✔ Tổng tương tác sau làm sạch: {total:,}")

    os.makedirs(output_dir, exist_ok=True)
    print(f"  Đang lưu Parquet → {output_dir}")

    # repartition(N): chia đều data ra N file Parquet
    # → Các bước đọc sau sẽ load song song N file cùng lúc
    (
        df_interactions
        .repartition(PARTITION_COUNT)
        .write
        .mode("overwrite")
        .parquet(output_dir)
    )

    print(f"  ✔ Đã lưu {PARTITION_COUNT} partition Parquet tại: {output_dir}\n")
    return total
