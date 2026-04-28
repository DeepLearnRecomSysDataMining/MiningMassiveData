# ============================================================
# src/schema_scanner.py
# Giai đoạn 0 – Khảo sát schema và thống kê nhanh tất cả file
# Chạy file này TRƯỚC để biết cấu trúc data
# ============================================================

import os
import json
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def scan_all_files(spark: SparkSession, data_dir: str) -> dict:
    """
    Quét toàn bộ file .jsonl và .jsonl.gz / .json.gz trong data_dir.
    Trả về dict: {filename: {"columns": [...], "count_approx": int}}
    """
    print("\n" + "="*60)
    print("  SCHEMA SCANNER – Khảo sát cấu trúc dữ liệu thô")
    print("="*60)

    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Không tìm thấy thư mục: {data_dir}")

    results = {}
    files = sorted(os.listdir(data_dir))
    target_files = [
        f for f in files
        if f.endswith(".jsonl")
        or f.endswith(".jsonl.gz")
        or f.endswith(".json.gz")   # ← hỗ trợ thêm .json.gz
    ]

    print(f"\n  Tổng số file cần quét: {len(target_files)}\n")

    for file_name in target_files:
        file_path = os.path.join(data_dir, file_name)
        print(f"  → Đang đọc: {file_name} ...")

        try:
            df = (
                spark.read
                .option("mode",          "DROPMALFORMED")
                .option("samplingRatio", "0.001")   # Chỉ sample 0.1% để suy luận schema nhanh
                .json(file_path)
            )

            dtypes   = df.dtypes
            n_approx = df.rdd.countApprox(timeout=5000, confidence=0.9)

            results[file_name] = {
                "path":       file_path,
                "columns":    dtypes,
                "count_approx": n_approx,
                "is_meta":    file_name.startswith("meta_"),
            }

            print(f"     Số cột   : {len(dtypes)}")
            print(f"     Số dòng  : ~{n_approx:,}")
            for col_name, col_type in dtypes[:15]:
                short_type = col_type if len(col_type) < 55 else col_type[:52] + "..."
                print(f"       - {col_name}: {short_type}")
            if len(dtypes) > 15:
                print(f"       ... và {len(dtypes) - 15} cột khác")
            print()

        except Exception as e:
            print(f"     [LỖI] Không đọc được file: {e}\n")
            results[file_name] = {"error": str(e)}

    # ── Tóm tắt cuối ────────────────────────────────────────
    print("\n" + "="*60)
    print("  TÓM TẮT")
    print("="*60)
    meta_files    = [f for f, v in results.items() if v.get("is_meta")]
    review_files  = [f for f, v in results.items() if not v.get("is_meta") and "error" not in v]
    print(f"  File META  (sản phẩm) : {len(meta_files)}")
    print(f"  File REVIEW (tương tác): {len(review_files)}")
    print("="*60 + "\n")

    return results
