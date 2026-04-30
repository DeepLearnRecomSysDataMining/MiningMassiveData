# ============================================================
# src/schema_scanner.py
# Giai đoạn 0 – Khảo sát schema và thống kê nhanh tất cả file
# Chạy file này TRƯỚC để biết cấu trúc data_small
# ============================================================

import os
import json
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def scan_all_files(spark: SparkSession, data_dir: str) -> dict:
    """
    Quet toan bo file .jsonl va .jsonl.gz / .json.gz trong data_dir.
    Tra ve dict: {filename: {"columns": [...], "count_approx": int}}
    """
    print("\n" + "="*60)
    print("  SCHEMA SCANNER - Khao sat cau truc du lieu tho")
    print("="*60)

    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"Khong tim thay thu muc: {data_dir}")

    results = {}
    files = sorted(os.listdir(data_dir))
    target_files = [
        f for f in files
        if f.endswith(".jsonl")
        or f.endswith(".jsonl.gz")
        or f.endswith(".json.gz")
    ]

    print(f"\n  Tong so file can quet: {len(target_files)}\n")

    for file_name in target_files:
        file_path = os.path.join(data_dir, file_name)
        print(f"  -> Dang doc: {file_name} ...")

        try:
            df = (
                spark.read
                .option("mode", "DROPMALFORMED")
                .json(file_path)
            )

            dtypes   = df.dtypes
            n_rows = df.count()

            results[file_name] = {
                "path":       file_path,
                "columns":    dtypes,
                "count":      n_rows,
                "is_meta":    file_name.startswith("meta_"),
            }

            print(f"     So cot   : {len(dtypes)}")
            print(f"     So dong  : {n_rows:,}")
            for col_name, col_type in dtypes[:15]:
                short_type = col_type if len(col_type) < 55 else col_type[:52] + "..."
                print(f"       - {col_name}: {short_type}")
            if len(dtypes) > 15:
                print(f"       ... va {len(dtypes) - 15} cot khac")
            print()

        except Exception as e:
            print(f"     [LOI] Khong doc duoc file: {e}\n")
            results[file_name] = {"error": str(e)}

    # -- Tom tat cuoi ----------------------------------------
    print("\n" + "="*60)
    print("  TOM TAT")
    print("="*60)
    meta_files    = [f for f, v in results.items() if v.get("is_meta")]
    review_files  = [f for f, v in results.items() if not v.get("is_meta") and "error" not in v]
    print(f"  File META  (san pham) : {len(meta_files)}")
    print(f"  File REVIEW (tuong tac): {len(review_files)}")
    print("="*60 + "\n")

    return results
