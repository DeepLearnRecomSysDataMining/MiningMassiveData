# ============================================================
# src/schema_scanner.py
# Giai đoạn 0 – Khảo sát schema và thống kê nhanh tất cả file
# Chạy file này TRƯỚC để biết cấu trúc data_small
# ============================================================

import os
import json
import logging
from pyspark.sql import SparkSession

from .file_utils import list_files

logger = logging.getLogger(__name__)


def scan_all_files(spark: SparkSession, data_dir: str) -> dict:
    """
    Quet toan bo file .jsonl va .jsonl.gz / .json.gz trong data_dir.
    Tra ve dict: {filename: {"columns": [...], "count_approx": int}}
    """
    print("\n" + "="*60)
    print("  SCHEMA SCANNER - Khao sat cau truc du lieu tho")
    print("="*60)

    results = {}
    all_paths = sorted(list_files(data_dir))
    target_paths = [
        p for p in all_paths
        if p.endswith(".jsonl")
        or p.endswith(".jsonl.gz")
        or p.endswith(".json.gz")
    ]

    print(f"\n  Tong so file can quet: {len(target_paths)}\n")

    for file_path in target_paths:
        file_name = os.path.basename(file_path)
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
