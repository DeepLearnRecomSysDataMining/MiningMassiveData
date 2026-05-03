# ============================================================
# src/schema_scanner.py
# Giai đoạn 0 – Khảo sát schema và thống kê nhanh tất cả file
# Chạy file này TRƯỚC để biết cấu trúc data_small
# ============================================================

import os
import json
import logging
from pyspark.sql import SparkSession

from src.file_utils import list_files, detect_jsonl_type

logger = logging.getLogger(__name__)


def scan_all_files(spark: SparkSession, data_dir: str) -> dict:
    """
    Quet toan bo file .jsonl trong data_dir.
    Tra ve dict: { 'vn_review': [], 'amz_review': [], 'vn_item': [], 'amz_item': [] }
    Gup tranh viec scan lap lai o cac giai doan sau.
    """
    print("\n" + "="*60)
    print("  SCHEMA SCANNER - Khao sat va Phan loai du lieu")
    print("="*60)

    file_groups = {
        "vn_review": [],
        "amz_review": [],
        "vn_item": [],
        "amz_item": [],
        "unknown": []
    }
    
    all_paths = sorted(list_files(data_dir))
    target_paths = [p for p in all_paths if p.endswith(".jsonl")]

    print(f"\n  Tong so file can phan loai: {len(target_paths)}\n")

    for file_path in target_paths:
        file_name = os.path.basename(file_path)
        f_type = detect_jsonl_type(file_path)
        
        if f_type in file_groups:
            file_groups[f_type].append(file_path)
            print(f"  [OK] {file_name} -> {f_type}")
        else:
            file_groups["unknown"].append(file_path)
            print(f"  [??] {file_name} -> unknown")

    # -- Tom tat cuoi ----------------------------------------
    print("\n" + "="*60)
    print("  TOM TAT PHAN LOAI")
    print("="*60)
    for g, paths in file_groups.items():
        if paths:
            print(f"  {g:<12}: {len(paths)} file(s)")
    print("="*60 + "\n")

    return file_groups

