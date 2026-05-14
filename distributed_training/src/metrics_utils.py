# src/metrics_utils.py
import os
import csv
import logging
from datetime import datetime

logger = logging.getLogger("metrics_utils")

def write_metrics_csv(csv_path, rows):
    try:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
        if isinstance(rows, dict):
            rows = [rows]

        rows = [
            {"timestamp": datetime.utcnow().isoformat(), **row}
            for row in rows
        ]

        if not rows:
            return

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        logger.error(f"Lỗi khi ghi file {csv_path}: {e}")