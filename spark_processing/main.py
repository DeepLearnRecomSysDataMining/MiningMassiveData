# ============================================================
# main.py  –  Entry point chính của project
# Chạy file này từ PyCharm để thực thi toàn bộ ETL pipeline
#
# Cách dùng:
#   python main.py                    → chạy tất cả giai đoạn
#   python main.py --scan-only        → chỉ khảo sát schema
#   python main.py --skip-scan        → bỏ qua scan, chạy ETL luôn
#   python main.py --validate         → kiểm tra output sau ETL
# ============================================================

import sys
import time
import logging
import argparse
import os

# ── Setup logging ra file + console ─────────────────────────
LOG_DIR = r"D:\Data\amazon_processed\logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "etl_pipeline.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("main")

# ── Import các module trong project ─────────────────────────
from config.spark_config import create_spark_session, PathConfig
from src.schema_scanner  import scan_all_files
from src.etl_interactions import run_etl_interactions
from src.etl_item_nodes   import run_etl_item_nodes
from src.evaluation_dataset import run_evaluation_generator
from src.data_validator   import validate_interactions, validate_item_nodes


def parse_args():
    parser = argparse.ArgumentParser(description="Amazon ETL Pipeline với PySpark")
    parser.add_argument("--scan-only",  action="store_true", help="Chỉ chạy Schema Scanner")
    parser.add_argument("--skip-scan",  action="store_true", help="Bỏ qua bước scan")
    parser.add_argument("--validate",   action="store_true", help="Validate output sau ETL")
    parser.add_argument(
        "--data-dir",
        default=PathConfig.RAW_DATA_DIR,
        help=f"Thư mục data thô (default: {PathConfig.RAW_DATA_DIR})"
    )
    return parser.parse_args()


def print_banner():
    print("""
╔══════════════════════════════════════════════════════════╗
║       AMAZON × VN  –  BIG DATA ETL PIPELINE             ║
║       PySpark  ·  Phân tán song song  ·  Parquet         ║
╚══════════════════════════════════════════════════════════╝
""")


def main():
    print_banner()
    args    = parse_args()
    t_start = time.time()
    n_interactions = 0
    n_items = 0
    n_eval = 0

    # ── Khởi tạo SparkSession ────────────────────────────────
    spark = create_spark_session("AmazonETL_Pipeline")

    try:
        # ────────────────────────────────────────────────────
        # BƯỚC 0: Schema Scanner
        # ────────────────────────────────────────────────────
        if not args.skip_scan:
            logger.info("Bắt đầu Schema Scanner...")
            scan_all_files(spark, args.data_dir)

            if args.scan_only:
                logger.info("--scan-only: dừng sau khi scan xong.")
                return

        # ────────────────────────────────────────────────────
        # BƯỚC 1: ETL Tương tác (Giai đoạn 2.1)
        # ────────────────────────────────────────────────────
        logger.info("Bắt đầu ETL Interactions (Giai đoạn 2.1)...")
        t1 = time.time()

        n_interactions = run_etl_interactions(
            spark,
            data_dir   = args.data_dir,
            output_dir = PathConfig.INTERACTIONS_OUT,
        )

        logger.info(f"ETL 2.1 hoàn tất: {n_interactions:,} tương tác | "
                    f"Thời gian: {time.time()-t1:.1f}s")

        # ────────────────────────────────────────────────────
        # BƯỚC 2: ETL Sản phẩm (Giai đoạn 2.2)
        # ────────────────────────────────────────────────────
        logger.info("Bắt đầu ETL Item Nodes (Giai đoạn 2.2)...")
        t2 = time.time()

        n_items = run_etl_item_nodes(
            spark,
            data_dir   = args.data_dir,
            output_dir = PathConfig.ITEM_NODES_OUT,
        )

        logger.info(f"ETL 2.2 hoàn tất: {n_items:,} sản phẩm | "
                    f"Thời gian: {time.time()-t2:.1f}s")

        # ────────────────────────────────────────────────────
        # BƯỚC 3: Tạo Evaluation Dataset (MỚI)
        # ────────────────────────────────────────────────────
        logger.info("Bắt đầu tạo Evaluation Dataset...")
        t3 = time.time()
        
        n_eval = run_evaluation_generator(
            spark,
            item_nodes_path = PathConfig.ITEM_NODES_OUT,
            output_path     = PathConfig.EVALUATION_OUT
        )
        
        logger.info(f"Tạo Evaluation Dataset hoàn tất: {n_eval:,} bộ | "
                    f"Thời gian: {time.time()-t3:.1f}s")

        # ────────────────────────────────────────────────────
        # BƯỚC 3 (tuỳ chọn): Validate output
        # ────────────────────────────────────────────────────
        if args.validate:
            logger.info("Đang validate output...")
            validate_interactions(spark, PathConfig.INTERACTIONS_OUT)
            validate_item_nodes(spark,   PathConfig.ITEM_NODES_OUT)

        # ── Tổng kết ─────────────────────────────────────────
        elapsed = time.time() - t_start
        print(f"""
╔══════════════════════════════════════════════════════════╗
║  ✔  PIPELINE HOÀN TẤT
║     Tương tác : {n_interactions:>12,}
║     Sản phẩm  : {n_items:>12,}
║     Evaluation: {n_eval:>12,}
║     Thời gian : {elapsed:>11.1f} s
║
║  Output:
║    {PathConfig.INTERACTIONS_OUT}
║    {PathConfig.ITEM_NODES_OUT}
║    {PathConfig.EVALUATION_OUT}
╚══════════════════════════════════════════════════════════╝
""")

    except Exception as e:
        logger.error(f"PIPELINE LỖI: {e}", exc_info=True)
        raise

    finally:
        spark.stop()
        logger.info("SparkSession đã đóng.")


if __name__ == "__main__":
    main()
