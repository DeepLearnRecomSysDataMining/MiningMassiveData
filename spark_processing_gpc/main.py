import sys
import time
import logging
import argparse
import os
from pathlib import Path

from config.spark_config    import create_spark_session, PathConfig
from src.schema_scanner     import scan_all_files
from src.etl_interactions   import run_etl_interactions
from src.etl_item_nodes     import run_etl_item_nodes
from src.evaluation_dataset import run_evaluation_generator
from src.data_validator     import validate_interactions, validate_item_nodes
from src.file_utils         import decompress_gz_files

# Setup logging
is_cloud = os.getenv("SPARK_ENV") == "cloud"

log_handlers = [logging.StreamHandler(sys.stdout)]

# Chỉ tạo FileHandler và thư mục logs nếu KHÔNG chạy trên Cloud
if not is_cloud:
    os.makedirs(PathConfig.LOGS_DIR, exist_ok=True)
    log_handlers.append(logging.FileHandler(os.path.join(PathConfig.LOGS_DIR, "etl_pipeline.log"), encoding="utf-8"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=log_handlers,
)
logger = logging.getLogger("main")


def parse_args():
    parser = argparse.ArgumentParser(description="Amazon ETL Pipeline voi PySpark")
    parser.add_argument("--scan-only",  action="store_true", help="Chi chay Schema Scanner")
    parser.add_argument("--skip-scan",  action="store_true", help="Bo qua buoc scan")
    parser.add_argument("--validate",   action="store_true", help="Validate output sau ETL")
    parser.add_argument("--decompress", action="store_true", help="Giai nen file .gz truoc khi xu ly")
    parser.add_argument("--data-dir",   default=PathConfig.RAW_DATA_DIR, help=f"Thu muc data tho (default: {PathConfig.RAW_DATA_DIR})")
    return parser.parse_args()


def print_banner():
    print("""
+----------------------------------------------------------+
|       AMAZON x VN  -  BIG DATA ETL PIPELINE             |
|       PySpark  .  Phan tan song song  .  Parquet         |
+----------------------------------------------------------+
""")


def main():
    print_banner()
    args    = parse_args()
    t_start = time.time()
    n_interactions = 0
    n_items = 0
    n_eval = 0

    # -- Buoc tien xu ly: Giai nen neu can -------------------
    if args.decompress:
        logger.info("Dang kiem tra va giai nen cac file .gz...")
        decompress_gz_files(args.data_dir)

    # -- Khoi tao SparkSession --------------------------------
    spark = create_spark_session("AmazonETL_Pipeline")

    try:
        # BUOC 0: Schema Scanner
        if not args.skip_scan:
            logger.info("Bat dau Schema Scanner...")
            scan_all_files(spark, args.data_dir)
            if args.scan_only:
                logger.info("--scan-only: dung sau khi scan xong.")
                return

        # BUOC 1: ETL Tuong tac (Giai doan 2.1)
        logger.info("Bat dau ETL Interactions (Giai doan 2.1)...")
        t1 = time.time()
        n_interactions = run_etl_interactions( spark, data_dir = args.data_dir, output_dir = PathConfig.INTERACTIONS_OUT, )
        logger.info(f"ETL 2.1 hoan tat: {n_interactions:,} tuong tac | " f"Thoi gian: {time.time()-t1:.1f}s")

        # BUOC 2: ETL San pham (Giai doan 2.2)
        logger.info("Bat dau ETL Item Nodes (Giai doan 2.2)...")
        t2 = time.time()
        n_items = run_etl_item_nodes( spark, data_dir = args.data_dir, output_dir = PathConfig.ITEM_NODES_OUT, )
        logger.info(f"ETL 2.2 hoan tat: {n_items:,} san pham | "f"Thoi gian: {time.time()-t2:.1f}s")

        # BUOC 3: Tao Evaluation Dataset
        logger.info("Bat dau tao Evaluation Dataset...")
        t3 = time.time()
        n_eval = run_evaluation_generator( spark, item_nodes_path = PathConfig.ITEM_NODES_OUT, output_path = PathConfig.EVALUATION_OUT)
        logger.info(f"Tao Evaluation Dataset hoan tat: {n_eval:,} bo | " f"Thoi gian: {time.time()-t3:.1f}s")

        # BUOC 4 (tuy chon): Validate output
        if args.validate:
            logger.info("Dang validate output...")
            validate_interactions(spark, PathConfig.INTERACTIONS_OUT)
            validate_item_nodes(spark,   PathConfig.ITEM_NODES_OUT)

        # -- Tong ket -----------------------------------------
        elapsed = time.time() - t_start
        print(f"""
        +----------------------------------------------------------+
        |  V  PIPELINE HOAN TAT
        |     Tuong tac : {n_interactions:>12,}
        |     San pham  : {n_items:>12,}
        |     Evaluation: {n_eval:>12,}
        |     Thoi gian : {elapsed:>11.1f} s
        |
        |  Output:
        |    {PathConfig.INTERACTIONS_OUT}
        |    {PathConfig.ITEM_NODES_OUT}
        |    {PathConfig.EVALUATION_OUT}
        +----------------------------------------------------------+
        """)

    except Exception as e:
        logger.error(f"PIPELINE LOI: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession da dong.")

if __name__ == "__main__":
    main()
