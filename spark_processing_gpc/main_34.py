import sys
import time
import logging
import os
from config.spark_config    import create_spark_session, PathConfig
from src.evaluation_dataset_v2 import run_evaluation_generator
from src.data_validator_v2     import validate_interactions, validate_item_nodes
from src.debug_utils import log_spark_configs

# Setup logging cho Cloud
log_handlers = [logging.StreamHandler(sys.stdout)]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=log_handlers,
)
logger = logging.getLogger("main2_eval")

def main():
    print("""
+----------------------------------------------------------+
|       AMAZON x VN  -  EVALUATION GENERATOR ONLY         |
|       Chay doc lap Phase 3 (Negative Mining)             |
+----------------------------------------------------------+
""")
    t_start = time.time()

    spark = create_spark_session("AmazonETL_Phase3_Only")
    log_spark_configs(spark)
    
    try:
        # KIEM TRA DU LIEU DAU VAO CUA PHASE 2
        logger.info(f"Kiem tra du lieu dau vao tai: {PathConfig.ITEM_NODES_OUT}")
        
        # BUOC 3: Tao Evaluation Dataset
        logger.info(">>> START PHASE 3: Evaluation Dataset Generation (Standalone)")
        t3 = time.time()
        n_eval = run_evaluation_generator( 
            spark, 
            items_path = PathConfig.ITEM_NODES_OUT, 
            output_path = PathConfig.EVALUATION_OUT
        )
        logger.info(f"V PHASE 3 DONE: {n_eval:,} evaluation queries | Time: {time.time()-t3:.1f}s")

        # BUOC 4: Tuy chon Validate
        if "--validate" in sys.argv:
            logger.info(">>> START PHASE 4: Data Validation")
            # Kiem tra dau vao
            validate_item_nodes(spark, PathConfig.ITEM_NODES_OUT)
            # Kiem tra dau ra cua Phase 3
            if n_eval > 0:
                logger.info(f"Kiem tra file Evaluation tai: {PathConfig.EVALUATION_OUT}")
                df_check = spark.read.parquet(PathConfig.EVALUATION_OUT)
                df_check.printSchema()
                logger.info(f"So luong ban ghi thuc te: {df_check.count():,}")
            
            logger.info("V PHASE 4 DONE: Validation complete.")

        # Tong ket
        elapsed = time.time() - t_start
        print(f"""
        +----------------------------------------------------------+
        |  V  PHASE 3 HOAN TAT
        |     Evaluation Queries: {n_eval:>12,}
        |     Thoi gian chay    : {elapsed:>11.1f} s
        |
        |  Output GCS:
        |    {PathConfig.EVALUATION_OUT}
        +----------------------------------------------------------+
        """)

    except Exception as e:
        logger.error(f"LOI KHI CHAY PHASE 3: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("SparkSession da dong.")

if __name__ == "__main__":
    main()
