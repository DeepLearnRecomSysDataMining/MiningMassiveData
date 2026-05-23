import sys
import time
import logging

from config.spark_config import create_spark_session, PathConfig
from src.prepare_llm_chgnn_train_dataset_v2 import run_prepare_llm_chgnn_train_dataset
from src.debug_utils import log_spark_configs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("main_llm_chgnn")


def main():

    print("""
+----------------------------------------------------------+
|     LLM-CHGNN TRAIN DATASET GENERATOR                    |
+----------------------------------------------------------+
""")

    t_start = time.time()

    spark = create_spark_session( "LLM_CHGNN_Train_Dataset")
    log_spark_configs(spark)

    try:
        n_rows = run_prepare_llm_chgnn_train_dataset(
            spark=spark,
            interactions_path=PathConfig.INTERACTIONS_OUT,
            item_nodes_path=PathConfig.ITEM_NODES_OUT,
            output_path=PathConfig.LLM_CHGNN_TRAIN_OUT,
            negatives_per_query=20,
        )

        elapsed = time.time() - t_start

        print(f"""
+----------------------------------------------------------+
|  V  LLM-CHGNN DATASET DONE
|     Rows        : {n_rows:>12,}
|     Time        : {elapsed:>11.1f} s
|
|  Output:
|     {PathConfig.LLM_CHGNN_TRAIN_OUT}
+----------------------------------------------------------+
""")

    except Exception as e:
        logger.error(f"PIPELINE ERROR: {e}", exc_info=True)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()