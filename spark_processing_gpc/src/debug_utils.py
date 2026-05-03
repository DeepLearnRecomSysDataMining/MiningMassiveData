import logging
from pyspark.sql import DataFrame

logger = logging.getLogger("spark_debug")

def log_spark_configs(spark):
    """In toàn bộ các tham số cấu hình của phiên Spark hiện tại."""
    logger.info("="*60)
    logger.info("🛠️ DUMPING ALL SPARK CONFIGURATIONS 🛠️")
    logger.info("="*60)
    configs = spark.sparkContext.getConf().getAll()
    for key, value in sorted(configs):
        logger.info(f"  {key} = {value}")
    logger.info("="*60)

def log_df_size(df: DataFrame, df_name: str = "DataFrame"):
    """
    In ra số dòng và dung lượng bộ nhớ ước tính của DataFrame.
    Lưu ý: Việc đếm dòng sẽ kích hoạt Spark Action.
    """
    try:
        # Lấy dung lượng vật lý ước tính từ Catalyst Execution Plan
        size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
        size_mb = size_in_bytes / (1024 * 1024)
        size_str = f"{size_mb:.2f} MB"
    except Exception as e:
        size_str = f"Không tính được ({e})"

    # Đếm số dòng thực tế
    row_count = df.count()
    
    logger.info("-" * 60)
    logger.info(f"📊 [DEBUG] {df_name}")
    logger.info(f"   + Số dòng: {row_count:,}")
    logger.info(f"   + Dung lượng Logical ước tính: {size_str}")
    logger.info("-" * 60)