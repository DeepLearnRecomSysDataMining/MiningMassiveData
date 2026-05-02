import os
from pathlib import Path
from pyspark.sql import SparkSession

# ── 1. Cấu hình đường dẫn Cloud-Only ───────────────────────────
class PathConfig:
    @staticmethod
    def _get_env_or_default(key, default):
        return os.getenv(key, default).replace("\\", "/")

    @property
    def RAW_DATA_DIR(self):
        return self._get_env_or_default("RAW_DATA_DIR", "gs://mining-data-2/raw_data/amazon_gpc/")

    @property
    def OUTPUT_BASE(self):
        return self._get_env_or_default("OUTPUT_BASE", "gs://mining-data-2/output/")

    @staticmethod
    def _join_gcs(base, sub):
        return f"{base.rstrip('/')}/{sub}"

    @property
    def INTERACTIONS_OUT(self):
        return self._join_gcs(self.OUTPUT_BASE, "all_interactions")
    
    @property
    def ITEM_NODES_OUT(self):
        return self._join_gcs(self.OUTPUT_BASE, "item_nodes")
    
    @property
    def EVALUATION_OUT(self):
        return self._join_gcs(self.OUTPUT_BASE, "evaluation_dataset")

    @property
    def LOGS_DIR(self):
        return self._join_gcs(self.OUTPUT_BASE, "logs")

# Khởi tạo instance
PathConfig = PathConfig()

# ── 2. Tạo SparkSession tối ưu cho Dataproc ────────────────────
def create_spark_session(app_name: str = "AmazonETL_Cloud") -> SparkSession:
    """Tạo SparkSession mặc định chạy trên cụm Dataproc (YARN)."""
    
    builder = (SparkSession.builder
        .appName(app_name)
        # Tối ưu cho GCS Connector (Bắt buộc để đọc gs://)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        
        # Cấu hình tài nguyên cho cụm n4-standard-2 (8GB RAM)
        .config("spark.executor.memory", os.getenv("EXECUTOR_MEMORY", "4g"))
        .config("spark.executor.cores",  os.getenv("EXECUTOR_CORES", "2"))
        .config("spark.driver.memory",   os.getenv("DRIVER_MEMORY", "2g"))
        
        # Bộ nhớ Off-heap và Tối ưu Shuffle
        .config("spark.memory.offHeap.enabled",  "true")
        .config("spark.memory.offHeap.size",     "512m")
        .config("spark.sql.shuffle.partitions",  os.getenv("SHUFFLE_PARTITIONS", "16"))
        .config("spark.default.parallelism",     os.getenv("DEFAULT_PARALLELISM", "16"))

        # Xử lý lỗi cho Spot VM (Secondary Workers)
        .config("spark.task.maxFailures",        "8") 
        .config("spark.speculation",            "true")
        
        # Đọc JSON Amazon (phân biệt hoa/thường)
        .config("spark.sql.caseSensitive",       "true")
    )

    return builder.getOrCreate()
