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

def create_spark_session(app_name: str = "AmazonETL_Cloud") -> SparkSession:
    """Tạo SparkSession tối ưu khống chế RAM và ép dùng Local Disk."""
    
    builder = (SparkSession.builder
        .appName(app_name)
        # Tối ưu cho GCS Connector (Bắt buộc để đọc gs://)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        
        # 1. FIX LỖI 143: Khống chế RAM nghiêm ngặt cho Node 16GB
        .config("spark.executor.memory", "7g")           # RAM cho JVM
        .config("spark.executor.memoryOverhead", "3g")   # RAM cho Python và ngoài JVM (Cực kỳ quan trọng)
        .config("spark.driver.memory", "7g")
        .config("spark.driver.memoryOverhead", "3g")

        # 2. FIX LỖI TỐN TIỀN GCS: Ép Spark ghi dữ liệu tạm (Shuffle) xuống ổ đĩa
        .config("spark.local.dir", "/tmp/spark-local")

        # Cấu hình cũ của bạn
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "1g")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "200"))
        .config("spark.default.parallelism", os.getenv("DEFAULT_PARALLELISM", "200"))
        .config("spark.task.maxFailures", "8") 
        .config("spark.speculation", "true")
        .config("spark.sql.caseSensitive", "true")
    )

    return builder.getOrCreate()
