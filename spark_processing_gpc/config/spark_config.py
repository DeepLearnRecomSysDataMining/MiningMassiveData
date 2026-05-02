import os
from pathlib import Path
from pyspark.sql import SparkSession

# ── 2. Hằng số đường dẫn – hỗ trợ override từ biến môi trường ──────────
class PathConfig:
    # BASE_DIR chỉ dùng để xác định đường dẫn local dự phòng
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    
    # Ưu tiên lấy từ biến môi trường (GCP sẽ truyền gs://bucket/...)
    RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", os.path.join(BASE_DIR, "data_small")).replace("\\", "/")
    OUTPUT_BASE  = os.getenv("OUTPUT_BASE", os.path.join(BASE_DIR, "output")).replace("\\", "/")
    
    # Tạo đường dẫn con bằng cách cộng chuỗi để tránh os.path.join dùng sai dấu gạch chéo trên Windows
    def _join_gcs(base, sub):
        return f"{base.rstrip('/')}/{sub}"

    INTERACTIONS_OUT = _join_gcs(OUTPUT_BASE, "all_interactions")
    ITEM_NODES_OUT   = _join_gcs(OUTPUT_BASE, "item_nodes")
    EVALUATION_OUT   = _join_gcs(OUTPUT_BASE, "evaluation_dataset")
    # Trên Cloud, logs nên in ra stdout, không cần lưu file cục bộ
    LOGS_DIR         = _join_gcs(OUTPUT_BASE, "logs")

# ── 3. Tạo SparkSession ──────────────────────────────────────
def create_spark_session(app_name: str = "AmazonETL") -> SparkSession:
    """ Tạo SparkSession linh hoạt cho cả Local và Cloud."""
    builder = SparkSession.builder.appName(app_name)

    # ── Chế độ chạy ──────────────────────────────────────
    env = os.getenv("SPARK_ENV", "local").lower()
    print(f"[DEBUG] SPARK_ENV hien tai: {env}")
    print(f"[DEBUG] RAW_DATA_DIR: {PathConfig.RAW_DATA_DIR}")

    if env == "cloud":
        # Tren Dataproc, Spark tu dong ket noi YARN, khong can set master
        builder = builder.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        
        # Cấu hình tài nguyên cho n4-standard-2 (8GB RAM / 2 vCPU)
        # Để lại ~2-3GB cho OS và overhead
        builder = builder.config("spark.executor.memory", os.getenv("EXECUTOR_MEMORY", "5g"))
        builder = builder.config("spark.executor.cores",  os.getenv("EXECUTOR_CORES", "2"))
        builder = builder.config("spark.driver.memory",   os.getenv("DRIVER_MEMORY", "2g"))

    spark = (
        builder
        # ── Bộ nhớ Off-heap ──────────────────────────────────
        .config("spark.memory.offHeap.enabled",  "true")
        .config("spark.memory.offHeap.size",     "1g")

        # ── Tối ưu shuffle và join ───────────────────────────
        # Với 4 nodes x 2 cores = 8 slots. Partition nên là 16 hoặc 32.
        .config("spark.sql.shuffle.partitions",  os.getenv("SHUFFLE_PARTITIONS", "16"))
        .config("spark.default.parallelism",     os.getenv("DEFAULT_PARALLELISM", "16"))

        # ── Xử lý lỗi cho Spot VM (Secondary Workers) ───────
        .config("spark.task.maxFailures",        "8") 
        .config("spark.speculation",            "true")

        # ── Đọc JSON Amazon (phân biệt hoa/thường) ───────────
        .config("spark.sql.caseSensitive",       "true")
        
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"[SparkSession] '{app_name}' khoi dong thanh cong!")
    print(f"  Master  : {spark.sparkContext.master}")
    print(f"  App ID  : {spark.sparkContext.applicationId}")
    return spark
