import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv(override=True)

if os.name == 'nt':
    os.environ["HADOOP_HOME"]       = r"D:\Apache_Hadoop"
    os.environ["JAVA_HOME"]         = r"D:\JavaJDK\jdk-11.0.30"
    os.environ["PATH"]              = ( os.environ["HADOOP_HOME"] + r"\bin;" + os.environ.get("PATH", ""))
os.environ["PYSPARK_PYTHON"]    = "python"
os.environ["SPARK_LOCAL_IP"]    = "127.0.0.1"

# ── 2. Hằng số đường dẫn – hỗ trợ override từ biến môi trường ──────────
class PathConfig:
    # BASE_DIR mặc định là root của project local
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    
    # Cho phép ghi đè các đường dẫn bằng biến môi trường (Ví dụ: export RAW_DATA_DIR=gs://bucket/raw)
    RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", os.path.join(BASE_DIR, "data_small"))

    # Thư mục lưu kết quả Parquet sau ETL
    OUTPUT_BASE = os.getenv("OUTPUT_BASE", os.path.join(BASE_DIR, "output"))
    
    INTERACTIONS_OUT = os.path.join(OUTPUT_BASE, "all_interactions").replace("\\", "/")
    ITEM_NODES_OUT   = os.path.join(OUTPUT_BASE, "item_nodes").replace("\\", "/")
    EVALUATION_OUT   = os.path.join(OUTPUT_BASE, "evaluation_dataset").replace("\\", "/")
    LOGS_DIR         = os.path.join(OUTPUT_BASE, "logs").replace("\\", "/")

# ── 3. Tạo SparkSession ──────────────────────────────────────
def create_spark_session(app_name: str = "AmazonETL") -> SparkSession:
    """ Tạo SparkSession linh hoạt cho cả Local và Cloud."""
    builder = SparkSession.builder.appName(app_name)

    # ── Chế độ chạy ──────────────────────────────────────
    # Chỉ đặt master("local[*]") nếu đang chạy local. Trên Cloud (YARN), bỏ qua dòng này.
    if os.getenv("SPARK_ENV") != "cloud":
        builder = builder.master("local[4]")
        builder = builder.config("spark.driver.memory", "4g")
        builder = builder.config("spark.executor.memory", "4g")
    else:
        # Tối ưu cho Cloud (GCS Connector)
        builder = builder.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")

    spark = (
        builder
        # ── Bộ nhớ Off-heap ──────────────────────────────────
        .config("spark.memory.offHeap.enabled",  "true")
        .config("spark.memory.offHeap.size",     "1g")

        # ── Tối ưu shuffle và join ───────────────────────────
        .config("spark.sql.shuffle.partitions",  os.getenv("SHUFFLE_PARTITIONS", "8"))
        .config("spark.default.parallelism",     os.getenv("DEFAULT_PARALLELISM", "8"))

        # ── Đọc JSON Amazon (phân biệt hoa/thường) ───────────
        .config("spark.sql.caseSensitive",       "true")
        
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"[SparkSession] '{app_name}' khoi dong thanh cong!")
    print(f"  Master  : {spark.sparkContext.master}")
    print(f"  App ID  : {spark.sparkContext.applicationId}")
    return spark
