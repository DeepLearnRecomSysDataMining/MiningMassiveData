# ============================================================
# config/spark_config.py
# Cấu hình SparkSession cho Local Cluster (nhiều core)
# hoặc kết nối Spark Standalone / YARN nếu có cluster thật
# ============================================================

import os
from dotenv import load_dotenv

# ── 1. Load .env và ghi đè biến môi trường ──────────────────
load_dotenv(override=True)

os.environ["HADOOP_HOME"]    = r"D:\Apache_Hadoop"
os.environ["JAVA_HOME"]      = r"D:\JavaJDK\jdk-11.0.30"
os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PATH"]           = (
    os.environ["HADOOP_HOME"] + r"\bin;" + os.environ.get("PATH", "")
)

from pyspark.sql import SparkSession

"""
# Cấu hình tại hàm create_spark_session
.config("spark.driver.memory",          "6g")   # Dành 6GB cho Driver xử lý logic chính
.config("spark.executor.memory",         "4g")   # Dành 4GB cho việc tính toán song song
.config("spark.executor.memoryOverhead", "1g")   # RAM dự phòng để tránh lỗi OOM (Out of Memory)
.config("spark.sql.shuffle.partitions",  "16")   # Khớp với số luồng của CPU i5-1240P
"""

# ── 2. Hằng số đường dẫn – chỉnh sửa theo máy bạn ──────────
class PathConfig:
    # Thư mục chứa toàn bộ raw data Amazon và VN
    # SỬA: Lấy đường dẫn tuyệt đối của project
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DATA_DIR = os.path.join(BASE_DIR, "data")

    # Thư mục lưu kết quả Parquet sau ETL
    OUTPUT_BASE = os.path.join(BASE_DIR, "output")

    INTERACTIONS_OUT = os.path.join(OUTPUT_BASE, "all_interactions")
    ITEM_NODES_OUT = os.path.join(OUTPUT_BASE, "item_nodes")
    EVALUATION_OUT = os.path.join(OUTPUT_BASE, "evaluation_dataset")
    LOGS_DIR = os.path.join(OUTPUT_BASE, "logs")


# ── 3. Tạo SparkSession ──────────────────────────────────────
def create_spark_session(app_name: str = "AmazonETL") -> SparkSession:
    """
    Tạo SparkSession tối ưu cho máy 16GB RAM.

    Chế độ mặc định: local[*]  → dùng tất cả CPU core trên 1 máy.

    Nếu bạn có Spark Standalone Cluster (nhiều máy), đổi master thành:
        .master("spark://<IP_MÁY_MASTER>:7077")
    Nếu dùng YARN:
        .master("yarn")
    """
    spark = (
        SparkSession.builder
        .appName(app_name)

        # ── Chế độ chạy ──────────────────────────────────────
        # local[*]  = dùng tất cả CPU core, 1 máy
        # local[4]  = dùng đúng 4 core
        .master("local[*]")

        # ── Bộ nhớ ───────────────────────────────────────────
        # Máy 16GB: dành 6g cho Driver, 4g cho mỗi Executor
        .config("spark.driver.memory",          "6g")
        .config("spark.executor.memory",         "4g")
        .config("spark.executor.memoryOverhead", "1g")

        # ── Tối ưu shuffle và join ───────────────────────────
        .config("spark.sql.shuffle.partitions",  "8")   # ~2x số core, điều chỉnh tùy máy
        .config("spark.default.parallelism",     "8")

        # ── Đọc JSON Amazon (phân biệt hoa/thường) ───────────
        .config("spark.sql.caseSensitive",       "true")

        # ── Tăng tốc đọc file nén .gz song song ──────────────
        .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", str(128 * 1024 * 1024))  # 128MB/partition

        # ── Tắt UI nếu không cần (tiết kiệm RAM) ─────────────
        # .config("spark.ui.enabled", "false")  # Bỏ comment nếu muốn tắt

        # ── Ghi đè session cũ nếu còn sót ────────────────────
        .config("spark.driver.allowMultipleContexts", "true")

        .getOrCreate()
    )

    # Tắt log INFO rác (chỉ hiện WARNING trở lên)
    spark.sparkContext.setLogLevel("WARN")

    print(f"[SparkSession] '{app_name}' khởi động thành công!")
    print(f"  Master  : {spark.sparkContext.master}")
    print(f"  App ID  : {spark.sparkContext.applicationId}")
    print(f"  UI URL  : {spark.sparkContext.uiWebUrl}")
    return spark
