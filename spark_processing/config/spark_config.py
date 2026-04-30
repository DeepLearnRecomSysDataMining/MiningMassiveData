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

# ── 2. Hằng số đường dẫn – chỉnh sửa theo máy bạn ──────────
class PathConfig:
    # Thư mục chứa toàn bộ raw data_small Amazon và VN
    # SỬA: Lấy đường dẫn tuyệt đối của project (root directory)
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    RAW_DATA_DIR = os.path.join(BASE_DIR, "data_small")

    # Thư mục lưu kết quả Parquet sau ETL
    OUTPUT_BASE = os.path.join(BASE_DIR, "output")
    INTERACTIONS_OUT = os.path.join(OUTPUT_BASE, "all_interactions")
    ITEM_NODES_OUT = os.path.join(OUTPUT_BASE, "item_nodes")
    EVALUATION_OUT = os.path.join(OUTPUT_BASE, "evaluation_dataset")
    LOGS_DIR = os.path.join(OUTPUT_BASE, "logs")

# ── 3. Tạo SparkSession ──────────────────────────────────────
def create_spark_session(app_name: str = "AmazonETL") -> SparkSession:
    """ Tạo SparkSession tối ưu cho máy 16GB RAM."""
    spark = (
        SparkSession.builder.appName(app_name)

        # ── Chế độ chạy ──────────────────────────────────────
        # Giới hạn 8 core thay vì 16 để tránh quá tải RAM (mỗi worker tốn RAM)
        .master("local[4]")

        # ── Bộ nhớ ───────────────────────────────────────────
        # Tăng RAM để xử lý 17 triệu dòng dữ liệu
        .config("spark.driver.memory",           "4g")
        .config("spark.executor.memory",         "4g")
        .config("spark.memory.offHeap.enabled",  "true")
        .config("spark.memory.offHeap.size",     "1g")

        # ── Tối ưu shuffle và join ───────────────────────────
        .config("spark.sql.shuffle.partitions",  "8")
        .config("spark.default.parallelism",     "8")

        # ── Đọc JSON Amazon (phân biệt hoa/thường) ───────────
        .config("spark.sql.caseSensitive",       "true")

        # ── Tắt UI nếu không cần (tiết kiệm RAM) ─────────────
        # .config("spark.ui.enabled", "false") 

        # ── Ghi đè session cũ nếu còn sót ────────────────────
        .config("spark.driver.allowMultipleContexts", "true")

        .getOrCreate()
    )

    # Tắt log INFO rác (chỉ hiện WARNING trở lên)
    spark.sparkContext.setLogLevel("WARN")

    print(f"[SparkSession] '{app_name}' khoi dong thanh cong!")
    print(f"  Master  : {spark.sparkContext.master}")
    print(f"  App ID  : {spark.sparkContext.applicationId}")
    print(f"  UI URL  : {spark.sparkContext.uiWebUrl}")
    return spark
