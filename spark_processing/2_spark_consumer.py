import os
from dotenv import load_dotenv
import findspark

# 1. Ép dotenv ghi đè mọi biến cũ (thêm override=True)
load_dotenv(override=True)

# 2. GÁN CỨNG ĐƯỜNG DẪN BẰNG LỆNH PYTHON (Ghi đè 100% Windows)
# Chữ 'r' ở trước chuỗi giúp Python không bị lỗi gạch chéo
os.environ["HADOOP_HOME"] = r"D:\Apache_Hadoop"
# Đảm bảo đường dẫn này cũng đúng với máy bạn nhé
os.environ["JAVA_HOME"] = r"D:\JavaJDK\jdk-11.0.30"
os.environ["PYSPARK_PYTHON"] = "python"
# THÊM DÒNG NÀY: Ép Python nhét thư mục bin vào PATH để chắc chắn 100% thấy hadoop.dll
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ.get("PATH", "")

# 3. Khởi tạo Spark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

print(f"Đang dùng HADOOP_HOME: {os.environ.get('HADOOP_HOME')}")
print(f"Đang dùng JAVA_HOME: {os.environ.get('JAVA_HOME')}")

# 2. Khởi tạo PySpark (Lưu ý: Nó sẽ tự động tải thư viện kết nối Kafka về máy ở lần chạy đầu)
spark = SparkSession.builder \
    .appName("TestKafkaSpark") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Giảm log rác của Java
spark.sparkContext.setLogLevel("WARN")

print("⏳ Đang kết nối tới Kafka để lắng nghe dữ liệu...")

# 3. Đọc dữ liệu dạng luồng (Streaming) từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test_bigdata_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Ép kiểu dữ liệu (Vì data từ Kafka luôn là dạng nhị phân)
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("rating", IntegerType(), True)
])

df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select("data.*")

# 3. THÊM ĐOẠN NÀY: Ép Spark lưu Checkpoint ra ổ D, tránh xa ổ C của Windows
checkpoint_dir = r"D:\Apache_spark_checkpoint"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)

# 5. In thẳng kết quả ra màn hình Console của PyCharm để nghiệm thu
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

query.awaitTermination()