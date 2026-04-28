# Amazon × VN – Big Data ETL Pipeline

## Cấu trúc project
```
amazon_etl_project/
├── main.py                  ← Entry point – chạy file này
├── requirements.txt
├── config/
│   └── spark_config.py      ← Cấu hình Spark + đường dẫn
└── src/
    ├── schema_scanner.py    ← Khảo sát schema tất cả file
    ├── etl_interactions.py  ← ETL tương tác (review files)
    ├── etl_item_nodes.py    ← ETL sản phẩm (meta/details)
    └── data_validator.py    ← Kiểm tra chất lượng output
```

---

## Cách dùng nhanh

### 1. Cài thư viện
```bash
pip install -r requirements.txt
```

### 2. Chỉnh đường dẫn trong `config/spark_config.py`
```python
# Sửa 3 đường dẫn này cho đúng máy bạn:
os.environ["HADOOP_HOME"] = r"D:\Apache_Hadoop"
os.environ["JAVA_HOME"]   = r"D:\JavaJDK\jdk-11.0.30"

class PathConfig:
    RAW_DATA_DIR = r"D:\Data\amazon"   # ← thư mục chứa file .jsonl.gz
```

### 3. Chạy pipeline

```bash
# Khảo sát schema trước (nên chạy lần đầu)
python main.py --scan-only

# Chạy toàn bộ ETL
python main.py

# Chạy ETL + validate output
python main.py --validate

# Bỏ qua scan, chạy ETL thẳng
python main.py --skip-scan

# Đổi thư mục data
python main.py --data-dir "E:\my_data\amazon"
```

---

## Cơ chế xử lý song song

```
Spark local[*]  →  dùng TẤT CẢ CPU core trên máy bạn
                   Mỗi file .gz được chia thành ~128MB partition
                   Mỗi partition xử lý đồng thời trên 1 core
```

### Muốn chạy Distributed thật (nhiều máy)?

Trong `config/spark_config.py`, đổi dòng `.master(...)`:

```python
# Spark Standalone Cluster
.master("spark://192.168.1.100:7077")

# YARN (Hadoop cluster)
.master("yarn")
```

---

## Output

| Thư mục | Nội dung |
|---|---|
| `amazon_processed/all_interactions/` | Parquet tương tác (user_id, item_id, rating, timestamp) |
| `amazon_processed/item_nodes/` | Parquet sản phẩm phân vùng theo domain |
| `amazon_processed/logs/` | Log file của pipeline |

---

## Điều chỉnh hiệu năng (máy 16GB RAM)

Trong `config/spark_config.py`:
```python
.config("spark.driver.memory",         "6g")   # Tăng nếu còn RAM
.config("spark.executor.memory",        "4g")
.config("spark.sql.shuffle.partitions", "8")    # = 2x số CPU core
```
