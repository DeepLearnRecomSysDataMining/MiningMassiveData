# Amazon × VN – Big Data ETL Pipeline

## Cấu trúc project
```
root/spark_processing/
├── main.py                  ← Entry point – chạy file này
├── requirements.txt
├── config/
│   └── spark_config.py      ← Cấu hình Spark + đường dẫn
└── src/
    ├── etl_evaluation.py      ← Tạo evaluation dataset (1 Amazon + 99 VN)
    ├── schema_scanner.py    ← Khảo sát schema tất cả file
    ├── etl_interactions.py  ← ETL tương tác (review files)
    ├── etl_item_nodes.py    ← ETL sản phẩm (meta/details)
    └── data_validator.py    ← Kiểm tra chất lượng output
```

---

## Cách dùng nhanh

### 1. Cài thư viện
```bash
pip install pyspark==3.5.0 findspark python-dotenv
```

---

## ⚙️ Thiết lập cấu hình

### 1. File môi trường `.env` (Nếu dùng)
Hoặc bạn có thể chỉnh trực tiếp trong `spark_processing/config/spark_config.py`.

### 2. Cấu hình đường dẫn trong `spark_config.py`
Mở file `spark_processing/config/spark_config.py` và cập nhật các đường dẫn môi trường cho đúng với máy bạn:
```python
os.environ["HADOOP_HOME"]    = r"C:\path\to\your\Hadoop" # Ví dụ: D:\Apache_Hadoop
os.environ["JAVA_HOME"]      = r"C:\path\to\your\Java"   # Ví dụ: D:\JavaJDK\jdk-11.0.30
```
*Lưu ý: Hệ thống đã tự động thiết lập `RAW_DATA_DIR` trỏ vào thư mục `data/` ở gốc dự án.*

### 3. Chuẩn bị dữ liệu
Copy các file `.jsonl` hoặc `.jsonl.gz` vào thư mục `data/` ở gốc dự án:
*   Sản phẩm VN: `sample_metadatas_vn.jsonl`, `dmx_metadatas.jsonl`,...
*   Sản phẩm Amazon: `sample_metadatas_amazon.jsonl`, `sample_metadatas.jsonl`,...
*   Reviews: `sample_reviews.jsonl`,...

---

## 🛠️ Cách vận hành Pipeline

Toàn bộ quy trình từ làm sạch dữ liệu đến tạo bộ Dataset kiểm thử được gói gọn trong file `main.py`.

### Chạy toàn bộ quy trình (Khuyến nghị)
```bash
python spark_processing/main.py
```

### Các tùy chọn nâng cao
*   **Chỉ khảo sát Schema**: Xem cấu trúc dữ liệu thô mà không xử lý.
    ```bash
    python spark_processing/main.py --scan-only
    ```
*   **Bỏ qua bước Scan**: Chạy thẳng vào ETL để tiết kiệm thời gian.
    ```bash
    python spark_processing/main.py --skip-scan
    ```
*   **Kiểm tra chất lượng (Validate)**: Tự động kiểm tra file output sau khi chạy xong.
    ```bash
    python spark_processing/main.py --validate
    ```

---

## 📂 Cấu trúc dữ liệu đầu ra (Output)

Kết quả sau khi chạy sẽ nằm trong thư mục `output/`:

1.  **`output/item_nodes/`**: Danh mục sản phẩm chuẩn hóa (Amazon + VN). 
    *   *Chứa: product_id, asin, category, full_text, parsed_specs, domain.*
2.  **`output/all_interactions/`**: Dữ liệu review người dùng đã chuẩn hóa.
3.  **`output/evaluation_dataset/`**: **BỘ ĐỀ THI CHO AI**. 
    *   Mỗi dòng chứa 1 sản phẩm Amazon và **100 ứng viên** VN (gồm 1 đúng + 99 sai).
    *   Đây là dữ liệu nạp cho 5 baseline (BM25, SBERT, DSSM, GCN, Hybrid).

---

## 💡 Lưu ý về Hiệu năng
Hệ thống mặc định chạy ở chế độ `local[*]` (sử dụng tất cả nhân CPU hiện có). 
Nếu máy bị lag, bạn có thể giảm RAM tại `spark_config.py`:
*   `spark.driver.memory`: "4g"
*   `spark.executor.memory`: "2g"
