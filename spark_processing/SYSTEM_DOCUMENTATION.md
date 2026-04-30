# Amazon × VN: Big Data Spark ETL System Documentation

## 1. Tổng quan hệ thống
Hệ thống `spark_processing` là giải pháp thay thế hoàn toàn cho các script Colab (`MMD_v2`, `MMD_v3`), được thiết kế để xử lý dữ liệu thương mại điện tử quy mô lớn sử dụng **Apache Spark**. Hệ thống thực hiện việc chuẩn hóa dữ liệu từ nhiều nguồn (Amazon & Việt Nam), làm sạch văn bản, trích xuất thông số kỹ thuật (Specs) và tạo bộ dữ liệu đánh giá cho các mô hình AI.

### Mục tiêu chính:
*   **Scalability**: Xử lý hàng triệu bản ghi vượt qua giới hạn bộ nhớ của Pandas/Colab.
*   **Standardization**: Đưa dữ liệu Amazon (`.jsonl.gz`) và VN (`.jsonl`) về một Schema đồng nhất.
*   **Negative Mining**: Tự động tạo bộ dữ liệu kiểm thử (1 True + 99 Negatives) phục vụ 5 baseline model.

---

## 2. Kiến trúc Pipeline (4 Giai đoạn)

### Giai đoạn 0: Schema Scanner (`src/schema_scanner.py`)
Khảo sát cấu trúc của tất cả các file JSONL trong thư mục dữ liệu. Spark sẽ lấy mẫu (sampling) để xác định các trường dữ liệu hiện có, giúp tránh lỗi "Missing Column" ở các giai đoạn sau.

### Giai đoạn 1: ETL Interactions (`src/etl_interactions.py`)
Xử lý dữ liệu Review/Tương tác người dùng.
*   **Đầu vào**: Các file review thô.
*   **Xử lý**: Lọc bỏ trùng lặp (`user_id`, `product_id`), chuẩn hóa Rating, gắn nhãn Domain.
*   **Đầu ra**: `output/all_interactions/` (Parquet).

### Giai đoạn 2: ETL Item Nodes (`src/etl_item_nodes.py`)
Xử lý dữ liệu Metadata sản phẩm (Đây là phần quan trọng nhất).
*   **Clean Text UDF**: Chuyển đổi các trường dạng List/String phức tạp về văn bản thuần.
*   **Parse Specs UDF**: Sử dụng Logic từ Colab v3 để tách các cặp thuộc tính (Key::Value).
*   **Category UDF**: Tự động phân loại sản phẩm dựa trên từ khóa (Laptop, Smartphone, Television, ...).
*   **Đầu ra**: `output/item_nodes/` (Parquet).

### Giai đoạn 3: Evaluation Generator (`src/evaluation_dataset.py`)
Tạo bộ dữ liệu "đề thi" cho AI.
*   **Matching**: Khớp cặp Amazon-VN thông qua mã ASIN.
*   **Negative Sampling**: Với mỗi cặp đúng, hệ thống lấy 70% Hard Negatives (cùng loại) và 30% Easy Negatives (khác loại).
*   **Đầu ra**: `output/evaluation_dataset/` (Parquet - Sẵn sàng cho Distributed Training).

---

## 3. Hướng dẫn Cấu hình (`config/spark_config.py`)

Hệ thống cho phép tinh chỉnh linh hoạt tùy theo tài nguyên máy:

| Tham số | Ý nghĩa | Gợi ý (16GB RAM) |
| :--- | :--- | :--- |
| `.master("local[*]")` | Sử dụng bao nhiêu nhân CPU | `local[4]` hoặc `local[8]` |
| `spark.driver.memory` | Bộ nhớ cho tiến trình điều phối | `2g` đến `4g` |
| `spark.executor.memory` | Bộ nhớ cho mỗi luồng tính toán | `1g` đến `2g` |
| `spark.sql.shuffle.partitions` | Số lượng phân vùng khi Join | `8` đến `16` |

---

## 4. Hướng dẫn Chạy Hệ thống

Mở Terminal tại thư mục gốc dự án và sử dụng các lệnh sau:

### A. Chạy mặc định (Toàn bộ Pipeline)
```bash
python spark_processing/main.py
```
### A.2. Sau lần chạy mặc định , đã giải nén + scan rồi thì lần sau chạy
> Lệnh này sẽ giải nén file .gz trước, sau đó chạy ETL với cấu hình RAM mới và code đã tối ưu
```bash
python spark_processing/main.py --decompress --skip-scan
```
### B. Chạy kiểm tra dữ liệu mới (Scan only)
Dùng khi bạn mới tải thêm dữ liệu và muốn xem cấu trúc trước khi chạy thật.
```bash
python spark_processing/main.py --scan-only
```

### C. Chạy tăng tốc (Skip Scan)
Nếu bạn đã chạy một lần và dữ liệu không thay đổi cấu trúc, hãy bỏ qua bước scan để vào thẳng ETL.
```bash
python spark_processing/main.py --skip-scan
```

### D. Chạy kèm Validate (Kiểm tra chất lượng)
Sau khi lưu file Parquet, hệ thống sẽ tự động đọc lại để báo cáo số lượng và kiểm tra lỗi Null.
```bash
python spark_processing/main.py --validate
```

---

## 5. Cấu trúc Dữ liệu Đầu ra (Output Schema)

### 1. `item_nodes.parquet`
*   `product_id`: ID duy nhất (ASIN hoặc ID VN).
*   `product_name`: Tên sản phẩm đã làm sạch.
*   `category`: Phân loại (Laptop, Smartphone, ...).
*   `full_text`: Văn bản tổng hợp để làm Embedding.
*   `parsed_specs`: Map chứa các thông số kỹ thuật (Dùng cho Baseline 5 - Hybrid).

### 2. `evaluation_dataset.parquet`
*   `query_text`: Văn bản sản phẩm Amazon (Truy vấn).
*   `true_vn_id`: ID sản phẩm VN tương ứng (Ground Truth).
*   `candidate_ids`: Danh sách 100 ID ứng viên (1 Đúng + 99 Sai).
*   `candidate_texts`: Danh sách 100 đoạn văn bản của ứng viên (Dùng cho SBERT/DSSM).

---

## 6. Lưu ý khi chạy trên Google Cloud (GCP)
1.  **Hệ điều hành**: Đảm bảo comment/xóa các đường dẫn Windows (`D:\...`) trong `spark_config.py`.
2.  **Shared Drive**: Khuyên dùng NFS để cả cụm máy cùng đọc chung thư mục `output`.
3.  **Environment**: Cài đặt `pyspark`, `pandas`, `pyarrow` trên tất cả các máy.
