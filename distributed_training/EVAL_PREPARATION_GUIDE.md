# Hướng dẫn chuẩn bị dữ liệu Evaluation (.pkl)

Tài liệu này hướng dẫn cách thiết lập và chạy script `src/prepare_eval_pkl.py` để tạo bộ dữ liệu đánh giá cho các mô hình training.

## 1. Yêu cầu hệ thống & Thư viện
Script yêu cầu Python 3.8+ và các thư viện xử lý dữ liệu lớn trên Cloud.

Cài đặt bằng lệnh:
```bash
pip install pandas pyarrow gcsfs
```

*   **pyarrow**: Engine xử lý định dạng Parquet tốc độ cao.
*   **gcsfs**: Cho phép Pandas đọc/ghi trực tiếp trên Google Cloud Storage.

### Tạo hẳn 1 VM 32Gb RAM nhé, xong thì xóa đi là được.

- Instance Type: e2-standard-8 (8 vCPU, 32GB RAM).
- Tại sao: Dòng e2 có chi phí rất rẻ (~0.26 USD/giờ). 8 vCPU sẽ giúp thư viện pyarrow giải nén file Parquet 35GB nhanh hơn gấp nhiều lần.
- Boot Disk: 50GB - 100GB (Standard Persistent Disk).
- Tại sao: Bạn cần đủ không gian để lưu thư mục /tmp/training_data khi tải file về.
- Quyền hạn (Scopes): Chọn Allow full access to all Cloud APIs.
- Tại sao: Để máy ảo có quyền dùng lệnh gsutil tải và upload dữ liệu lên GCS mà không cần cấu hình file JSON key thủ công.

## 2. Cấu hình Môi trường
Script tự động nhận diện môi trường dựa trên biến `TRAINING_ENV`.

### Chế độ Cloud (GCP VM / Vertex AI)
Thiết lập biến môi trường để script sử dụng đường dẫn GCS và tự động upload kết quả:
```bash
export TRAINING_ENV=cloud
```
*Dữ liệu sẽ được lưu tạm tại `/tmp/training_data/` để tối ưu tốc độ.*

### Chế độ Local
Không cần cấu hình, script sẽ mặc định tìm dữ liệu trong thư mục `data/` tại local.

## 3. Kiểm tra Dữ liệu đầu vào
Đảm bảo các đường dẫn sau đã có dữ liệu (kết quả từ Phase Spark):
- `gs://mining-data-2/output/evaluation_dataset/` (Dữ liệu ID-Only)
- `gs://mining-data-2/output/item_nodes/` (Metadata sản phẩm)

## 4. Thực thi
Chạy script từ thư mục gốc của `distributed_training`:
```bash
python src/prepare_eval_pkl.py
```

## 5. Quy trình hoạt động (Logic)
1.  **Bước 1**: Đọc tập ID từ file Eval Parquet để xác định danh sách sản phẩm cần lấy metadata.
2.  **Bước 2**: Đọc file `item_nodes` có lọc (Filter) theo danh sách ID và chọn cột (Column Selection) để tiết kiệm RAM.
3.  **Bước 3**: Xây dựng Lookup Dictionary trong bộ nhớ.
4.  **Bước 4**: Ánh xạ ID sang Text Metadata (Title, Category).
5.  **Bước 5**: Lưu kết quả ra file `.pkl`.
6.  **Bước 6**: (Chỉ Cloud) Upload file lên GCS tại `gs://mining-data-2/output/prepared_data_improved/evaluation_dataset.pkl`.

## 6. Xử lý sự cố
- **Tràn RAM**: Script đã được tối ưu cho dữ liệu 25GB+, nếu vẫn tràn RAM, hãy kiểm tra xem có đang load thừa cột nào không.
- **Lỗi GCS**: Đảm bảo máy ảo đã được cấp quyền `Storage Object Admin` hoặc đã chạy `gcloud auth application-default login`.
