# Kiến trúc Luồng Dữ liệu RecSys trên Google Cloud Platform (GCP)

Tài liệu này mô tả chi tiết luồng di chuyển dữ liệu từ khi thu thập đến khi hoàn tất huấn luyện mô hình phân tán.

## 1. Tổng quan Kiến trúc
Hệ thống sử dụng **Google Cloud Storage (GCS)** làm Data Lake trung tâm, giúp tách biệt hoàn toàn giữa Tính toán (Compute) và Lưu trữ (Storage).

## 2. Luồng dữ liệu Chi tiết (A -> Z)

### Giai đoạn 1: Ingestion (Thu thập dữ liệu)
*   **Nguồn**: Dữ liệu thô (JSONL) từ Google Drive.
*   **Công cụ**: `download_data.py` chạy trên Coordinating VM.
*   **Luồng đi**:
    1.  Tải từ Drive về ổ cứng tạm trên VM.
    2.  Sử dụng `gsutil -m cp` đẩy lên GCS: `gs://[BUCKET]/raw_data/amazon_gpc/`.
    3.  Xóa dữ liệu tạm trên VM.

### Giai đoạn 2: ETL & Processing (Xử lý dữ liệu lớn)
*   **Công cụ**: PySpark chạy trên **Google Cloud Dataproc**.
*   **Mã nguồn**: Thư mục `spark_processing_gpc/`.
*   **Luồng đi**:
    1.  **Đọc**: Worker nodes đọc song song từ `gs://[BUCKET]/raw_data/amazon_gpc/`.
    2.  **Xử lý**: Chuẩn hóa schema, làm sạch văn bản và Negative Mining.
    3.  **Ghi**: Xuất file Parquet phân tán vào `gs://[BUCKET]/processed_data/evaluation_dataset/`.

### Giai đoạn 3: Distributed Training (Huấn luyện phân tán)
*   **Công cụ**: PyTorch DDP chạy trên cụm **GPU T4 nodes**.
*   **Mã nguồn**: Thư mục `distributed_training/`.
*   **Luồng đi**:
    1.  **Streaming**: Các GPU nodes dùng `pyarrow` hút dữ liệu trực tiếp từ GCS vào RAM/GPU.
    2.  **Sync**: Đồng bộ Gradient qua mạng nội bộ GCP (backend `nccl`).
    3.  **Checkpointing**: Chỉ Node Master (Rank 0) lưu model.

### Giai đoạn 4: Model Archiving (Lưu trữ kết quả)
*   **Luồng đi**:
    1.  Node Master lưu file `.pt` cục bộ.
    2.  Sử dụng script tích hợp đẩy lên GCS: `gs://[BUCKET]/models/sbert_baseline/`.
    3.  Xóa file tạm trên máy GPU.

## 3. Sơ đồ tóm tắt (Data Pipeline)
`Drive` ➔ `Coordinating VM` ➔ **`GCS (Raw)`** ➔ `Dataproc (Spark)` ➔ **`GCS (Processed)`** ➔ `GPU Nodes (RAM)` ➔ **`GCS (Models)`**

---

## 4. Các lưu ý vận hành
*   **Chi phí**: Hãy tắt cụm Dataproc và GPU nodes ngay sau khi hoàn thành công việc để tối ưu chi phí. Dữ liệu trên GCS có chi phí lưu trữ rất rẻ.
*   **Hiệu suất**: Luôn sử dụng đường dẫn `gs://` trực tiếp thay vì Mount ổ đĩa để đạt băng thông cao nhất.
*   **Bảo mật**: Đảm bảo Service Account của máy ảo có quyền `Storage Object Admin` trên Bucket.
