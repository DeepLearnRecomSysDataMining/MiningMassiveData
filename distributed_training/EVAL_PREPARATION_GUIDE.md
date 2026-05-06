# Hướng dẫn Chuẩn bị Dữ liệu Đánh giá (.pkl)

Tài liệu này hướng dẫn cách thiết lập và chạy script `src/prepare_eval_pkl.py` để tạo bộ dữ liệu đánh giá chuẩn cho các mô hình training.

## 1. Yêu cầu Hệ thống & VM đề xuất
- **Instance Type**: `e2-standard-4` (4 vCPU, 16GB RAM).
- **Tại sao**: Code V2 đã tối ưu RAM (chỉ dùng ~4-6GB), giúp tiết kiệm chi phí so với máy 32GB trước đây.
- **Boot Disk**: 50GB.
- **Quyền hạn (Scopes)**: Chọn `Allow full access to all Cloud APIs`.

## 2. Khởi tạo Môi trường & Cài đặt Thư viện
Sau khi SSH vào máy ảo, hãy thực hiện các lệnh sau để tạo môi trường sạch:

```bash
# 1. Di chuyển vào thư mục dự án
cd ~/MiningMassiveData

# 2. Tạo môi trường ảo (Virtual Environment)
python3 -m venv eval_env

# 3. Kích hoạt môi trường ảo
source eval_env/bin/activate

# 4. Cài đặt các thư viện xử lý dữ liệu lớn
pip install --upgrade pip
pip install pandas pyarrow gcsfs
```

- **pyarrow**: Xử lý định dạng Parquet (item_nodes) tốc độ cao.
- **gcsfs**: Cho phép Pandas đọc dữ liệu trực tiếp từ GCS.

## 3. Cấu hình & Chạy Script
Thiết lập biến môi trường để script biết cần đọc dữ liệu từ GCS:

```bash
# 1. Thiết lập biến môi trường (Chấp nhận cả SPARK_ENV hoặc TRAINING_ENV)
export SPARK_ENV=cloud
export TRAINING_ENV=cloud

# 2. Chạy script đóng gói dữ liệu
# Lưu ý: Chạy dưới dạng module (-m) để tránh lỗi import
python3 -m distributed_training.src.prepare_eval_pkl
```

### Kết quả mong đợi:
- Script sẽ đọc `gs://mining-data-2/output/evaluation_dataset` (Parquet) và `gs://mining-data-2/output/item_nodes` (Parquet).
- Tạo file `.pkl` tại: `distributed_training/data/prepared_data_improved/evaluation_dataset.pkl`.
- Tự động upload kết quả lên: `gs://mining-data-2/output/prepared_data_improved/evaluation_dataset.pkl`.

## 4. Kiểm tra
Nếu bảng thống kê hiện ra con số **4,082,820 products** và số lượng Queries tương ứng, bạn đã thành công!
- `gs://mining-data-2/output/evaluation_dataset/` (Dữ liệu ID-Only)
- `gs://mining-data-2/output/item_nodes/` (Metadata sản phẩm)

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
