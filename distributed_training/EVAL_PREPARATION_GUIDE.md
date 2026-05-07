# Hướng dẫn Chuẩn bị Dữ liệu Đánh giá (.pkl)

Tài liệu này hướng dẫn cách thiết lập và chạy script `src/prepare_eval_pkl.py` để tạo bộ dữ liệu đánh giá chuẩn cho các mô hình training.

## 1. Yêu cầu Hệ thống & VM đề xuất
- **Instance Type**: `e2-standard-4` (4 vCPU, 16GB RAM).
- **Tại sao**: Code V2 đã tối ưu RAM (chỉ dùng ~4-6GB), giúp tiết kiệm chi phí so với máy 32GB trước đây.
- **Boot Disk**: 50GB.
- **Quyền hạn (Scopes)**: Chọn `Allow full access to all Cloud APIs`.

### Nếu chọn VM với GPU:
- **VM type**: `n1-standard-4 4 vCPUs, 15 GB RAM with NVIDIA Tesla T4 GPU`
- **Boot Disk**: 50GB.
- **Quyền hạn (Scopes)**: Chọn `Allow full access to all Cloud APIs`.
- **giảm giá hơn**: kéo xuồng dưới tìm chọn `provision Model` đổi standard thành Spot

## 2. Khởi tạo Môi trường & Cài đặt Thư viện
Sau khi SSH vào máy ảo mới, hãy thực hiện các lệnh sau theo đúng thứ tự:

### Bước 2.1: Cấu hình Google Cloud Project
```bash
# Xóa cấu hình cũ để tránh lỗi quyền hạn
rm -rf ~/.config/gcloud
# Thiết lập Project ID thực tế của bạn
gcloud config set project mining-data-2
```

### Bước 2.2: Cài đặt công cụ hệ thống (Bắt buộc dùng sudo)
Máy ảo mới thường thiếu gói tạo môi trường ảo, bạn cần cài đặt nó trước:
```bash
# Cập nhật hệ thống
sudo apt update
# Cài đặt gói python3-venv
sudo apt install -y python3-venv
```

### Bước 2.3: Tạo & Kích hoạt Môi trường ảo
```bash
# 1. Di chuyển vào thư mục dự án
cd ~/MiningMassiveData

# 2. Tạo môi trường ảo
python3 -m venv eval_env

# 3. Kích hoạt môi trường ảo
source eval_env/bin/activate
```

### Bước 2.4: Cài đặt các thư viện Python
```bash
# Nâng cấp pip
pip install --upgrade pip

# CHỌN MỘT TRONG HAI LỆNH SAU:

# LỰA CHỌN A: Dành cho máy ảo CPU (Tiết kiệm, chỉ dùng đóng gói dữ liệu)
pip install pandas pyarrow gcsfs torch --index-url https://download.pytorch.org/whl/cpu

# LỰA CHỌN B: Dành cho máy ảo GPU (Dùng để Train model trực tiếp)
pip install pandas pyarrow gcsfs torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121
```

- **pyarrow**: Xử lý định dạng Parquet (item_nodes) tốc độ cao.
- **gcsfs**: Cho phép Pandas đọc dữ liệu trực tiếp từ GCS.

## 3. Cấu hình & Chạy Script
Thiết lập biến môi trường để script biết cần đọc dữ liệu từ GCS:

```bash
# 1. Thiết lập biến môi trường (Chấp nhận cả SPARK_ENV hoặc TRAINING_ENV)
export SPARK_ENV=cloud
export TRAINING_ENV=cloud

cd ~/MiningMassiveData/distributed_training

# 2. Chạy script đóng gói dữ liệu
# Lưu ý: Chạy dưới dạng module (-m) để tránh lỗi import
python3 -m src.prepare_eval_pkl
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
