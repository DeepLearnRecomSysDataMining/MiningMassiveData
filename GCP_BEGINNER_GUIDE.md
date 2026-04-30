# Hướng dẫn Triển khai RecSys từ A-Z trên Google Cloud (Cho người mới bắt đầu)

Chào mừng bạn đến với thế giới của GCP! Dưới đây là lộ trình để bạn đưa dự án từ Local lên Cloud.

---

## Bước 1: Kích hoạt các dịch vụ cần thiết (API)
Mặc định GCP sẽ khóa các dịch vụ để tránh tốn tiền. Bạn cần mở chúng lên:
1. Mở **Google Cloud Console** (https://console.cloud.google.com).
2. Chọn đúng Project của bạn ở góc trên bên trái.
3. Mở thanh tìm kiếm và gõ "API Library", sau đó tìm và nhấn **Enable** cho các dịch vụ sau:
   - **Compute Engine API** (Để chạy máy ảo)
   - **Cloud Dataproc API** (Để chạy Spark)
   - **Cloud Storage API** (Để lưu trữ dữ liệu)

---

## Bước 2: Tạo "Trái tim" lưu trữ (GCS Bucket)
Đây là nơi chứa toàn bộ dữ liệu của bạn.
1. Tìm kiếm "Storage" trong Console.
2. Nhấn **Create Bucket**.
3. **Name**: Đặt tên duy nhất (Ví dụ: `recsys-data-xxxx`). Ghi nhớ tên này.
4. **Location**: Chọn `us-central1` (Thường rẻ và nhiều GPU).
5. Nhấn **Create**.

---

## Bước 3: Tạo máy ảo điều phối (Coordinating VM)
Máy này dùng để chạy các script điều khiển (như `download_data.py`).
1. Tìm kiếm "Compute Engine" -> "VM Instances" -> **Create Instance**.
2. **Name**: `coordinator-vm`.
3. **Machine type**: `e2-medium` (Rẻ, đủ dùng cho điều phối).
4. **Boot Disk**: Chọn Ubuntu 22.04 LTS.
5. Nhấn **Create**.
6. Khi máy hiện lên, nhấn nút **SSH** để mở cửa sổ dòng lệnh.

---

## Bước 4: Đưa code lên VM và Tải dữ liệu từ Drive
Trong cửa sổ **SSH** vừa mở, hãy chạy các lệnh sau:

```bash
# 1. Cài đặt Git và Python
sudo apt-get update
sudo apt-get install git python3-pip -y

# 2. Clone code của bạn từ GitHub (Hoặc dùng công cụ upload file của GCP)
git clone [LINK_GITHUB_CUA_BAN]
cd [TEN_THU_MUC_PROJECT]

# 3. Cài đặt thư viện gdown
pip3 install gdown

# 4. Sửa file download_data.py (Dùng lệnh 'nano download_data.py')
# - Điền GOOGLE_DRIVE_ID của bạn.
# - Điền GCS_DESTINATION (gs://ten-bucket-vua-tao/raw_data/amazon_gpc/).

# 5. Chạy tải dữ liệu
python3 download_data.py
```

---

## Bước 5: Tạo cụm Spark (Dataproc) để xử lý ETL
1. Tìm kiếm "Dataproc" -> **Create Cluster** -> **Cluster on Compute Engine**.
2. **Name**: `spark-cluster`.
3. **Region**: `us-central1`.
4. **Master node**: `e2-standard-4` (4 vCPU, 16GB RAM).
5. **Worker nodes**: Chọn 2 máy `e2-standard-4`.
6. Nhấn **Create**.

---

## Bước 6: Chạy ETL (Xử lý dữ liệu)
Quay lại máy ảo **coordinator-vm** (Cửa sổ SSH), chạy lệnh này để Spark bắt đầu làm việc:

```bash
gcloud dataproc jobs submit pyspark spark_processing_gpc/main.py \
    --cluster=spark-cluster \
    --region=us-central1 \
    -- \
    --data-dir gs://[TEN-BUCKET-CUA-BAN]/raw_data/amazon_gpc/
```

---

## Bước 7: Huấn luyện trên GPU (Distributed Training)
Bước này tốn kém nhất, nên hãy chỉ làm khi Bước 6 đã xong:
1. Tạo một máy ảo GPU (Compute Engine -> Create Instance).
2. Ở phần **GPU**, chọn `NVIDIA T4`.
3. Cài đặt driver GPU và chạy file `distributed_training/train_sbert.py`.

---

## Lời khuyên cực kỳ quan trọng:
- **TẮT MÁY (STOP)**: Khi không làm việc, hãy nhấn nút **Stop** cho VM và **Delete** cụm Dataproc. Nếu để nó chạy qua đêm, bạn sẽ mất rất nhiều tiền (Quota GPU và Dataproc tính phí theo giờ).
- **Quota**: Nếu không tạo được GPU, bạn cần vào "IAM & Admin" -> "Quotas" để yêu cầu Google tăng hạn mức GPU (với tài khoản mới thường là 0).

Bạn hãy bắt đầu từ **Bước 1 và Bước 2** đi, nếu vướng ở đâu hãy hỏi tôi ngay nhé!
