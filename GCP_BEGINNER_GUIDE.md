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

## Bước 4: Thiết lập Môi trường Cloud (Quan trọng)
Trong cửa sổ **SSH**, hãy cài đặt các biến sau để code nhận diện bạn đang chạy trên GCP:

```bash
# Thay đổi 'ten-bucket-cua-ban' thành tên Bucket bạn tạo ở Bước 2
export SPARK_ENV="cloud"
export RAW_DATA_DIR="gs://ten-bucket-cua-ban/raw_data/amazon_gpc/"
export OUTPUT_BASE="gs://ten-bucket-cua-ban/output/"
```

---

## Bước 5: Đưa code lên VM và Tải dữ liệu từ Drive
Trong cửa sổ **SSH**, thực hiện:

```bash
# 1. Cài đặt Python và Thư viện GCP
sudo apt-get update
sudo apt-get install git python3-pip -y
pip3 install gdown google-cloud-storage

# 2. Clone code
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
## Bước 5: Tạo cụm Spark (Dataproc) để xử lý ETL
1. Tìm kiếm "Dataproc" -> **Create Cluster** -> **Cluster on Compute Engine**.
2. **Name**: `spark-cluster`.
3. **Region**: `us-central1`.
4. **Master node**: `e2-standard-4` (4 vCPU, 16GB RAM).
5. **Worker nodes**: Chọn 2 máy `e2-standard-4`.
6. Nhấn **Create**.
---

## Bước 6: Tạo cụm Spark (Dataproc) và Chạy ETL
1. Tạo cụm Dataproc (Master: 1 máy, Worker: 2 máy).
2. Chạy lệnh Submit Job chuyên nghiệp:

```bash
gcloud dataproc jobs submit pyspark spark_processing_gpc/main.py \
    --cluster=spark-cluster \
    --region=us-central1 \
    --properties="spark.executorEnv.SPARK_ENV=cloud,spark.yarn.appMasterEnv.SPARK_ENV=cloud" \
    -- \
    --data-dir gs://[TEN-BUCKET-CUA-BAN]/raw_data/amazon_gpc/
```
*Lưu ý: Flag `--properties` giúp Spark nhận diện môi trường Cloud ngay cả bên trong các máy con (Workers).*

---

## Bước 7: Huấn luyện trên GPU (Distributed Training)
Bước này tốn kém nhất, nên hãy chỉ làm khi Bước 6 đã xong:
1. Tạo một máy ảo GPU (Compute Engine -> Create Instance).
2. Ở phần **GPU**, chọn `NVIDIA T4`.
3. Cài đặt driver GPU và chạy file `distributed_training/train_sbert.py`.

---

## Bước 8: Theo dõi Log
Vì chúng ta đã tắt tính năng ghi log vào file cục bộ (để tránh lỗi GCS), bạn hãy theo dõi log tại:
1. Console Dataproc -> Jobs -> Nhấn vào Job ID đang chạy.
2. Tab **Output**: Xem log thời gian thực.
3. Hoặc vào **Cloud Logging** để xem log chi tiết của toàn bộ hệ thống.

---

## Lời khuyên cực kỳ quan trọng:
- **TẮT MÁY (STOP)**: Khi không làm việc, hãy nhấn nút **Stop** cho VM và **Delete** cụm Dataproc. Nếu để nó chạy qua đêm, bạn sẽ mất rất nhiều tiền (Quota GPU và Dataproc tính phí theo giờ).
- **Quota**: Nếu không tạo được GPU, bạn cần vào "IAM & Admin" -> "Quotas" để yêu cầu Google tăng hạn mức GPU (với tài khoản mới thường là 0).
- **TẮT MÁY (STOP)**: Luôn **Delete** cụm Dataproc sau khi xong việc. GCS sẽ giữ lại toàn bộ kết quả Parquet cho bạn.
- **Quota**: Nếu bị lỗi tạo máy ảo, hãy kiểm tra "Quotas" để đảm bảo tài khoản của bạn được phép dùng máy ảo tại `us-central1`.

Bạn hãy bắt đầu từ **Bước 1 và Bước 2** đi, nếu vướng ở đâu hãy hỏi tôi ngay nhé!
