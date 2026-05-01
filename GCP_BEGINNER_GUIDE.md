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
   - Tôi chọn `asia` để ko mất phí liên lục địa -> từ các bước sau phi chọn Location/Region thuộc châu á
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

### Giao diện 2026 hơi khác , nhìn thanh sidebar bên trái để ấn vào `OS and Storage` mới chọn được Urbuntu

---

## Bước 4: Thiết lập Môi trường Cloud (Quan trọng)
Trong cửa sổ **SSH**, hãy cài đặt các biến sau để code nhận diện bạn đang chạy trên GCP:

project này có bucket : mining-data-2

```bash
# Thay đổi 'ten-bucket-cua-ban' thành tên Bucket bạn tạo ở Bước 2
export SPARK_ENV="cloud"
export RAW_DATA_DIR="gs://ten-bucket-cua-ban/raw_data/amazon_gpc/"
export OUTPUT_BASE="gs://ten-bucket-cua-ban/output/"
```
```bash
export SPARK_ENV="cloud"
export RAW_DATA_DIR="gs://mining-data-2/raw_data/amazon_gpc/"
export OUTPUT_BASE="gs://mining-data-2/output/"
```

### (Lưu ý: Nếu sau này bạn tắt cửa sổ SSH này đi và mở lại, bạn sẽ phải chạy lại 3 dòng lệnh này nhé).

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

# 4. Sửa file download_data_to_gcs.py (Dùng lệnh 'nano download_data_to_gcs.py')
# - Điền GOOGLE_DRIVE_ID của bạn.
# - Điền GCS_DESTINATION (gs://ten-bucket-vua-tao/raw_data/amazon_gpc/).

# 5. Chạy tải dữ liệu
python3 download_data_to_gcs.py
```

### Lỗi ngay chạy lệnh `sudo apt-get update`

Vấn đề do VM instance không có External IP nên không thể kết nối ra ngoài để tải update Ubuntu

- Cách 1: Edit cái VM Instance này để chọn : nhìn cột external Ip của VM instance sẽ thấy None 
  - Cuộn xuống tìm mục Network interfaces (Giao diện mạng), nhấn vào mũi tên thả xuống để mở rộng phần này ra. 
  - Tìm dòng External IPv4 address (Địa chỉ IPv4 bên ngoài). Hãy đổi nó từ "None" thành Ephemeral (Tạm thời).
  - Ấn nút Save
- Cách 1 có thể lỗi vì không chọn được (Ephemeral) do Chính sách tổ chức (Organization Policy) (thường gặp nếu bạn dùng tài khoản của công ty, trường học, hoặc được cấp sẵn). Chính sách này 
cấm việc gắn IP công cộng (External IP) vào máy ảo để đảm bảo bảo mật. Fix bằng dùng `Cloud NAT` giúp bên ngoài ko kết nối xâm nhập đc nhưng bên trong kết nối ra được. 
  - Tìm Cloud NAT -> ấn Get Started (hoặc Create a NAT gateway) 
  - Điền thông tin: 
    - Gateway name (Tên cổng): Đặt là my-nat-gateway.
    - Network (Mạng): Chọn default
    - Region (Khu vực): Bắt buộc chọn asia-southeast1 (hoặc đúng khu vực bạn đã tạo máy ảo coordinator-vm lúc nãy).
  - Ở phần Cloud Router, nhấp vào ô thả xuống và chọn Create new router:
    - Name: Đặt là my-router
    - Nhấn Create (Tạo) để hoàn tất việc tạo Router.
  - Quay lại trang tạo NAT (các thông số khác cứ để mặc định), kéo xuống dưới cùng và nhấn Create
  - Chạy lại lệnh trong SSH
    - đợi 1-2 phút: Mở lại SSH ở VM Instance rồi chạy lại 3 lệnh  export
    - Chạy lại lệnh update: sudo apt-get update 

```bash
export SPARK_ENV="cloud"
export RAW_DATA_DIR="gs://mining-data-2/raw_data/amazon_gpc/"
export OUTPUT_BASE="gs://mining-data-2/output/"
```

### Lỗi chạy lệnh: `pip3 install gdown google-cloud-storage`

- Ý nghĩa của lỗi: Hệ điều hành đang khóa không cho bạn dùng lệnh pip để cài thư viện bừa bãi vào hệ thống gốc. 
Lý do là Linux cũng dùng Python để chạy các ứng dụng lõi của nó, nếu bạn cài đè các thư viện mới lên có thể làm "sập" hệ điều hành.
- cách chuyên nghiệp và an toàn nhất hiện nay là tạo một Môi trường ảo (Virtual Environment - venv).
  - Bước 1: Cài đặt gói hỗ trợ môi trường ảo: 
      `sudo apt-get install python3-venv -y`
  - Bước 2: Tạo môi trường ảo riêng cho dự án của bạn. Lệnh này sẽ tạo ra một thư mục tên là recsys_env chứa một bản Python hoàn toàn độc lập.
      `python3 -m venv recsys_env`
  - Bước 3: Kích hoạt môi trường ảo
      `source recsys_env/bin/activate`
      - Sau khi chạy lệnh này, bạn sẽ thấy chữ (recsys_env) hiện lên ở tuốt đầu dòng lệnh, báo hiệu bạn đã vào trong "vùng an toàn"
  - Bước 4: Cài đặt thư viện thoải mái
      `pip install gdown google-cloud-storage`

### Chú ý: Từ giờ trở đi, mỗi lần bạn tắt cửa sổ SSH và mở lại, máy ảo sẽ quay về môi trường gốc (không có thư viện gdown). 
Do đó, trước khi muốn chạy file `download_data.py`, bạn chỉ cần gõ lại lệnh kích hoạt ở Bước 3 (`source recsys_env/bin/activate`) là được nhé!

### Các lệnh chạy sau đó ở Bước 5 này phải chạy với venv hết vì cần python3, gdown, ...

### Vấn đề với lệnh `python3 download_data.py` 
- yêu cầu VM instance có Disk > 10Gb , hoặc vài chục GB để load data từ drive về, sau đó đẩy lên.
- Nhưng nếu tôi muốn giản nén các file `.gz` ra ngay tại VM rồi mới nhét vào GCS, thì VM cần disk > 10GB rất nhiều.
- Giải pháp: dùng Colab + kỹ thuật Streaming + xử lý từng file lần lượt.
- file [streaming_drive_gcs.ipynb](spark_processing_gpc/streaming_drive_gcs.ipynb)

---

## Bước 6: Tạo cụm Spark (Dataproc) để xử lý ETL
1. Tìm kiếm "Dataproc" -> **Create Cluster** -> **Cluster on Compute Engine**.
2. **Name**: `spark-cluster`.
3. **Region**: `us-central1` hoặc mặc định đặt `asia...`.
4. **Master node**: `e2-standard-4` (4 vCPU, 16GB RAM).
5. **Worker nodes**: Chọn 2 máy `e2-standard-4`.
6. Nhấn **Create**.
---

## Bước 7: Tạo cụm Spark (Dataproc) và Chạy ETL
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

## Bước 8: Huấn luyện trên GPU (Distributed Training)
Bước này tốn kém nhất, nên hãy chỉ làm khi Bước 6 đã xong:
1. Tạo một máy ảo GPU (Compute Engine -> Create Instance).
2. Ở phần **GPU**, chọn `NVIDIA T4`.
3. Cài đặt driver GPU và chạy file `distributed_training/train_sbert.py`.

---

## Bước 9: Theo dõi Log
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
