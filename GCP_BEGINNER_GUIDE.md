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

## Bước 6: Tạo cụm Spark (Dataproc) để xử lý ETL - 2026: là Managed Apache Spark

### Cách 1: dùng UI GPC để làm (cách này khá nhanh và dễ)
1. Tìm kiếm "Dataproc"(nay là Managed Apache Spark) -> chọn -> giao diện Managed Apache Spark
2. Side Bar bên trái , ấn **Clusters** -> **Create Cluster** -> giao diện **Cluster on Compute Engine**.
2. **Name**: `amazon-cluster`.
3. **Region**: `us-central1` hoặc mặc định đặt `asia-southeast1`.
4. **Cluster type**: **Standard** (1 master, N workers). Ở dưới thì mặc định OS là Debian nhưng có thể đổi sang Ubuntu 22.
5. **Kéo xuống, mở rộng để cấu hình Master & Worker (tránh tốn RAM, CPU, GB tốn nhiều tiền)**: 
  - **Master**: Đưa về N4-Standard-2 (vCPU:2, RAM:8GB)
  - **Worker**: đưa về N4-Standard-2 (vCPU:2, RAM:8GB)
  - **Number of workers**: Đưa về 2, hoặc 3, 4 tùy bạn, ở ví dụ này đưa về 2 cho tiết kiệm 
  - **Disksize**: Data ở GCS tổng 50GB nên chọn mỗi cái node khoảng 50Gb là đủ.

| Tiêu chí                         | 2 Node Worker                | 3 Node Worker                | 4 Node Worker                |
|----------------------------------|------------------------------|------------------------------|------------------------------|
| Tổng số Node (Gồm 1 Master)     | 3 Máy                        | 4 Máy                        | 5 Máy                        |
| Tổng vCPU & RAM toàn cụm        | 6 vCPU / 24GB RAM            | 8 vCPU / 32GB RAM            | 10 vCPU / 40GB RAM           |
| Cấu hình Ổ cứng                 | Master: 50GB<br>Worker: 50GB/máy | Master: 50GB<br>Worker: 50GB/máy | Master: 50GB<br>Worker: 50GB/máy |
| Sức tải (Mỗi Worker gánh)       | ~25GB dữ liệu                | ~16.6GB dữ liệu              | ~12.5GB dữ liệu              |
| Hiệu năng & Thời gian (Ước tính)| Chậm nhất.<br>45 - 60 phút   | Tốc độ tốt.<br>30 - 40 phút  | Nhanh nhất.<br>20 - 25 phút  |
| Giới hạn Quota GCP              | An toàn 100%                 | Chạm trần (8 vCPU).<br>Có thể bị từ chối | Chắc chắn lỗi (vượt quota)   |
| Chi phí ước tính ($/giờ)        | ~$0.36 / giờ                | ~$0.48 / giờ                | ~$0.60 / giờ                |

> Với 50GB dữ liệu jsonl

mỗi node worker cần Disk từ 1,5 -> 3 lần GB mà dữ liệu nó xử lý, nên

| Worker | Data / node | Disk đề xuất |
| ------ | ----------- | ------------ |
| 2      | 25GB        | 40–50GB      |
| 3      | 16.6GB      | 30–40GB      |
| 4      | 12.5GB      | 25–30GB      |

### Cách 2: Làm bằng code qua SSH
Mở cửa sổ SSH của `coordinator-vm`, copy toàn bộ khối này dán vào và Enter:
```bash
gcloud dataproc clusters create amazon-cluster \
    --region=asia-southeast1 \
    --master-machine-type=n4-standard-2 \
    --master-boot-disk-size=50GB \
    --num-workers=2 \
    --worker-machine-type=n4-standard-2 \
    --worker-boot-disk-size=50GB \
    --image-version=2.1-debian11 (hoặc chọn Ubuntu 22)
```

### Chú ý: Cách để giảm chi phí: 

#### 🚀 Hướng dẫn tạo Dataproc Cluster tối ưu (Spark ETL ~50GB)

#### 🎯 Mục tiêu
- Tối ưu **chi phí / hiệu năng / độ ổn định**
- Sử dụng **Spot VM (Secondary workers)**
- Đảm bảo chạy ETL ~50GB ổn định

---

#### 🧱 Tổng quan kiến trúc

| Thành phần | Số lượng | Loại |
|-----------|--------|------|
| Manager (Master) | 1 | On-demand |
| Primary Workers | 2 | On-demand |
| Secondary Workers | 2 | 🔥 Spot (Preemptible) |

---

#### ⚙️ Cấu hình chi tiết

> 1. Cluster basic

- **Cluster name**: `amazon-cluster`
- **Region**: `asia-southeast1`
- **Cluster type**: Standard (1 master, N workers)
- **Image version**: `2.3-ubuntu22`

---

> 2. Manager node (Master)

| Thuộc tính | Giá trị |
|-----------|--------|
| Machine type | `n4-standard-2` (2 vCPU, 8GB RAM) |
| Disk | 30GB |
| Disk type | Hyperdisk Balanced |
| Local SSD | 0 |

---

> 3. Primary Worker nodes

| Thuộc tính | Giá trị |
|-----------|--------|
| Number of workers | 2 |
| Machine type | `n4-standard-2` |
| vCPU | 2 |
| RAM | 8GB |
| Disk | 30GB |
| Disk type | Hyperdisk Balanced |
| Local SSD | 0 |

 ⚠️ Lưu ý: Primary workers bắt buộc ≥ 2

---

> 4. Secondary Worker nodes (Spot VM)

| Thuộc tính | Giá trị |
|-----------|--------|
| Number of workers | 2 |
| Preemptibility | ✅ Preemptible (Spot) |
| Machine type | `n4-standard-2` |
| Disk | 30GB |
| Disk type | Hyperdisk Balanced |
| Local SSD | 0 |

 🔥 Đây là phần giúp giảm ~50–60% chi phí

---

#### ⚙️ Spark cấu hình khuyến nghị

Thêm vào **Cluster properties**:

```bash
spark:spark.sql.shuffle.partitions=16
spark:spark.default.parallelism=16
spark:spark.task.maxFailures=8
spark:spark.speculation=true
```

---

## Hủy ngay khi chạy xong Apache Spark để tránh tốn tiền

- Xóa cluster để giảm tiền
- xóa Compute Engine → VM instances
- xóa Compute Engine → Disks, và External Disk
- xóa storage bucket: Cloud Storage. Tìm bucket dạng: dataproc-staging-* hoặc dataproc-temp-*. Nếu chỉ dùng tạm:❗ DELETE bucket. Nếu dùng lâu dài: giữ lại nhưng clear data
- xóa Compute Engine → Snapshots
- xóa Compute Engine → Images
- có thể xóa Logs (Cloud Logging), nhưng nó ko tốn nhiều tiền

---

## Bước 7: Submit Job từ GCS và xử lý ETL 
### Cách 1: Làm trên Giao diện Web (Phức tạp hơn)
Giao diện Web không thể lấy code từ máy coordinator-vm của bạn. Để submit qua UI, bạn phải:

Mở SSH coordinator-vm, nén code và dùng gsutil đẩy cả file main.py và dependencies.zip lên một Bucket GCS (ví dụ: gs://mining-data-2/code/).

Lên giao diện Dataproc -> Chọn tab Jobs -> Bấm SUBMIT JOB.

Cấu hình Job:

Cluster: Chọn amazon-cluster

Job type: PySpark

Main python file: Gõ đường dẫn GCS (vd: gs://mining-data-2/code/main.py)

Python files (thư mục phụ): Gõ đường dẫn (vd: gs://mining-data-2/code/dependencies.zip)

Bấm SUBMIT.

### Cách 2: Làm bằng code qua SSH (Được khuyên dùng)
Mở cửa sổ SSH của `coordinator-vm`, đứng tại thư mục project `MiningMassiveData`, chạy các bước sau:

1. **Chuẩn bị file nén chứa code phụ trợ**:
Lệnh này giúp Spark phân phối các thư mục `config` và `src` đến tất cả các máy Workers trong cụm.
```bash
cd spark_processing_gpc
zip -r dependencies.zip config src
```

2. **Bắn job sang cụm Dataproc**:
Thay thế `gs://mining-data-2/` bằng tên bucket của bạn nếu khác.
```bash
gcloud dataproc jobs submit pyspark main.py \
    --cluster=amazon-cluster \
    --region=asia-southeast1 \
    --py-files=dependencies.zip \
    --properties="spark.executorEnv.SPARK_ENV=cloud,spark.yarn.appMasterEnv.SPARK_ENV=cloud,spark.executorEnv.RAW_DATA_DIR=gs://mining-data-2/raw_data/amazon_gpc/,spark.executorEnv.OUTPUT_BASE=gs://mining-data-2/output/" \
    -- \
    --validate
```
*Giải thích các tham số:*
- `--py-files`: Gửi kèm các module `src` và `config`.
- `--properties`: Truyền biến môi trường để code Python bên trong các Worker nhận diện được GCS.
- `--`: Sau dấu này là các tham số truyền trực tiếp vào hàm `main()` của Python (ví dụ: `--validate`, `--skip-scan`).

3. **Theo dõi tiến trình**:
- Màn hình SSH sẽ hiển thị log trực tiếp.
- Bạn sẽ thấy các dòng log mới dạng `>>> START PHASE 1...` để biết hệ thống đang chạy đến đâu.

### Theo dõi Log qua Cloud Logging (Cách chuyên nghiệp)
Nếu bạn lỡ đóng cửa sổ SSH, bạn vẫn có thể xem log:
1. Vào **Dataproc** -> **Jobs**.
2. Nhấn vào **Job ID** đang chạy.
3. Chọn tab **Monitoring** để xem biểu đồ CPU/RAM.
4. Chọn tab **Output** hoặc nhấn vào link **View logs in Cloud Logging** để xem log chi tiết, lọc theo từng Worker.

### Xóa Hạ tầng để tránh tốn tiền thêm
Ngay khi Job báo "Succeeded" và bạn đã kiểm tra kết quả trên GCS thành công.

1. Qua giao diện UI:
Vào **Dataproc** -> **Clusters** -> Chọn tick vào `amazon-cluster` -> Bấm nút **DELETE**.

2. Qua code SSH:
```bash
gcloud dataproc clusters delete amazon-cluster --region=asia-southeast1 -q
```

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
