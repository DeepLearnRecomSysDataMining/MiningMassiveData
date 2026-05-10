# BÁO CÁO CHI TIẾT: QUY TRÌNH ETL SPARK CHO DỮ LIỆU 55GB TRÊN GOOGLE CLOUD

**Mục tiêu:** Xử lý ổn định, tránh tràn RAM, tăng tốc đọc/xử lý và tối ưu chi phí hạ tầng Cloud.
**Phạm vi:** `etl_item_nodes_v2.py`, `etl_interactions_v2.py`, `evaluation_dataset_v2.py`, `data_validator_v2.py`

---

## 1. Tóm tắt điều hành (Executive Summary)

Pipeline ETL hiện tại được thiết kế theo hướng **“Spark-native”**: đọc JSONL bằng schema tường minh, chuẩn hóa dữ liệu bằng DataFrame API, hợp nhất dữ liệu Amazon và Việt Nam về schema chung, ghi kết quả trung gian sang Parquet, sau đó tạo bộ evaluation ở dạng ID-only. 

Cách làm này phù hợp với dữ liệu khoảng 55GB vì hạn chế suy luận schema, tránh kéo toàn bộ dữ liệu về driver, tận dụng xử lý phân tán, và giảm dung lượng đọc lại ở các bước sau. Điểm mạnh nổi bật là các bước xử lý đều ưu tiên select cột cần thiết, chuẩn hóa bằng hàm Spark thay vì Python loop, và tránh join metadata text lớn trong bước tạo negative candidates.

---

## 2. Quy trình ETL hiện tại

### 2.1. Chuẩn hóa dữ liệu sản phẩm (`etl_item_nodes_v2.py`)
Bước item nodes đọc metadata từ file JSONL của VN và Amazon bằng schema định nghĩa sẵn. 
*   **Dữ liệu đầu vào:** VN (product_id, asin, name, specs, desc, bc); Amazon (parent_asin, asin, title, features, desc, details).
*   **Kỹ thuật tối ưu:**
    *   **Schema tường minh:** Spark không phải scan dữ liệu để đoán schema, giảm I/O tới Cloud Storage.
    *   **Logic ASIN:** Ưu tiên `asin` > `parent_asin` > `details['ASIN']`.
    *   **Category UDF:** Phân loại bằng rule Regex từ breadcrumb và tên sản phẩm (chạy phân tán).
    *   **Output:** Ghi Parquet, `coalesce(16)`, khử trùng theo `product_id + asin`.

### 2.2. Chuẩn hóa dữ liệu tương tác/review (`etl_interactions_v2.py`)
*   **Quy trình:** Đọc review VN/Amazon, map các trường tương ứng, `unionByName`, loại dòng trống.
*   **Kỹ thuật tối ưu:**
    *   Khử trùng theo `user_id + product_id` làm giảm kích thước downstream.
    *   Ghi Parquet trung gian giúp các bước sau không phải đọc lại JSONL 55GB gốc.
    *   **Output:** Ghi Parquet với `coalesce(32)`.

### 2.3. Tạo evaluation dataset (`evaluation_dataset_v2.py`)
*   **Chiến thuật ID-only:** Chỉ select ID, ASIN, Category. Positive pairs khớp theo ASIN.
*   **Negative Mining:** Tách thành hard negatives (cùng category) và easy negatives (random).
*   **Kỹ thuật tối ưu:**
    *   Giới hạn Hard pool (~500 item/cat) và Easy pool (~2000 item) trước khi Join để tránh bùng nổ dữ liệu.
    *   **Không join metadata text lớn:** Đây là quyết định quan trọng nhất để tránh RAM tăng đột biến (full_text/specs_text chiếm 90% dung lượng).

---

## 3. Xử lý dữ liệu 55GB và tránh tràn RAM (OOM)

| Rủi ro | Cách xử lý trong V2 | Tác động |
| :--- | :--- | :--- |
| Spark phải đoán schema JSONL | Định nghĩa Schema (StructType) trước khi đọc | Giảm scan đầu vào, giảm thời gian và I/O |
| Cross join làm nổ dữ liệu | Giới hạn hard/easy negative pool trước khi broadcast | Kiểm soát số record sinh ra, tránh OOM |
| Cache tràn memory executor | Dùng MEMORY_AND_DISK, unpersist sau khi dùng | Spill sang disk thay vì fail job |
| Đọc quá nhiều cột text | Select cột cần thiết, evaluation lưu ID-only | Giảm RAM, shuffle và chi phí đọc |
| Quá nhiều file nhỏ | Coalesce/repartition trước khi ghi (mục tiêu 256-512MB/file) | Giảm overhead request và planning |

**Cấu hình khuyến nghị:**
- `spark.sql.files.maxPartitionBytes`: 128MB-256MB để tối ưu song song.
- `spark.sql.shuffle.partitions`: Gấp 2-3 lần tổng số vCPU của cụm máy.
- Bật **Adaptive Query Execution (AQE)** để Spark tự tối ưu plan khi chạy.

---

## 4. Tối ưu tốc độ đọc và xử lý

*   **Định dạng Parquet/Snappy:** Ưu tiên cho dữ liệu trung gian. Parquet cho phép đọc theo cột, giảm byte read khi chỉ cần lấy ID.
*   **Giảm số lần Action:** Validator đã gom nhiều thống kê vào một `agg.collect` thay vì nhiều lệnh `count()` riêng lẻ.
*   **Tránh Python UDF:** Sử dụng tối đa hàm Spark SQL (`regexp_replace`, `concat_ws`) để Spark Catalyst Optimizer có thể can thiệp tối ưu.
*   **Kiểm soát Skew:** Loại bỏ hoặc sampling riêng các category "other" quá lớn để tránh tình trạng 1 task chạy mãi không xong.

---

## 5. Giảm chi phí trên Google Cloud Storage (GCS)

*   **Lưu trữ theo cột:** Chỉ đọc cột cần thiết để giảm trực tiếp chi phí Retrieval/Read.
*   **Hạn chế file nhỏ:** Nhiều file nhỏ làm tăng phí Request (List/Get). Mục tiêu file đầu ra 256MB-512MB.
*   **Vị trí địa lý:** Đặt Bucket và Compute (Dataproc) cùng Region để tránh phí Network Transfer và độ trễ.
*   **Lifecycle Policy:** Chuyển Raw JSONL sang lớp lưu trữ rẻ hơn (Coldline/Archive) sau khi đã tạo Silver Parquet.

---

## 6. Kết luận và Ưu tiên triển khai

Pipeline ETL hiện tại đã có nền tảng cực tốt để xử lý 55GB. Để tăng độ ổn định, ưu tiên:
1.  **Chuẩn hóa cấu hình Spark theo cụm máy:** AQE bật, shuffle partitions theo vCPU.
2.  **Tối ưu layout dữ liệu:** Duy trì kích thước file Parquet chuẩn, giảm số lần đọc lại dữ liệu thô.
3.  **Quản trị chi phí Cloud:** Chạy batch bằng ephemeral cluster + autoscaling.

---

## 7. Checklist vận hành đề xuất

### Trước khi chạy:
- [ ] Kiểm tra tổng số file và kích thước file trung bình đầu vào.
- [ ] Xác nhận Region của bucket và cụm máy đồng nhất.

### Trong khi chạy:
- [ ] Theo dõi Spark UI: kiểm tra shuffle spill và task retry.
- [ ] Giám sát input bytes và thời gian từng stage.

### Sau khi chạy:
- [ ] Ghi lại số lượng output row và kích thước file trung bình.
- [ ] Ước tính chi phí GCS operation dựa trên số lượng file đã ghi.
