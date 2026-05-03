# GPC Training Guide

Để chạy phân tán hoàn toàn trên GCP (Google Cloud Platform) bằng cách sử dụng Vertex AI Training hoặc Compute Engine Multi-GPU

## 1. Khởi tạo Hạ tầng
Để chạy phân tán, bạn không dùng Spark (Dataproc) cho training mà dùng Vertex AI Custom Training.

- Storage: Dùng GCS Bucket (đã có trong spark_processing_gpc).
- Compute: Sử dụng các máy n1-standard-8 kèm GPU (T4 hoặc A100).
- Container: Đóng gói code vào Docker để đảm bảo môi trường đồng nhất trên mọi GPU Node.

## 2. Thay đổi code distributed_training để tương thích GCP
