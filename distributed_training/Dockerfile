# Sử dụng image PyTorch chính thức từ NVIDIA (Đã cài sẵn CUDA)
FROM gcr.io/deeplearning-platform-release/pytorch-gpu.2-4

WORKDIR /app

# Copy code vào container
COPY . /app/

# Cài đặt các thư viện cần thiết cho 6 Baselines
# Sử dụng --no-cache-dir để giảm dung lượng Image
RUN pip install --no-cache-dir -r requirements.txt

# Thiết lập PYTHONPATH để import được các module trong src/ và config/
ENV PYTHONPATH="/app"

# Biến môi trường mặc định để chạy trên Cloud
ENV TRAINING_ENV="cloud"

# Entrypoint chuẩn cho Distributed Training (DDP)
# Cho phép truyền tham số vào main.py (ví dụ: --baseline 3)
ENTRYPOINT ["python", "-m", "torch.distributed.run", "--nproc_per_node=auto", "main.py"]
