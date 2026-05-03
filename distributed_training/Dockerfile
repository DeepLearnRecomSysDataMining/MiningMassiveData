# Sử dụng image PyTorch chính thức từ NVIDIA
FROM gcr.io/deeplearning-platform-release/pytorch-gpu.2-0

WORKDIR /app

# Copy code vào container
COPY . /app/

# Cài đặt thư viện (requirements_gpu.txt bạn đã có)
RUN pip install --no-cache-dir -r requirements_gpu.txt

# Thiết lập PYTHONPATH để import được các module trong src/
ENV PYTHONPATH="${PYTHONPATH}:/app"
ENV TRAINING_ENV="cloud"

# Entrypoint cho distributed training
ENTRYPOINT ["python", "-m", "torch.distributed.run", "--nproc_per_node=auto", "main.py"]
