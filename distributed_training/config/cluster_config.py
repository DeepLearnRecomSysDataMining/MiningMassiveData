import os

class ClusterConfig:
    # Số lượng máy tham gia huấn luyện (Lấy từ env hoặc mặc định 2)
    WORLD_SIZE = int(os.getenv("WORLD_SIZE", 2))
    
    # IP và Port của máy Master (Node 0)
    MASTER_ADDR = os.getenv("MASTER_ADDR", "10.128.0.2")
    MASTER_PORT = os.getenv("MASTER_PORT", "12355")
    
    # --- ĐƯỜNG DẪN TRÊN GCS ---
    # Lấy từ env để linh hoạt (Ví dụ: export GCS_BUCKET=gs://my-recsys-bucket)
    GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://my-recsys-project-bucket")
    
    # Dữ liệu đầu ra từ Spark ETL (Bước 3)
    DATA_PATH = f"{GCS_BUCKET}/processed_data/evaluation_dataset"
    
    # Nơi lưu Model checkpoints (Bước 4)
    MODEL_SAVE_PATH = f"{GCS_BUCKET}/models"

    # Cấu hình cho từng Baseline
    BASELINES = [
        "BM25",      # Ranker truyền thống
        "SBERT",     # Deep Learning (DDP)
        "DSSM",      # Deep Learning (DDP)
        "GCN",       # Graph Learning (DDP)
        "HYBRID"     # Ensemble logic
    ]
