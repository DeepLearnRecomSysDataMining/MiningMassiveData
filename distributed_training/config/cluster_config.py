# ============================================================
# distributed_training/config/cluster_config.py
# Cấu hình cho việc huấn luyện đa máy chủ (Multi-node)
# ============================================================

import os

class ClusterConfig:
    # Số lượng máy tham gia huấn luyện
    WORLD_SIZE = 2  # Bạn có thể nâng lên 4 tùy vào tài nguyên GCP
    
    # IP và Port của máy Master (Node 0)
    MASTER_ADDR = "10.128.0.2"  # Thay bằng IP nội bộ của máy Master trên GCP
    MASTER_PORT = "12355"
    
    # Đường dẫn dữ liệu đầu ra từ Spark
    # DATA_PATH = "../output/evaluation_dataset"
    # MODEL_SAVE_PATH = "../output/trained_models"
    # TRỎ VÀO Ổ ĐĨA MẠNG DÙNG CHUNG
    DATA_PATH = "/mnt/shared_data/MiningSparkProcess/output/evaluation_dataset"
    MODEL_SAVE_PATH = "/mnt/shared_data/MiningSparkProcess/output/trained_models"

    # Cấu hình cho từng Baseline
    BASELINES = [
        "BM25",      # Ranker truyền thống (không train DDP, chạy Spark job)
        "SBERT",     # Deep Learning (DDP)
        "DSSM",      # Deep Learning (DDP)
        "GCN",       # Graph Learning (DDP)
        "HYBRID"     # Ensemble logic
    ]

os.makedirs(ClusterConfig.MODEL_SAVE_PATH, exist_ok=True)
