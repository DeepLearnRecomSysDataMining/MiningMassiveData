import os
import torch
import logging

class TrainingConfig:
    # Nhận diện môi trường GCP (giống spark_processing_gpc)
    IS_CLOUD = os.getenv("TRAINING_ENV") == "cloud"
    
    # 1. GCS Paths
    GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://mining-data-2")
    GCS_ROOT = f"{GCS_BUCKET}/output"
    
    # 2. Local Paths (Sử dụng /tmp/training trên Cloud để tránh tràn disk hệ thống)
    LOCAL_BASE = "/tmp/training_data" if IS_CLOUD else "data/prepared_data_improved"
    LOCAL_MODELS_DIR = "models_checkpoints"
    @property
    def DEVICE(self):
        # Trên GCP, ép dùng CUDA nếu có, nếu không dùng CPU
        local_rank = int(os.getenv("LOCAL_RANK", 0))
        if torch.cuda.is_available():
            return torch.device(f"cuda:{local_rank}")
        return torch.device("cpu")
    # 3. Distributed Params (Lấy từ Vertex AI Environment Variables)
    @property
    def WORLD_SIZE(self): return int(os.getenv("WORLD_SIZE", "1"))
    
    @property
    def RANK(self): return int(os.getenv("RANK", "0"))

    @staticmethod
    def _get_env_or_default(key, default):
        return os.getenv(key, default).replace("\\", "/")

    # --- 1. Cloud Paths (GCS) ---
    @property
    def GCS_BUCKET(self):
        return self._get_env_or_default("GCS_BUCKET", "gs://mining-data-2")
    
    @property
    def GCS_OUTPUT_DIR(self):
        return f"{self.GCS_BUCKET}/output"

    @property
    def GCS_PREPARED_DATA(self):
        return f"{self.GCS_OUTPUT_DIR}/prepared_data_improved"

    @property
    def GCS_INTERACTIONS(self):
        return f"{self.GCS_OUTPUT_DIR}/all_interactions"

    @property
    def GCS_ITEM_NODES(self):
        return f"{self.GCS_OUTPUT_DIR}/item_nodes"

    # --- 2. Local Paths (Scratch) ---
    @property
    def LOCAL_DATA_DIR(self):
        # Use current directory/data or a temp dir
        path = "data/prepared_data_improved"
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def LOCAL_MODELS_DIR(self):
        path = "models_checkpoints"
        os.makedirs(path, exist_ok=True)
        return path

    @property
    def EVAL_PKL_PATH(self):
        return os.path.join(self.LOCAL_DATA_DIR, "evaluation_dataset.pkl")

    @property
    def VN_CORPUS_PKL_PATH(self):
        return os.path.join(self.LOCAL_DATA_DIR, "vn_corpus.pkl")

    # --- 3. Training Hyperparameters ---
    @property
    def DEVICE(self):
        # Lấy GPU theo rank cục bộ (local_rank)
        local_rank = int(os.getenv("LOCAL_RANK", 0))
        return torch.device(f"cuda:{local_rank}" if torch.cuda.is_available() else "cpu")

    @property
    def WORLD_SIZE(self):
        return int(os.getenv("WORLD_SIZE", "1"))

    @property
    def RANK(self):
        return int(os.getenv("RANK", "0"))

    @property
    def MASTER_ADDR(self):
        return self._get_env_or_default("MASTER_ADDR", "localhost")

    @property
    def MASTER_PORT(self):
        return self._get_env_or_default("MASTER_PORT", "12355")

    @property
    def BATCH_SIZE(self):
        return int(self._get_env_or_default("BATCH_SIZE", "64"))

    @property
    def EPOCHS(self):
        return int(self._get_env_or_default("EPOCHS", "15"))

    @property
    def LR(self):
        return float(self._get_env_or_default("LR", "1e-3"))

# Khởi tạo instance duy nhất
TrainingConfig = TrainingConfig()

# Cấu hình Logging chuyên nghiệp
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("training_pipeline.log", encoding="utf-8")
        ]
    )
