import subprocess
import os
import logging
from config.training_config import TrainingConfig

logger = logging.getLogger("gcs_manager")

def download_training_data():
    """
    Downloads evaluation_dataset.pkl and vn_corpus.pkl from GCS to local disk.
    Follows the pattern of efficient data sync.
    """
    files_to_download = ["evaluation_dataset.pkl", "vn_corpus.pkl"]
    
    gcs_base = TrainingConfig.GCS_PREPARED_DATA
    local_base = TrainingConfig.LOCAL_DATA_DIR
    
    logger.info(f"Synchronizing training data from {gcs_base} to {local_base}...")
    
    for filename in files_to_download:
        gcs_path = f"{gcs_base}/{filename}"
        local_path = os.path.join(local_base, filename)
        
        # Kiểm tra nếu file đã tồn tại cục bộ thì có thể bỏ qua (hoặc force download)
        try:
            logger.info(f"Downloading {filename}...")
            subprocess.run(["gsutil", "cp", gcs_path, local_path], check=True)
            logger.info(f"Successfully downloaded {filename}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to download {filename} from GCS: {e}")
            # Nếu vn_corpus.pkl không có cũng không sao (tùy baseline)
            if filename == "evaluation_dataset.pkl":
                raise e

def upload_model_checkpoint(local_ckpt_path):
    """
    Uploads trained model checkpoint to GCS.
    """
    filename = os.path.basename(local_ckpt_path)
    gcs_dest = f"{TrainingConfig.GCS_OUTPUT_DIR}/models_checkpoints/{filename}"
    
    try:
        logger.info(f"Uploading checkpoint {filename} to GCS...")
        subprocess.run(["gsutil", "cp", local_ckpt_path, gcs_dest], check=True)
        logger.info(f"Checkpoint uploaded to: {gcs_dest}")
    except Exception as e:
        logger.error(f"GCS Upload Failed: {e}")
