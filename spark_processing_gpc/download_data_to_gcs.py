import os
import sys
import subprocess
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("gpc_data_bridge")

def install_dependencies():
    """Cài đặt gdown nếu chưa có (thường dùng trên GCP VM mới)."""
    try:
        import gdown
    except ImportError:
        logger.info("Dang cai dat thu vien gdown...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "gdown"])

def upload_to_gcs(local_path, gcs_bucket_path):
    """Sử dụng gsutil để đẩy dữ liệu lên GCS với băng thông cao."""
    logger.info(f"Dang tai du lieu len GCS: {gcs_bucket_path}...")
    try:
        # Sử dụng -m để đa luồng, giúp upload nhanh hơn rất nhiều
        subprocess.run(["gsutil", "-m", "cp", "-r", local_path, gcs_bucket_path], check=True)
        logger.info(">>> Day du lieu len GCS THANH CONG!")
    except Exception as e:
        logger.error(f"Loi khi su dung gsutil: {e}")

def run_cloud_pipeline(drive_id, gcs_bucket):
    """Luồng xử lý dành riêng cho GCP."""
    install_dependencies()
    import gdown

    # 1. Tạo thư mục tạm trên VM
    local_tmp = "./tmp_drive_data"
    os.makedirs(local_tmp, exist_ok=True)
    
    # 2. Tải từ Drive về VM
    logger.info(f"--- BUOC 1: Tai tu Drive ve VM (ID: {drive_id}) ---")
    try:
        gdown.download_folder(id=drive_id, output=local_tmp, quiet=False)
    except Exception as e:
        logger.error(f"Loi khi tai tu Drive: {e}")
        return

    # 3. Đẩy từ VM lên GCS
    # GCS_PATH ví dụ: gs://my-recsys-project-bucket/raw_data/amazon_gpc/
    logger.info(f"--- BUOC 2: Day tu VM len GCS ({gcs_bucket}) ---")
    upload_to_gcs(f"{local_tmp}/*", gcs_bucket)

    # 4. Dọn dẹp VM (để tránh đầy ổ cứng Local SSD)
    logger.info("--- BUOC 3: Don dep du lieu tam tren VM ---")
    subprocess.run(["rm", "-rf", local_tmp])
    logger.info(">>> HOAN TAT LUONG CLOUD!")

if __name__ == "__main__":
    # --- CAU HINH DANH RIENG CHO CLOUD (GPC) ---
    
    # ID thư mục Drive của bạn
    GOOGLE_DRIVE_ID = "1tvFgRsMkN-11Ucvnjl9AfLPHGLGAIAuJ"
    
    # Đường dẫn GCS Bucket đích (Phải bắt đầu bằng gs://)
    # Ví dụ: "gs://my-recsys-project-bucket/raw_data/amazon_gpc/"
    GCS_DESTINATION = "gs://mining-data-2/raw_data/amazon_gpc/"

    if "YOUR_FOLDER_ID_HERE" in GOOGLE_DRIVE_ID or "your-bucket-name" in GCS_DESTINATION:
        logger.warning("Vui long mo file 'download_data_to_gcs.py' va dien thong tin GCS Bucket + Drive ID cua ban.")
    else:
        run_cloud_pipeline(GOOGLE_DRIVE_ID, GCS_DESTINATION)
