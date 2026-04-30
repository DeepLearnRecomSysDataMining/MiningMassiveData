import os
import gzip
import shutil
import logging
from google.cloud import storage

logger = logging.getLogger("file_utils")

def list_files(path: str):
    """Lists files in a directory (local or GCS)."""
    if path.startswith("gs://"):
        # GCS path handling
        bucket_name = path.replace("gs://", "").split("/")[0]
        prefix = "/".join(path.replace("gs://", "").split("/")[1:])
        
        storage_client = storage.Client()
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
        # Return full gs:// paths
        return [f"gs://{bucket_name}/{blob.name}" for blob in blobs if not blob.name.endswith('/')]
    else:
        # Local path handling
        if not os.path.exists(path):
            return []
        return [os.path.join(path, f) for f in os.listdir(path)]

def decompress_gz_files(data_dir: str):
    """ (Local only) Decompresses all .gz files. For GCS, better to upload uncompressed."""
    if data_dir.startswith("gs://"):
        logger.warning("Decompression on GCS should be done before upload. Skipping.")
        return
    
    all_files = os.listdir(data_dir)
    gz_files = [f for f in all_files if f.endswith(".gz")]

    if not gz_files:
        logger.info("Khong tim thay file .gz nao can giai nen.")
        return

    for f in gz_files:
        gz_path = os.path.join(data_dir, f)
        target_name = f[:-3] 
        target_path = os.path.join(data_dir, target_name)

        if os.path.exists(target_path):
            logger.info(f"File {target_name} da ton tai, bo qua giai nen.")
            continue
        logger.info(f"Dang giai nen {f} -> {target_name}...")

        try:
            with gzip.open(gz_path, 'rb') as f_in:
                with open(target_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            logger.info(f"Giai nen thanh cong: {target_name}")
        except Exception as e:
            logger.error(f"Loi khi giai nen {f}: {e}")

def detect_jsonl_type(file_path):
    """
    Detects type based on JSON keys. Supports local and GCS paths.
    """
    try:
        head = ""
        if file_path.startswith("gs://"):
            # Read from GCS
            bucket_name = file_path.replace("gs://", "").split("/")[0]
            blob_name = "/".join(file_path.replace("gs://", "").split("/")[1:])
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            head = blob.download_as_string(start=0, end=2048).decode('utf-8', errors='ignore')
        else:
            # Read from Local
            if not os.path.isfile(file_path):
                return 'unknown'
            with open(file_path, 'r', encoding='utf-8') as f:
                head = f.read(2048)
            
        if not head:
            return 'unknown'

        # --- 1. Review/Interaction Detection ---
        is_review = False
        if '"rating"' in head:
            if '"average_rating"' in head:
                if '"user_id"' in head or '"fullName"' in head:
                    is_review = True
            else:
                is_review = True

        if is_review:
            if '"fullName"' in head or '"productId"' in head:
                return 'vn_review'
            return 'amz_review'
        
        # --- 2. Item/Metadata Detection ---
        if '"specifications"' in head or '"product_url"' in head:
            return 'vn_item'
            
        if '"features"' in head or '"main_category"' in head or '"parent_asin"' in head:
            return 'amz_item'
            
        return 'unknown'
    except Exception as e:
        logger.debug(f"Error detecting type for {file_path}: {e}")
        return 'unknown'
