import os
import gzip
import shutil
import logging

logger = logging.getLogger("file_utils")

def decompress_gz_files(data_dir: str):
    """ Decompresses all .gz files in the data_dir to .jsonl if they don't exist yet. This makes them splittable for Spark, improving distributed processing performance."""
    all_files = os.listdir(data_dir)
    gz_files = [f for f in all_files if f.endswith(".gz")]

    if not gz_files:
        logger.info("Khong tim thay file .gz nao can giai nen.")
        return

    for f in gz_files:
        gz_path = os.path.join(data_dir, f)
        # Target file name (remove .gz). Vi du: reviews_amazon.jsonl.gz -> reviews_amazon.jsonl
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
    Reads the beginning of a file and detects its type based on JSON keys.
    Returns: 'vn_item', 'amz_item', 'vn_review', 'amz_review', or 'unknown'
    """
    if not os.path.isfile(file_path):
        return 'unknown'
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            head = f.read(2048)
            
            # --- 1. Review/Interaction Detection ---
            # reviews must have 'rating' and NOT 'average_rating' (usually)
            # or have clear review keys like 'user_id', 'fullName', 'review_text', 'content'
            is_review = False
            if '"rating"' in head:
                # If it's average_rating, it's likely an item, unless 'user_id'/'fullName' is also there
                if '"average_rating"' in head:
                    if '"user_id"' in head or '"fullName"' in head:
                        is_review = True
                else:
                    is_review = True

            if is_review:
                if '"fullName"' in head or '"productId"' in head:
                    return 'vn_review'
                return 'amz_review' # Default to Amazon for other reviews
            
            # --- 2. Item/Metadata Detection ---
            if '"specifications"' in head or '"product_url"' in head:
                return 'vn_item'
                
            if '"features"' in head or '"main_category"' in head or '"parent_asin"' in head:
                return 'amz_item'
                
            return 'unknown'
    except Exception:
        return 'unknown'
