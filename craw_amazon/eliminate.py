import os
import json
import logging
import sys

import pandas as pd
import glob

# ==========================================
# CẤU HÌNH THƯ MỤC
# ==========================================
CSV_DIR = 'data_integrated'
JSONL_DIR = 'metadata_1'
OUTPUT_DIR = 'uncrawled'


def init_logging():
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler("filter_uncrawled.log", encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )


def load_scraped_asins(jsonl_folder):
    """Đọc toàn bộ file .jsonl và gom tất cả ASIN đã cạo vào một Set để tra cứu nhanh"""
    scraped_asins = set()
    jsonl_files = glob.glob(os.path.join(jsonl_folder, '*.jsonl'))

    if not jsonl_files:
        logging.warning(f"⚠️ Không tìm thấy file .jsonl nào trong thư mục '{jsonl_folder}'.")
        return scraped_asins

    logging.info(f"📂 Đang tải dữ liệu từ {len(jsonl_files)} file .jsonl...")

    for filepath in jsonl_files:
        filename = os.path.basename(filepath)
        count = 0
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        # Hỗ trợ cả key 'asin' viết thường và 'ASIN' viết hoa
                        asin = data.get('asin') or data.get('ASIN')
                        if asin:
                            scraped_asins.add(str(asin))
                            count += 1
                    except json.JSONDecodeError:
                        logging.error(f"    ❌ Lỗi parse JSON dòng trong file {filename}")
            logging.info(f"    - {filename}: Nạp {count} ASIN.")
        except Exception as e:
            logging.error(f"    ❌ Lỗi đọc file {filename}: {e}")

    logging.info(f"✅ TỔNG CỘNG: Đã nạp {len(scraped_asins)} ASIN duy nhất đã cạo thành công.")
    return scraped_asins


def filter_uncrawled_csvs(csv_folder, output_folder, scraped_asins_set):
    """Quét các file CSV, lọc bỏ ASIN đã cạo và lưu phần còn lại vào thư mục mới"""
    os.makedirs(output_folder, exist_ok=True)
    csv_files = glob.glob(os.path.join(csv_folder, '*.csv'))

    if not csv_files:
        logging.warning(f"⚠️ Không tìm thấy file .csv nào trong thư mục '{csv_folder}'.")
        return

    logging.info(f"\n🚀 BẮT ĐẦU LỌC {len(csv_files)} FILE CSV...")

    total_original = 0
    total_remaining = 0

    for filepath in csv_files:
        filename = os.path.basename(filepath)
        try:
            df = pd.read_csv(filepath)

            # Kiểm tra xem file có cột 'asin' không
            if 'asin' not in df.columns and 'ASIN' not in df.columns:
                logging.warning(f"    ⚠️ Bỏ qua {filename}: Không tìm thấy cột 'asin'.")
                continue

            asin_col = 'asin' if 'asin' in df.columns else 'ASIN'

            # Ép kiểu cột ASIN về chuỗi để so sánh cho chuẩn xác
            df[asin_col] = df[asin_col].astype(str)

            original_count = len(df)
            total_original += original_count

            # LỌC DATA: Giữ lại những dòng mà ASIN KHÔNG nằm trong set scraped_asins
            df_uncrawled = df[~df[asin_col].isin(scraped_asins_set)]

            remaining_count = len(df_uncrawled)
            total_remaining += remaining_count

            # Lưu ra file mới
            output_filepath = os.path.join(output_folder, filename)
            df_uncrawled.to_csv(output_filepath, index=False, encoding='utf-8')

            filtered_out = original_count - remaining_count
            logging.info(
                f"    👉 {filename}: Gốc có {original_count} -> Đã cạo {filtered_out} -> Còn lại {remaining_count} lưu vào {output_folder}")

        except Exception as e:
            logging.error(f"    ❌ Lỗi xử lý file CSV {filename}: {e}")

    logging.info(f"\n🏁 HOÀN TẤT!")
    logging.info(
        f"📊 THỐNG KÊ: Tổng sản phẩm ban đầu: {total_original} | Đã lọc bỏ: {total_original - total_remaining} | Cần cạo tiếp: {total_remaining}")


if __name__ == "__main__":
    init_logging()

    # Bước 1: Thu thập danh sách ASIN đã cạo
    scraped_asins = load_scraped_asins(JSONL_DIR)

    # Bước 2: Lọc CSV và lưu ra thư mục mới
    if scraped_asins:
        filter_uncrawled_csvs(CSV_DIR, OUTPUT_DIR, scraped_asins)
    else:
        logging.warning("⚠️ Không có ASIN nào được nạp từ JSONL. Toàn bộ CSV sẽ được sao chép sang thư mục mới.")
        # Vẫn chạy bộ lọc (lúc này set rỗng, nên nó sẽ giữ lại 100% data)
        filter_uncrawled_csvs(CSV_DIR, OUTPUT_DIR, scraped_asins)