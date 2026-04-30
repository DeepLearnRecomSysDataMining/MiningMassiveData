import os
import glob
import logging
import sys
import pandas as pd


def init_logging():
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler("merge_csv.log", encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )


def merge_csv_in_directory(target_dir, output_filename):
    """
    Hàm gộp tất cả các file CSV trong một thư mục thành 1 file duy nhất.
    Tự động bỏ qua file output nếu nó đã tồn tại để tránh lỗi gộp đệ quy.
    """
    if not os.path.exists(target_dir):
        logging.warning(f"⚠️ Thư mục '{target_dir}' không tồn tại. Bỏ qua.")
        return

    # Đường dẫn file output cuối cùng
    output_filepath = os.path.join(target_dir, output_filename)

    # Lấy tất cả file CSV trong thư mục
    all_csv_files = glob.glob(os.path.join(target_dir, '*.csv'))

    # Loại bỏ file output khỏi danh sách cần gộp (để tránh gộp chính nó)
    files_to_merge = [f for f in all_csv_files if os.path.abspath(f) != os.path.abspath(output_filepath)]

    if not files_to_merge:
        logging.info(f"    - Không có file CSV con nào trong '{target_dir}' để gộp.")
        return

    logging.info(f"📂 Đang tiến hành gộp {len(files_to_merge)} file trong thư mục '{target_dir}'...")

    dataframes = []
    total_rows = 0

    for filepath in files_to_merge:
        filename = os.path.basename(filepath)
        try:
            df = pd.read_csv(filepath)
            if not df.empty:
                dataframes.append(df)
                total_rows += len(df)
                logging.info(f"    - Đọc thành công '{filename}' ({len(df)} dòng)")
            else:
                logging.info(f"    - Bỏ qua '{filename}' vì file rỗng.")
        except Exception as e:
            logging.error(f"    ❌ Lỗi đọc file '{filename}': {e}")

    if not dataframes:
        logging.warning(f"⚠️ Không có dữ liệu hợp lệ nào để gộp trong '{target_dir}'.")
        return

    # Gộp tất cả DataFrames
    merged_df = pd.concat(dataframes, ignore_index=True)

    # Chuẩn hóa tên cột ASIN để xóa trùng lặp (nếu có)
    asin_col = None
    if 'asin' in merged_df.columns:
        asin_col = 'asin'
    elif 'ASIN' in merged_df.columns:
        asin_col = 'ASIN'

    if asin_col:
        merged_df[asin_col] = merged_df[asin_col].astype(str)
        initial_len = len(merged_df)
        merged_df.drop_duplicates(subset=[asin_col], keep='first', inplace=True)
        dropped_count = initial_len - len(merged_df)
        if dropped_count > 0:
            logging.info(f"    ✂️ Đã xóa {dropped_count} dòng ASIN trùng lặp sau khi gộp.")

    # Lưu ra file tổng
    try:
        merged_df.to_csv(output_filepath, index=False, encoding='utf-8')
        logging.info(f"✅ HOÀN TẤT: Đã gộp thành công vào file '{output_filename}' (Tổng: {len(merged_df)} dòng).\n")
    except Exception as e:
        logging.error(f"❌ LỖI GHI FILE '{output_filename}': {e}")


if __name__ == "__main__":
    init_logging()

    logging.info("🚀 BẮT ĐẦU CHẠY SCRIPT GỘP DỮ LIỆU...\n")

    # 1. Gộp các file trong thư mục 'uncrawled'
    merge_csv_in_directory(
        target_dir='uncrawled',
        output_filename='ALL_UNCRAWLED_MERGED.csv'
    )

    # 2. Gộp các file trong thư mục 'data_integrated'
    merge_csv_in_directory(
        target_dir='data_integrated',
        output_filename='all_links_amazon.csv'
    )

    logging.info("🏁 TOÀN BỘ QUÁ TRÌNH GỘP ĐÃ KẾT THÚC.")