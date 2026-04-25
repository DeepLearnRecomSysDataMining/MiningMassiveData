import pandas as pd
import os
import logging

# Cấu hình logging để theo dõi quá trình lọc
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def clean_and_compare_csv(folder_path):
    all_files = os.listdir(folder_path)
    file_2_list = [f for f in all_files if f.endswith('_2.csv')]

    if not file_2_list:
        logging.warning("❌ Không tìm thấy file nào có đuôi _2.csv trong thư mục!")
        return
    logging.info(f"🚀 Tìm thấy {len(file_2_list)} cặp file cần xử lý.")

    for f2 in file_2_list:
        # Xác định tên file gốc tương ứng
        original_file = f2.replace('_2.csv', '.csv')
        f2_path = os.path.join(folder_path, f2)
        original_path = os.path.join(folder_path, original_file)

        logging.info(f"--- Đang xử lý cặp: [{original_file}] <-> [{f2}] ---")

        # Kiểm tra nếu file gốc không tồn tại thì bỏ qua
        if not os.path.exists(original_path):
            logging.warning(f"⚠️ File gốc {original_file} không tồn tại. Bỏ qua bước lọc trùng.")
            continue

        try:
            # 1. Đọc file gốc để lấy tập hợp ASIN đã có
            df_orig = pd.read_csv(original_path)
            if 'asin' not in df_orig.columns:
                logging.error(f"❌ File {original_file} không có cột 'asin'.")
                continue
            existing_asins = set(df_orig['asin'].astype(str).unique())

            # 2. Đọc file _2
            df_2 = pd.read_csv(f2_path)
            if 'asin' not in df_2.columns:
                logging.error(f"❌ File {f2} không có cột 'asin'.")
                continue

            initial_count = len(df_2)

            # 3. Lọc: Chỉ giữ lại các row có ASIN CHƯA tồn tại trong file gốc
            # Đồng thời xóa luôn trùng lặp nội bộ trong file _2
            df_2_cleaned = df_2[~df_2['asin'].astype(str).isin(existing_asins)]
            df_2_cleaned = df_2_cleaned.drop_duplicates(subset=['asin'], keep='first')
            final_count = len(df_2_cleaned)
            removed_count = initial_count - final_count

            df_2_cleaned.to_csv(f2_path, index=False, encoding='utf-8')
            logging.info(f"✅ Hoàn tất: Loại bỏ {removed_count} dòng trùng.")
            logging.info(f"📊 File {f2} hiện tại còn: {final_count} ASIN mới duy nhất.")
        except Exception as e:
            logging.error(f"❌ Lỗi khi xử lý file {f2}: {str(e)}")

if __name__ == "__main__":
    # Điền đường dẫn thư mục chứa các file csv của bạn
    DATA_FOLDER = 'data_amazon_links'

    if os.path.exists(DATA_FOLDER):
        clean_and_compare_csv(DATA_FOLDER)
        logging.info("🏁 TẤT CẢ CÁC FILE ĐÃ ĐƯỢC LÀM SẠCH.")
    else:
        logging.error(f"❌ Thư mục {DATA_FOLDER} không tồn tại!")