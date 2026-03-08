import pandas as pd
import glob
import os
import logging


def process_csv_split(input_dir='data_amazon_origin', output_dir='data_amazon_split', rows_per_file=100):
    # 1. Thiết lập logging để theo dõi tiến trình
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if not os.path.exists(input_dir):
        logging.error(f"Thư mục đầu vào '{input_dir}' không tồn tại!")
        return

    # Tạo thư mục đầu ra nếu chưa có
    os.makedirs(output_dir, exist_ok=True)

    # 2. Lấy danh sách tất cả các file CSV trong thư mục (loại trừ các file đã chia nhỏ nếu nằm cùng chỗ)
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))

    if not csv_files:
        logging.warning("Không tìm thấy file CSV nào để xử lý.")
        return

    logging.info(f"Tìm thấy {len(csv_files)} file CSV. Đang tiến hành gộp dữ liệu...")

    # 3. Gộp tất cả file CSV vào một DataFrame duy nhất
    list_df = []
    for file in csv_files:
        try:
            # Đọc file, bỏ qua các dòng lỗi nếu có
            df_temp = pd.read_csv(file, on_bad_lines='skip', low_memory=False)
            list_df.append(df_temp)
            logging.info(f"  -> Đã nạp: {os.path.basename(file)}")
        except Exception as e:
            logging.error(f"  -> Lỗi khi đọc file {file}: {e}")

    if not list_df:
        return

    full_df = pd.concat(list_df, ignore_index=True)

    # Loại bỏ các dòng trùng lặp hoàn toàn (nếu muốn sạch dữ liệu)
    initial_count = len(full_df)
    full_df.drop_duplicates(inplace=True)
    logging.info(
        f"Tổng số dòng sau khi gộp và lọc trùng: {len(full_df)} (Đã loại {initial_count - len(full_df)} dòng trùng)")

    # 4. Chia nhỏ DataFrame và lưu thành từng file CSV mới
    logging.info(f"Bắt đầu chia nhỏ dữ liệu thành các file {rows_per_file} dòng...")

    file_count = 0
    for i in range(0, len(full_df), rows_per_file):
        chunk = full_df.iloc[i: i + rows_per_file]

        # Đặt tên file theo định dạng: part_1.csv, part_2.csv...
        file_count += 1
        output_filename = f"amazon_batch_{file_count}.csv"
        output_path = os.path.join(output_dir, output_filename)

        chunk.to_csv(output_path, index=False, encoding='utf-8-sig')

        if file_count % 10 == 0:
            logging.info(f"  -> Đã tạo xong {file_count} file...")

    logging.info(f"✅ HOÀN TẤT! Đã chia dữ liệu vào thư mục: '{output_dir}'")
    logging.info(f"Tổng cộng tạo ra: {file_count} tệp tin.")


if __name__ == "__main__":
    # Lưu ý: Sửa 'data_amazon' thành tên thư mục thật của bạn nếu khác
    process_csv_split(input_dir='data_amazon_origin', output_dir='data_amazon_split', rows_per_file=100)