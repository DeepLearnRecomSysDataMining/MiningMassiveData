import os
import json
import glob


def analyze_and_update(main_file_path, metadata_dir, output_file_path):
    # ==========================================
    # BƯỚC 1: XỬ LÝ DỮ LIỆU THƯ MỤC METADATA
    # ==========================================
    asin_to_categories_set = {}
    small_files = glob.glob(os.path.join(metadata_dir, '*.jsonl'))

    for file_path in small_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    row = json.loads(line)
                    asin = row.get('asin')
                    cat = row.get('main_category')
                    if asin and cat:
                        # Dùng set() để tự động loại bỏ các category trùng lặp
                        if asin not in asin_to_categories_set:
                            asin_to_categories_set[asin] = set()
                        asin_to_categories_set[asin].add(str(cat))
                except json.JSONDecodeError:
                    pass

    # Chuyển set thành chuỗi (nếu có nhiều category khác nhau thì nối bằng " | ")
    asin_to_category = {}
    for asin, cats in asin_to_categories_set.items():
        asin_to_category[asin] = " | ".join(sorted(list(cats)))

    print("=== THỐNG KÊ TỪ THƯ MỤC METADATA ===")
    print(f"Số lượng ASIN distinct (sau khi gộp trùng lặp): {len(asin_to_category)}")
    print("Mẫu 10 phần tử đầu tiên trong Dictionary:")
    sample_count = 0
    for k, v in asin_to_category.items():
        if sample_count < 10:  # Chỉ in 10 phần tử để tránh trôi màn hình console
            print(f"  - ASIN: {k}  =>  main_category: {v}")
            sample_count += 1
        else:
            break

    # ==========================================
    # BƯỚC 2: THỐNG KÊ FILE MAIN (metadatas_amazon.jsonl)
    # ==========================================
    main_asins_seen = set()
    total_main_rows = 0

    with open(main_file_path, 'r', encoding='utf-8') as f_in:
        for line in f_in:
            line = line.strip()
            if not line: continue
            total_main_rows += 1
            try:
                row = json.loads(line)
                asin = row.get('asin')
                if asin:
                    main_asins_seen.add(asin)
            except json.JSONDecodeError:
                pass

    print("\n=== THỐNG KÊ TỪ FILE METADATAS_AMAZON ===")
    print(f"Tổng số dòng ban đầu: {total_main_rows}")
    print(f"Tổng số ASIN distinct (đã loại bỏ dòng trùng): {len(main_asins_seen)}")

    # Tính toán trước số lượng sẽ khớp thành công
    matched_asins = main_asins_seen.intersection(set(asin_to_category.keys()))
    print(f"Số lượng ASIN có thể khớp dữ liệu thành công: {len(matched_asins)}")

    # ==========================================
    # BƯỚC 3: CẬP NHẬT VÀ GHI FILE MỚI (LOẠI BỎ TRÙNG LẶP)
    # ==========================================
    updated_count = 0
    written_rows = 0
    seen_in_output = set()  # Dùng để lọc trùng lặp khi ghi file

    print("\nĐang tiến hành ghi file mới (chỉ giữ các dòng distinct)...")
    with open(main_file_path, 'r', encoding='utf-8') as f_in, \
            open(output_file_path, 'w', encoding='utf-8') as f_out:

        for line in f_in:
            line = line.strip()
            if not line: continue
            try:
                row = json.loads(line)
                asin = row.get('asin')

                # Bỏ qua nếu dòng này có asin đã được ghi rồi (loại bỏ trùng lặp cho main file)
                if not asin or asin in seen_in_output:
                    continue
                seen_in_output.add(asin)

                # Cập nhật main_category nếu có
                if asin in asin_to_category:
                    row['main_category'] = asin_to_category[asin]
                    updated_count += 1

                # Ghi dòng dữ liệu
                f_out.write(json.dumps(row, ensure_ascii=False) + '\n')
                written_rows += 1

            except json.JSONDecodeError:
                pass

    print("\n=== KẾT QUẢ CUỐI CÙNG ===")
    print(f"Đã ghi thành công {written_rows} dòng distinct vào file mới.")
    print(f"Trong đó, số dòng được cập nhật 'main_category': {updated_count}")
    print(f"File kết quả: {output_file_path}")


# --- CẤU HÌNH ---
MAIN_FILE = 'metadatas_amazon.jsonl'
METADATA_DIR = 'metadata'
OUTPUT_FILE = 'metadatas_amazon.jsonl'

if __name__ == "__main__":
    analyze_and_update(MAIN_FILE, METADATA_DIR, OUTPUT_FILE)