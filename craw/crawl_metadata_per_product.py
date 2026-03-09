import json
import time
import os
import logging
import sys
import re
import glob
import pandas as pd
import contextlib
from selenium import webdriver
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def init(output_dir='data_amazon_split'):
    # Đảm bảo log hiển thị ra console của PyCharm
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("product_detail_scraper.log", encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Lấy đường dẫn tuyệt đối để tránh lỗi không tìm thấy thư mục
    abs_path = os.path.abspath(output_dir)
    os.makedirs(abs_path, exist_ok=True)
    logging.info(f"Hệ thống khởi tạo thành công. Thư mục làm việc: {abs_path}")
    return abs_path


def get_chrome_driver():
    options = Options()
    options.add_argument("--headless")  # Hiện màn hình để debug nếu muốn (xóa dòng này)
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    try:
        # Tự động tìm Driver (Selenium 4+)
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(60)
        # Ẩn cờ webdriver để Amazon ít nghi ngờ hơn
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except Exception as e:
        logging.error(f"Không thể khởi tạo Driver: {e}")
        sys.exit(1)


# [Hàm extract_product_details giữ nguyên như cũ]
# def extract_product_details(driver, url):
#     try:
#         if "captcha" in driver.current_url.lower() or "automated access" in driver.page_source:
#             return "CAPTCHA"
#
#         wait = WebDriverWait(driver, 20)
#         wait.until(EC.presence_of_element_located((By.ID, "productTitle")))
#
#         title = safe_get_text(driver, "#productTitle")
#         price = safe_get_text(driver, "span.a-price span.a-offscreen")
#         asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
#         asin = asin_match.group(1) if asin_match else "Unknown"
#
#         img_element = driver.find_element(By.ID, "landingImage")
#         img_url = img_element.get_attribute("src")
#         hi_res_img = re.sub(r'\._AC_.*_\.', '.', img_url)
#
#         specs = {}
#         try:
#             spec_tables = driver.find_elements(By.CSS_SELECTOR,
#                                                "table.prodDetTable, #productDetails_techSpec_section_1, #technical-details-table")
#             for table in spec_tables:
#                 rows = table.find_elements(By.TAG_NAME, "tr")
#                 for row in rows:
#                     key = safe_get_text(row, "th")
#                     val = safe_get_text(row, "td")
#                     if key and val: specs[key.strip()] = val.strip()
#         except:
#             pass
#
#         return {
#             "asin": asin, "title": title, "price": price, "image_url": hi_res_img,
#             "url": url, "specifications": specs, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
#         }
#     except Exception as e:
#         logging.error(f"    [Lỗi] Không thể trích xuất {url}: {str(e)}")
#         return None

def extract_product_details(driver, url):
    """Trích xuất chi tiết nội dung trang sản phẩm bao gồm Product Description"""
    try:
        # Kiểm tra Captcha nhanh
        if "captcha" in driver.current_url.lower() or "automated access" in driver.page_source:
            return "CAPTCHA"

        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.ID, "productTitle")))

        # --- BỔ SUNG: Cuộn trang để kích hoạt Lazy Load cho Description ---
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(1.5)  # Chờ một chút để nội dung render

        title = safe_get_text(driver, "#productTitle")
        price = safe_get_text(driver, "span.a-price span.a-offscreen")

        asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
        asin = asin_match.group(1) if asin_match else "Unknown"

        # Lấy ảnh HD
        img_url = ""
        try:
            img_element = driver.find_element(By.ID, "landingImage")
            img_raw = img_element.get_attribute("src")
            img_url = re.sub(r'\._AC_.*_\.', '.', img_raw)
        except:
            pass

        # --- BỔ SUNG: Trích xuất Product Description ---
        # Selector chính xác dựa trên HTML bạn cung cấp
        product_description = safe_get_text(driver, "#productDescription")
        if not product_description:
            # Dự phòng nếu Amazon dùng cấu trúc cũ hơn hoặc thẻ con
            product_description = safe_get_text(driver, "#productDescription_feature_div")

        # Thu thập Technical Details (Specifications)
        specs = {}
        try:
            spec_tables = driver.find_elements(By.CSS_SELECTOR,
                                               "table.prodDetTable, #productDetails_techSpec_section_1, #technical-details-table")
            for table in spec_tables:
                rows = table.find_elements(By.TAG_NAME, "tr")
                for row in rows:
                    key = safe_get_text(row, "th")
                    val = safe_get_text(row, "td")
                    if key and val: specs[key.strip()] = val.strip()
        except:
            pass

        # Lấy thêm từ bảng Product Overview nếu Specs vẫn trống
        if not specs:
            try:
                rows = driver.find_elements(By.CSS_SELECTOR, ".po-table tr")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 2: specs[cells[0].text.strip()] = cells[1].text.strip()
            except:
                pass

        return {
            "asin": asin,
            "title": title,
            "price": price,
            "description": product_description,  # <--- Trường dữ liệu mới
            "image_url": img_url,
            "url": url,
            "specifications": specs,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logging.error(f"    [Lỗi] Không thể trích xuất {url}: {str(e)}")
        return None


def safe_get_text(parent, selector, attr="innerText"):
    try:
        element = parent.find_element(By.CSS_SELECTOR, selector)
        return element.get_attribute(attr).strip()
    except NoSuchElementException:
        return None


def get_processed_urls(json_path):
    processed = set()
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'url' in data: processed.add(data['url'])
                except:
                    continue
    return processed


def save_batch_to_jsonl(batch_data, json_path):
    if not batch_data: return True
    try:
        with open(json_path, 'a', encoding='utf-8') as f:
            for item in batch_data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        return True
    except Exception as e:
        logging.error(f"        [Lỗi Ghi File] {e}")
        return False


def process_csv_files(source_dir):
    abs_source_dir = init(source_dir)
    try:
        all_entries = os.listdir(abs_source_dir)
    except Exception as e:
        logging.error(f"Không thể truy cập thư mục {abs_source_dir}: {e}")
        return
    csv_filenames = [ f for f in all_entries if f.lower().endswith('.csv') and re.search(r'amazon_batch_\d+', f) ]
    csv_filenames.sort(key=lambda f: int(re.findall(r'\d+', f)[0]))
    print(f"\n--- TÌM THẤY {len(csv_filenames)} FILE CSV HỢP LỆ ---")
    for name in csv_filenames:
        print(f"  > {name}")

    if not csv_filenames:
        logging.error("Không tìm thấy tên file .csv nào khớp định dạng. Dừng!")
        return

    driver = get_chrome_driver()
    batch_size = 20

    try:
        for file_name in csv_filenames[30:]:
            full_csv_path = os.path.join(abs_source_dir, file_name)
            json_name = file_name.replace(".csv", "_details.jsonl")
            full_json_path = os.path.join(abs_source_dir, json_name)
            logging.info(f"=== BẮT ĐẦU FILE: {file_name} ===")
            try:
                # low_memory=False giúp tránh cảnh báo với file lớn
                df = pd.read_csv(full_csv_path, low_memory=False)
            except Exception as e:
                logging.error(f"    [Lỗi] Không thể đọc {file_name}: {e}")
                continue
            if 'url' not in df.columns:
                logging.warning(f"    [Bỏ qua] Cột 'url' không tồn tại.")
                continue
            all_urls = df['url'].dropna().unique().tolist()
            processed_urls = get_processed_urls(full_json_path)
            urls_to_scrape = [u for u in all_urls if u not in processed_urls]
            logging.info(f"    Tiến độ: {len(processed_urls)}/{len(all_urls)} - Còn lại: {len(urls_to_scrape)}")

            for i in range(0, len(urls_to_scrape), batch_size):
                current_batch_urls = urls_to_scrape[i: i + batch_size]
                batch_results = []
                logging.info(f"    >>> ĐỢT {i // batch_size + 1} ({len(current_batch_urls)} link)")

                for idx, url in enumerate(current_batch_urls):
                    logging.info(f"        [{idx + 1}/{len(current_batch_urls)}] Cạo: {url}")
                    try:
                        driver.get(url)
                        product_data = extract_product_details(driver, url)
                        if product_data == "CAPTCHA":
                            logging.critical("    [!!!] CHẶN CAPTCHA - DỪNG CHƯƠNG TRÌNH.")
                            save_batch_to_jsonl(batch_results,'product_details.jsonl')
                            return
                        if product_data:
                            batch_results.append(product_data)
                    except Exception as e:
                        logging.error(f"        [Lỗi Duyệt] {url}: {e}")
                    time.sleep(4)
                if batch_results:
                    save_batch_to_jsonl(batch_results, 'product_details.jsonl')
                    logging.info(f"    <<< ĐÃ LƯU NỐI ĐUÔI {len(batch_results)} ITEM VÀO product_details.jsonl")
    finally:
        driver.quit()

if __name__ == "__main__":
    # Thay bằng tên thư mục chứa file CSV của bạn
    process_csv_files('data_amazon_split')