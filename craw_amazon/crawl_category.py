import json
import random
import time
import os
import logging
import sys
import re
import pandas as pd
from selenium import webdriver
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import undetected_chromedriver as uc

def init(output_dir='categoties'):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    file_handler = logging.FileHandler("amazon_category_scraper_1.log", encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(file_handler)
    logging.root.addHandler(stream_handler)

    abs_path = os.path.abspath(output_dir)
    os.makedirs(abs_path, exist_ok=True)
    logging.info(f"Hệ thống khởi tạo thành công. Thư mục: {abs_path}")
    return abs_path

def get_chrome_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--incognito")  # Dùng ẩn danh để tránh lưu session lỗi

    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    driver = uc.Chrome(options=options, version_main=146)
    driver.set_page_load_timeout(60)

    return driver

def set_us_location(driver, zip_code="10001"):
    """Đổi vùng sang Mỹ để lấy giá USD và đầy đủ thông tin nội địa"""
    try:
        logging.info(f"📍 Đang thiết lập vùng địa lý US (Zip: {zip_code})...")
        driver.get("https://www.amazon.com")
        wait = WebDriverWait(driver, 15)
        # Click nút chọn địa chỉ
        loc_btn = wait.until(EC.element_to_be_clickable((By.ID, "nav-global-location-popover-link")))
        loc_btn.click()
        # Nhập Zip
        zip_input = wait.until(EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput")))
        zip_input.send_keys(zip_code)
        # Apply
        driver.find_element(By.ID, "GLUXZipUpdate").click()
        time.sleep(2)
        driver.refresh()
        logging.info("✅ Đã chuyển vùng thành công.")
    except:
        logging.warning("⚠️ Không thể tự động đổi vùng. Giá có thể vẫn là VND.")

def get_breadcrumb(driver):
    try:
        wait = WebDriverWait(driver, 10)
        breadcrumb_container = wait.until(
            EC.presence_of_element_located((By.ID, "desktop-breadcrumbs_feature_div"))
        )

        items = breadcrumb_container.find_elements(
            By.CSS_SELECTOR,
            "ul.a-unordered-list.a-horizontal.a-size-small li span a"
        )

        categories = [item.text.strip() for item in items if item.text.strip()]

        if categories:
            return "›".join(categories)
        return None

    except Exception:
        return None

def extract_product_breadcrumb(driver, url):
    try:
        driver.get(url)
        if "captcha" in driver.current_url.lower() or "automated access" in driver.page_source:
            return "CAPTCHA"
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.ID, "desktop-breadcrumbs_feature_div")))
        time.sleep(3)

        breadcrumb = get_breadcrumb(driver)
        return {
            "url": url,
            "breadcrumb": breadcrumb,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    except Exception as e:
        logging.error(f"      ❌ Lỗi trích xuất tại {url}: {str(e)[:100]}")
        return None

def process_amazon_breadcum(source_dir, categories_dict):
    init(source_dir)
    jsonl_path = os.path.abspath('categories')
    os.makedirs(jsonl_path, exist_ok=True)
    logging.info(f"🚀 Hệ thống khởi tạo thành công. Kết quả sẽ lưu tại: {jsonl_path}")

    driver = get_chrome_driver()
    set_us_location(driver)
    CHUNK_SIZE = 10

    try:
        for manual_category, file_name in categories_dict.items():
            full_csv_path = os.path.join(source_dir, file_name)
            if not os.path.exists(full_csv_path):
                logging.warning(f"⚠️ Bỏ qua: Không tìm thấy file {file_name} cho danh mục {manual_category}")
                continue
            save_name = f"{manual_category.replace(' ', '_').lower()}_details.jsonl"
            full_json_path = os.path.join(jsonl_path, save_name)
            logging.info(f"📂 ĐANG XỬ LÝ DANH MỤC: {manual_category.upper()} | File: {file_name}")

            df = pd.read_csv(full_csv_path, low_memory=False)
            if 'url' not in df.columns or 'asin' not in df.columns:
                logging.error(f"❌ File {file_name} thiếu cột 'url' hoặc 'asin'. Bỏ qua.")
                continue
            df = df[['url', 'asin']].dropna().drop_duplicates()
            records = df.to_dict('records')
            logging.info(f"🔎 Tìm thấy {len(records)} records.")

            batch_results = []
            for idx, row in enumerate(records):
                url = row['url']
                asin = row['asin']

                success = False
                retry_count = 0
                data = None

                while retry_count < 3:
                    logging.info(f"   🔹 [{idx + 1}/{len(records)}] URL: {url} | Lần {retry_count + 1}")

                    data = extract_product_breadcrumb(driver, url)

                    if data == "CAPTCHA":
                        logging.warning("⚠️ CAPTCHA! Restart driver...")
                        driver.quit()
                        time.sleep(2)
                        driver = get_chrome_driver()
                        time.sleep(2)
                        set_us_location(driver)
                        retry_count += 1
                    else:
                        success = True
                        break

                if data and data != "CAPTCHA":
                    # 🔥 GẮN ASIN TỪ CSV
                    data['asin'] = asin
                    batch_results.append(data)

                if len(batch_results) >= CHUNK_SIZE or idx == len(records) - 1:
                    if batch_results:
                        with open(full_json_path, 'a', encoding='utf-8') as f:
                            for item in batch_results:
                                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                        logging.info(f"💾 Đã ghi {len(batch_results)} items vào {save_name}")
                        batch_results = []

                time.sleep(random.uniform(3.0, 5.0))

            logging.info(f"✅ Hoàn tất danh mục: {manual_category}")
    finally:
        driver.quit()
        logging.info("🏁 QUY TRÌNH HOÀN TẤT.")

if __name__ == "__main__":
    SOURCE_DIR = "data_integrated"

    categories_dict = {}
    for file in os.listdir(SOURCE_DIR):
        if file.endswith(".csv"):
            name = os.path.splitext(file)[0]
            categories_dict[name] = file

    process_amazon_breadcum(SOURCE_DIR, categories_dict)
