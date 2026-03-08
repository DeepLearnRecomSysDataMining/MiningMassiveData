import json
import time
import os
import logging
import re
import pandas as pd
import glob
from selenium import webdriver
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_chrome_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

def safe_get_text(parent, selector, attr="innerText"):
    try:
        element = parent.find_element(By.CSS_SELECTOR, selector)
        return element.get_attribute(attr).strip()
    except NoSuchElementException:
        return None

def extract_product_details(driver, url):
    try:
        # Kiểm tra Captcha nhanh qua tiêu đề hoặc nội dung
        if "captcha" in driver.page_source.lower() or "automated access" in driver.page_source:
            return "CAPTCHA"

        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.ID, "productTitle")))

        title = safe_get_text(driver, "#productTitle")
        price = safe_get_text(driver, "span.a-price span.a-offscreen")
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
        asin = asin_match.group(1) if asin_match else "Unknown"

        # Lấy ảnh HD (loại bỏ phần resize của Amazon)
        img_element = driver.find_element(By.ID, "landingImage")
        img_url = img_element.get_attribute("src")
        hi_res_img = re.sub(r'\._AC_.*_\.', '.', img_url)

        specs = {}
        # Ưu tiên lấy từ bảng 'Technical Details' (Cấu trúc bảng)
        try:
            # Selector bao quát nhiều loại bảng thông số trên Amazon
            spec_tables = driver.find_elements(By.CSS_SELECTOR,
                                               "table.prodDetTable, #productDetails_techSpec_section_1, #technical-details-table")
            for table in spec_tables:
                rows = table.find_elements(By.TAG_NAME, "tr")
                for row in rows:
                    key = safe_get_text(row, "th")
                    val = safe_get_text(row, "td")
                    if key and val:
                        specs[key.strip()] = val.strip()
        except:
            pass

        # Nếu vẫn trống, lấy từ 'Product Overview' (Dạng lưới phía trên)
        if not specs:
            try:
                rows = driver.find_elements(By.CSS_SELECTOR, ".po-table tr, .v-expander-content table tr")
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 2:
                        specs[cells[0].text.strip()] = cells[1].text.strip()
            except:
                pass

        return {
            "asin": asin,
            "title": title,
            "price": price,
            "image_url": hi_res_img,
            "url": url,
            "specifications": specs,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logging.error(f"    [Lỗi] Không thể trích xuất {url}: {str(e)}")
        return None


def get_processed_urls(json_path):
    processed = set()
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'url' in data:
                        processed.add(data['url'])
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
    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))
    if not csv_files:
        logging.error(f"Không tìm thấy file CSV nào trong {source_dir}")
        return

    driver = get_chrome_driver()
    batch_size = 20

    try:
        for csv_file in csv_files:
            file_name = os.path.basename(csv_file)
            json_name = file_name.replace(".csv", "_details.jsonl")
            json_path = os.path.join(source_dir, json_name)

            logging.info(f"=== BẮT ĐẦU FILE: {file_name} ===")

            try:
                df = pd.read_csv(csv_file)
            except Exception as e:
                logging.error(f"    [Lỗi] Không thể đọc {file_name}: {e}")
                continue

            if 'url' not in df.columns:
                logging.warning(f"    [Bỏ qua] Cột 'url' không tồn tại.")
                continue

            all_urls = df['url'].dropna().unique().tolist()
            processed_urls = get_processed_urls(json_path)
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
                            save_batch_to_jsonl(batch_results, json_path)
                            return
                        if product_data:
                            batch_results.append(product_data)
                    except Exception as e:
                        logging.error(f"        [Lỗi Duyệt] {url}: {e}")
                    time.sleep(4)  # Delay an toàn chống bot
                if batch_results:
                    save_batch_to_jsonl(batch_results, json_path)
                    logging.info(f"    <<< ĐÃ LƯU NỐI ĐUÔI {len(batch_results)} ITEM VÀO {json_name}")
    finally:
        driver.quit()
        logging.info("=== KẾT THÚC QUÁ TRÌNH ===")

if __name__ == "__main__":
    process_csv_files('data_amazon_split')