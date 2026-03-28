import time
import os
import logging
import re
import pandas as pd
from selenium import webdriver
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def init_logging():
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler("amazon_link_expander.log", encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

def get_chrome_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

def get_high_res_img(url):
    if not url: return url
    return re.sub(r'\._AC_.*_\.', '.', url)


def extract_asins_from_visible_dom(container, category_name):
    """Quét các phần tử có data-asin hiện có trong DOM, bao gồm cả swatch list"""
    items = {}

    # Tìm tất cả các phần tử có thuộc tính data-asin (bao gồm cả thẻ li trong swatch list)
    elements = container.find_elements(By.XPATH, ".//*[@data-asin]")

    for el in elements:
        asin = el.get_attribute("data-asin")

        # Chỉ lấy ASIN chuẩn 10 ký tự và chưa có trong dict tạm
        if asin and len(asin) == 10 and asin not in items:
            try:
                # 1. Thử tìm link sản phẩm truyền thống (/dp/)
                href = None
                try:
                    link_tag = el.find_element(By.XPATH, ".//a[contains(@href, '/dp/')]")
                    href = link_tag.get_attribute("href")
                except:
                    # Nếu không thấy (như trường hợp li swatch), tạo link từ ASIN
                    href = f"https://www.amazon.com/dp/{asin}"

                # 2. Thử tìm ảnh
                img_url = None
                try:
                    img_tag = el.find_element(By.TAG_NAME, "img")
                    src = img_tag.get_attribute("src")
                    if src and "media-amazon.com" in src:
                        img_url = get_high_res_img(src)
                except:
                    # Nếu không có ảnh trong thẻ này, để None hoặc xử lý sau
                    img_url = None

                # Làm sạch URL
                clean_url = href.split("/ref=")[0] if href else f"https://www.amazon.com/dp/{asin}"
                if clean_url.startswith("/"):
                    clean_url = "https://www.amazon.com" + clean_url

                items[asin] = {
                    'asin': asin,
                    'url': clean_url,
                    'image_url': img_url,
                    'category': category_name
                }
            except Exception as e:
                continue
    return items

def collect_related_links(driver, url, category_name):
    all_page_items = {}
    try:
        driver.get(url)
        if "captcha" in driver.current_url.lower() or "automated access" in driver.page_source.lower():
            return "CAPTCHA"
        wait = WebDriverWait(driver, 10)
        container = wait.until(EC.presence_of_element_located((By.ID, "dp-container")))
        final_items = extract_asins_from_visible_dom(container, category_name)
        all_page_items.update(final_items)
        asin_list = list(all_page_items.keys())
        logging.info(f"        ✅ Lấy được {len(asin_list)} ASIN: {', '.join(asin_list)}")
        return list(all_page_items.values())
    except Exception as e:
        logging.error(f"      ❌ Lỗi xử lý trang {url}: {str(e)[:100]}")
        return []

def save_and_clean(file_path, new_data_list):
    if not new_data_list: return
    df_new = pd.DataFrame(new_data_list)

    if os.path.exists(file_path) and os.stat(file_path).st_size > 0:
        df_old = pd.read_csv(file_path)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.drop_duplicates(subset=['asin'], keep='first', inplace=True)
    df_combined.to_csv(file_path, index=False, encoding='utf-8')
    logging.info(f"💾 CẬP NHẬT FILE: {os.path.basename(file_path)} | Tổng: {len(df_combined)} ASIN")


def expand_amazon_links(source_dir, categories_dict):
    init_logging()
    driver = get_chrome_driver()
    CHUNK_SIZE = 20

    try:
        for cat_name, file_name in categories_dict.items():
            if not file_name.lower().endswith('.csv'):
                file_name += '.csv'

            # CHỈNH SỬA: Lưu vào file đuôi _2.csv
            output_file_name = file_name.replace('.csv', '_2.csv')
            source_file_path = os.path.join(source_dir, file_name)
            output_file_path = os.path.join(source_dir, output_file_name)

            if not os.path.exists(source_file_path):
                logging.warning(f"⚠️ Bỏ qua {cat_name}: File {source_file_path} không tồn tại.")
                continue

            logging.info(f"📂 DANH MỤC: {cat_name.upper()} -> Output: {output_file_name}")
            df_original = pd.read_csv(source_file_path)
            urls = df_original['url'].dropna().unique().tolist()

            temp_items = []
            idx = 1160
            while idx < len(urls):
                url = urls[idx]
                logging.info(f"🔹 [{idx + 1}/{len(urls)}] Đang vét từ: {url}")

                results = collect_related_links(driver, url, cat_name)
                # CHỈNH SỬA: Xử lý CAPTCHA bằng cách khởi tạo lại driver và thử lại link hiện tại
                if results == "CAPTCHA":
                    logging.warning("🛑 Dính CAPTCHA! Đang khởi tạo lại driver...")
                    driver.quit()
                    time.sleep(3)
                    driver = get_chrome_driver()
                    idx += 1
                    continue
                if results:
                    temp_items.extend(results)
                if (idx + 1) % CHUNK_SIZE == 0 and temp_items:
                    save_and_clean(output_file_path, temp_items)
                    temp_items = []
                idx += 1
                time.sleep(1)
            if temp_items:
                save_and_clean(output_file_path, temp_items)
    finally:
        driver.quit()
        logging.info("🏁 HOÀN TẤT TOÀN BỘ.")

if __name__ == "__main__":
    SOURCE_FOLDER = 'data_amazon_links'

    TARGET_CATEGORIES = {
        # "Smartphone": "smartphone_products.csv",
        # "Desktop": "desktop_products.csv"
        # "CPU": "cpu_products.csv",
        # "PC": "pc_products.csv",
        # "GPU": "gpu_products.csv",
        # "Monitor": "monitor_products.csv"

        # "Computer": "computer_products.csv",
        # "Headphone": "headphone_products.csv",
        # "Laptop": "laptop_products.csv",
        "Tablets": "tablets_products.csv"
    }

    expand_amazon_links(SOURCE_FOLDER, TARGET_CATEGORIES)