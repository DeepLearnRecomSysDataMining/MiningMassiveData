import csv
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

def init(output_dir='data_amazon_split'):
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("amazon_deep_scraper.log", encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        logging.error(f"Lỗi khởi tạo: {str(e)}")
        sys.exit(1)

def get_chrome_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    return driver

def get_high_res_img(url):
    if not url: return url
    return re.sub(r'\._AC_.*_\.', '.', url)

def extract_product_data(driver, category_name):
    products = []
    try:
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-asin]")))
        for i in range(1, 4):
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i / 3});")
            time.sleep(1.5)
        items = driver.find_elements(By.CSS_SELECTOR, "div[data-asin]")

        for item in items:
            asin = item.get_attribute("data-asin")
            if not asin:
                continue
            try:
                link_tag = item.find_element(By.CSS_SELECTOR, "a.a-link-normal[href*='/dp/']")
                href = link_tag.get_attribute("href")
                clean_href = href.split("/ref=")[0] if "/ref=" in href else href
                img_tag = item.find_element(By.CSS_SELECTOR, "img.s-image")
                img_url = get_high_res_img(img_tag.get_attribute("src"))
                products.append({
                    'asin': asin,
                    'url': clean_href,
                    'image_url': img_url,
                    'category': category_name
                })
            except NoSuchElementException:
                continue
        unique_on_page = list({v['asin']: v for v in products}.values())
        logging.info(f"    Đã lấy được thêm {len(unique_on_page)} ASIN")
        return unique_on_page
    except TimeoutException:
        logging.error("    [Lỗi Timeout] Không tìm thấy danh sách sản phẩm.")
        return []

def save_to_csv(data, category_name, output_dir):
    if not data:
        return
    safe_name = category_name.replace(' ', '_').lower()
    file_path = os.path.join(output_dir, f"{safe_name}_products.csv")

    fieldnames = ['asin', 'url', 'image_url', 'category']
    new_df = pd.DataFrame(data, columns=fieldnames)
    new_df.drop_duplicates(subset=['asin'], keep='first', inplace=True)

    if os.path.exists(file_path) and os.stat(file_path).st_size > 0:
        try:
            # Chỉ đọc cột ASIN để tiết kiệm RAM
            existing_asins = pd.read_csv(file_path, usecols=['asin'], dtype={'asin': str})['asin'].unique()
            existing_set = set(existing_asins)
            # Chỉ giữ lại những dòng có ASIN chưa từng xuất hiện trong file
            new_df = new_df[~new_df['asin'].isin(existing_set)]
        except Exception as e:
            logging.error(f"    [Lỗi lọc trùng] {str(e)}")

    if new_df.empty:
        logging.info(f"    [Thông báo] Không có sản phẩm mới nào cho danh mục {category_name}.")
        return
    try:
        file_exists = os.path.exists(file_path) and os.stat(file_path).st_size > 0
        new_df.to_csv(file_path, mode='a', index=False, header=not file_exists, encoding='utf-8')
        logging.info(f"✅ Đã lưu {len(new_df)} sản phẩm mới vào {file_path}")
    except Exception as e:
        logging.error(f"❌ LỖI GHI FILE: {str(e)}")

def scrape_category(category_name, url_list, max_pages_per_url=20):
    init(output_dir='data_amazon_links')
    driver = get_chrome_driver()
    output_dir = 'data_amazon_links'

    try:
        for base_url in url_list:
            current_url = base_url
            logging.info(f"🚀 BẮT ĐẦU: Danh mục {category_name.upper()}")

            for p in range(1, max_pages_per_url + 1):
                logging.info(f"  Trang {p} | URL: {current_url}")
                driver.get(current_url)
                if "captcha" in driver.page_source.lower():
                    logging.error("  [CHẶN] Bị dính Captcha.")
                    break
                page_data = extract_product_data(driver, category_name)
                if not page_data:
                    break
                save_to_csv(page_data, category_name, output_dir)

                try:
                    next_btn = driver.find_element(By.CSS_SELECTOR, "a.s-pagination-next")
                    current_url = next_btn.get_attribute("href")
                    time.sleep(5)
                except NoSuchElementException:
                    logging.info("  [Hết trang] Không thấy nút Next.")
                    break
    finally:
        driver.quit()

if __name__ == '__main__':
    categories_config = {
        "Smartphone": [
            # "https://www.amazon.com/s?k=iphone",
            # "https://www.amazon.com/s?k=phones",
            # "https://www.amazon.com/s?k=smartphones",
            # "https://www.amazon.com/s?k=smartphone+samsung",
            "https://www.amazon.com/s?k=samsung+z+fold",
            "https://www.amazon.com/s?k=smartphones+motorola",
            "https://www.amazon.com/s?k=smartphone+apple",
            "https://www.amazon.com/s?k=smartphones+xiaomi",
            "https://www.amazon.com/s?k=blackberry+smartphones",
            "https://www.amazon.com/s?k=smartphones+redmi",
            "https://www.amazon.com/s?k=smartphones+realme",
            "https://www.amazon.com/s?k=smartphones+oppo",
            "https://www.amazon.com/s?k=vivo+smartphones"
        ],
        "Tablets": [
            "https://www.amazon.com/s?k=tablets",
            "https://www.amazon.com/s?k=tablets+samsung",
            "https://www.amazon.com/s?k=tablets+apple",
            "https://www.amazon.com/s?k=tablets+amazon",
            "https://www.amazon.com/s?k=tablets+lenovo",
            "https://www.amazon.com/s?k=tablets+microsoft",
            "https://www.amazon.com/s?k=tablets+google",
            "https://www.amazon.com/s?k=tablets+asus",
            "https://www.amazon.com/s?k=tablets+hp",
            "https://www.amazon.com/s?k=tablets+dell",
            "https://www.amazon.com/s?k=tablets+xiaomi",
            "https://www.amazon.com/s?k=tablets+acer",
            "https://www.amazon.com/s?k=tablets+lg",
            "https://www.amazon.com/s?k=tablets+sony",
            "https://www.amazon.com/s?k=tablets+msi",
            "https://www.amazon.com/s?k=ipad"
        ],
        "Headphone": [
            "https://www.amazon.com/s?k=headphones",
            "https://www.amazon.com/s?k=earphones",
            "https://www.amazon.com/s?k=earphones+wireless",
            "https://www.amazon.com/s?k=airpods"
        ],
        "Laptop": [
            "https://www.amazon.com/s?k=laptops",
            "https://www.amazon.com/s?k=laptops+hp",
            "https://www.amazon.com/s?k=laptops+dell",
            "https://www.amazon.com/s?k=laptops+lenovo",
            "https://www.amazon.com/s?k=surface+laptop",
            "https://www.amazon.com/s?k=macbook+apple",
            "https://www.amazon.com/s?k=laptops+asus",
            "https://www.amazon.com/s?k=laptops+acer",
            "https://www.amazon.com/s?k=laptops+samsung",
            "https://www.amazon.com/s?k=laptops+microsoft",
            "https://www.amazon.com/s?k=laptops+msi",
            "https://www.amazon.com/s?k=laptops+LG"
        ],
        "Computer": [
            "https://www.amazon.com/s?k=computers+hp",
            "https://www.amazon.com/s?k=computers+dell",
            "https://www.amazon.com/s?k=computers+lenovo",
            "https://www.amazon.com/s?k=computers+asus",
            "https://www.amazon.com/s?k=computers+acer",
            "https://www.amazon.com/s?k=computers+microsoft",
            "https://www.amazon.com/s?k=computers+samsung",
            "https://www.amazon.com/s?k=computers+msi",
            "https://www.amazon.com/s?k=computers+razer",
            "https://www.amazon.com/s?k=computers+lg",
            "https://www.amazon.com/s?k=computers+gigabyte",
            "https://www.amazon.com/s?k=computers+panasonic",
            "https://www.amazon.com/s?k=computers+fujitsu",
            "https://www.amazon.com/s?k=computers+intel"
        ],
        "Desktop": [
            "https://www.amazon.com/s?k=desktop+computer",
            "https://www.amazon.com/s?k=desktop+computer+dell",
            "https://www.amazon.com/s?k=desktop+computer+hp",
            "https://www.amazon.com/s?k=desktop+computer+lenovo",
            "https://www.amazon.com/s?k=desktop+computer+apple",
            "https://www.amazon.com/s?k=desktop+computer+asus",
            "https://www.amazon.com/s?k=desktop+computer+acer",
            "https://www.amazon.com/s?k=desktop+computer+msi",
            "https://www.amazon.com/s?k=desktop+computer+intel"
        ],
        "CPU": [
            "https://www.amazon.com/s?k=cpu",
            "https://www.amazon.com/s?k=cpu+amd",
            "https://www.amazon.com/s?k=cpu+intel",
            "https://www.amazon.com/s?k=cpu+apple",
            "https://www.amazon.com/s?k=cpu+samsung",
            "https://www.amazon.com/s?k=cpu+seagate",
            "https://www.amazon.com/s?k=cpu+dell",
            "https://www.amazon.com/s?k=cpu+hp",
            "https://www.amazon.com/s?k=cpu+asus",
            "https://www.amazon.com/s?k=cpu+lenovo",
            "https://www.amazon.com/s?k=cpu+corsair",
            "https://www.amazon.com/s?k=cpu+msi",
            "https://www.amazon.com/s?k=cpu+thermalright",
            "https://www.amazon.com/s?k=cpu+acer",
            "https://www.amazon.com/s?k=cpu+gigabyte",
            "https://www.amazon.com/s?k=cpu+ibm",
            "https://www.amazon.com/s?k=cpu+razer"
        ],
        "PC": [
            "https://www.amazon.com/s?k=PCs",
            "https://www.amazon.com/s?k=PCs+dell",
            "https://www.amazon.com/s?k=PCs+hp",
            "https://www.amazon.com/s?k=PCs+lenovo",
            "https://www.amazon.com/s?k=PCs+asus",
            "https://www.amazon.com/s?k=PCs+apple",
            "https://www.amazon.com/s?k=PCs+msi",
            "https://www.amazon.com/s?k=PCs+acer",
            "https://www.amazon.com/s?k=PCs+cyberpowerpc",
            "https://www.amazon.com/s?k=PCs+intel",
            "https://www.amazon.com/s?k=PCs+gigabyte"
        ],
        "GPU": [
            "https://www.amazon.com/s?k=gpu",
            "https://www.amazon.com/s?k=gpu+asus",
            "https://www.amazon.com/s?k=gpu+msi",
            "https://www.amazon.com/s?k=gpu+gigabyte",
            "https://www.amazon.com/s?k=gpu+amd",
            "https://www.amazon.com/s?k=gpu+intel",
            "https://www.amazon.com/s?k=gpu+hp",
            "https://www.amazon.com/s?k=gpu+nvidia"
        ],
        "Monitor": [
            "https://www.amazon.com/s?k=monitor",
            "https://www.amazon.com/s?k=monitor+dell",
            "https://www.amazon.com/s?k=monitor+ktc",
            "https://www.amazon.com/s?k=monitor+samsung",
            "https://www.amazon.com/s?k=monitor+asus",
            "https://www.amazon.com/s?k=monitor+lg",
            "https://www.amazon.com/s?k=monitor+acer",
            "https://www.amazon.com/s?k=monitor+hp",
            "https://www.amazon.com/s?k=monitor+msi",
            "https://www.amazon.com/s?k=monitor+benq",
            "https://www.amazon.com/s?k=monitor+sceptre",
            "https://www.amazon.com/s?k=monitor+aoc",
            "https://www.amazon.com/s?k=monitor+lenovo",
            "https://www.amazon.com/s?k=monitor+viewsonic",
            "https://www.amazon.com/s?k=monitor+gigabyte",
            "https://www.amazon.com/s?k=monitor+sony",
            "https://www.amazon.com/s?k=monitor+xiaomi",
            "https://www.amazon.com/s?k=monitor+philips"
        ]
    }

    # Chạy vòng lặp cho từng category
    for cat_name, urls in categories_config.items():
        scrape_category(cat_name, urls, max_pages_per_url=10)