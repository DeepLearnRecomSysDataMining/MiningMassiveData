import csv
import time
import os
import logging
import sys
import re
from selenium import webdriver
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def init(output_dir='data_amazon'):
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
        # for subdir in ["raw", "processed"]:
        #     os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)
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


def extract_product_data(driver):
    products = []
    logging.info("    [Bắt đầu trích xuất] Đang phân tích nội dung trang...")

    try:
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='listitem']")))

        # Cuộn trang để load ảnh
        logging.info("    [Cuộn trang] Đang kích hoạt Lazy load...")
        for i in range(1, 4):
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i / 3});")
            time.sleep(1.5)

        items = driver.find_elements(By.CSS_SELECTOR, "div[role='listitem'][data-asin]")
        logging.info(f"    [Xử lý] Tìm thấy {len(items)} thẻ sản phẩm.")

        for item in items:
            asin = item.get_attribute("data-asin")
            if not asin: continue

            try:
                # Tìm link
                link_tag = item.find_element(By.CSS_SELECTOR, "a.a-link-normal[href*='/dp/']")
                href = link_tag.get_attribute("href")
                clean_href = href.split("/ref=")[0] if "/ref=" in href else href

                # Tìm ảnh
                img_tag = item.find_element(By.CSS_SELECTOR, "img.s-image")
                img_url = get_high_res_img(img_tag.get_attribute("src"))

                products.append({'asin': asin, 'url': clean_href, 'image_url': img_url})
            except NoSuchElementException:
                logging.debug(f"        [Bỏ qua] ASIN {asin} là nội dung quảng cáo không chuẩn.")
                continue

        logging.info(f"    [Thành công] Đã lấy được {len(products)} item từ trang này.")
        return products

    except TimeoutException:
        logging.error("    [Lỗi Timeout] Không tìm thấy danh sách sản phẩm sau 15s.")
        return []


def scrape_category(base_url, max_pages=20):
    init()
    driver = get_chrome_driver()
    all_results = []

    try:
        current_url = base_url
        for p in range(1, max_pages + 1):
            logging.info(f"TRANG {p}: Đang tải dữ liệu...")
            logging.info(f"URL: {current_url}")

            driver.get(current_url)

            if "captcha" in driver.page_source.lower() or "automated access" in driver.page_source:
                logging.error("    [CHẶN BOT] Amazon đã phát hiện và chặn Captcha.")
                break

            data = extract_product_data(driver)
            if not data:
                logging.warning("    [Thông báo] Trang trống hoặc bị lỗi load.")
                break

            all_results.extend(data)

            # Tìm nút Next
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "a.s-pagination-next")
                current_url = next_btn.get_attribute("href")
                logging.info(f"    [Phân trang] Đã tìm thấy nút Next cho trang {p + 1}.")
                time.sleep(5)
            except NoSuchElementException:
                logging.info("    [Kết thúc] Đã tới trang cuối cùng hiển thị.")
                break
    finally:
        driver.quit()
        save_to_csv(all_results)


import pandas as pd


def save_to_csv(data):
    if not data:
        logging.warning("Không có dữ liệu mới để lưu.")
        return

    file_path = os.path.join('data_amazon', 'amazon_products_3.csv')
    fieldnames = ['asin', 'url', 'image_url']

    new_df = pd.DataFrame(data)
    new_df.drop_duplicates(subset=['asin'], keep='first', inplace=True)

    if os.path.exists(file_path) and os.stat(file_path).st_size > 0:
        logging.info(f"    [Xử lý tệp lớn] Đang lọc trùng lặp với dữ liệu cũ...")
        try:
            existing_asins = pd.read_csv(file_path, usecols=['asin'], dtype={'asin': str})['asin'].values
            existing_asins_set = set(existing_asins)
            # Lọc: Chỉ giữ lại những dòng mà ASIN chưa tồn tại trong file
            new_df = new_df[~new_df['asin'].isin(existing_asins_set)]
            del existing_asins_set
        except Exception as e:
            logging.error(f"    [Lỗi] Không thể đọc cột ASIN từ file cũ: {str(e)}")
    if new_df.empty:
        logging.info("    [Thông báo] Tất cả sản phẩm đều đã tồn tại trong file (sau khi lọc triệu dòng).")
        return

    # Bước 3: Ghi nối tiếp (mode='a')
    try:
        # header=False nếu file đã có dữ liệu để tránh lặp lại tiêu đề ở giữa file
        file_exists = os.path.exists(file_path) and os.stat(file_path).st_size > 0
        new_df.to_csv(file_path, mode='a', index=False, header=not file_exists, encoding='utf-8')

        logging.info(
            f"✅ THÀNH CÔNG: Đã nối thêm {len(new_df)} dòng mới vào tệp {len(new_df)} dòng.")
    except Exception as e:
        logging.error(f"❌ LỖI LƯU FILE: {str(e)}")


if __name__ == '__main__':
    # url = "https://www.amazon.com/s?k=smartphones"
    # list_url =[
    #     "https://www.amazon.com/s?k=desktop+computer",
    #     "https://www.amazon.com/s?k=electroinc+tablets",
    #     "https://www.amazon.com/s?k=laptops",
    #     "https://www.amazon.com/s?k=computers",
    #     "https://www.amazon.com/s?k=PCs",
    #     "https://www.amazon.com/s?k=Monitors",
    # ]
    # list_url = [
    #     "https://www.amazon.com/s?k=headphones"
    # ]
    list_url = [
        "https://www.amazon.com/s?i=computers&rh=n%3A1292115011&s=popularity-rank"
    ]
    for url in list_url:
        scrape_category(url, max_pages=200)