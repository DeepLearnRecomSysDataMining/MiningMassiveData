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
    # Thiết lập logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("amazon_ids_scraper.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    try:
        os.makedirs(output_dir, exist_ok=True)
        # for subdir in ["raw", "processed"]:
        #     os.makedirs(os.path.join(output_dir, subdir), exist_ok=True)
    except Exception as e:
        logging.error(f"Lỗi khi tạo thư mục: {str(e)}")
        sys.exit(1)


def get_chrome_driver():
    """Khởi tạo Chrome driver với các biện pháp tránh bị phát hiện"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def get_high_res_img(url):
    """Xử lý Regex để lấy link ảnh chất lượng cao nhất từ Amazon"""
    if not url: return url
    return re.sub(r'\._AC_.*_\.', '.', url)


def extract_product_data(driver):
    """Trích xuất ASIN, Link sản phẩm và Link Image từ các thẻ listitem"""
    products = []
    try:
        logging.info("    [Đang đợi phần tử] Tìm kiếm thẻ div[role='listitem']...")
        wait = WebDriverWait(driver, 20)

        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='listitem']")))
            logging.info("    [Thành công] Đã tìm thấy danh sách sản phẩm.")
        except TimeoutException:
            logging.error(
                "    [Lỗi Timeout] Quá thời gian chờ nhưng không tìm thấy 'listitem'. Có thể trang load lỗi hoặc bị Captcha.")
            return []

        # Cuộn trang để kích hoạt lazy load ảnh
        logging.info("    [Cuộn trang] Đang cuộn để load dữ liệu ẩn (Lazy load)...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        # Lấy tất cả listitem có chứa ASIN
        items = driver.find_elements(By.CSS_SELECTOR, "div[role='listitem'][data-asin]")
        logging.info(f"    [Xử lý] Tìm thấy {len(items)} thẻ chứa ASIN tiềm năng.")

        for item in items:
            asin = item.get_attribute("data-asin")
            if not asin:
                continue

            try:
                # 1. Tìm Link sản phẩm
                link_tag = item.find_element(By.CSS_SELECTOR, "a.a-link-normal[href*='/dp/']")
                href = link_tag.get_attribute("href")
                clean_href = href.split("/ref=")[0] if "/ref=" in href else href
                full_url = "https://www.amazon.com" + clean_href if clean_href.startswith('/') else clean_href

                # 2. Tìm Link Ảnh
                img_tag = item.find_element(By.CSS_SELECTOR, "img.s-image")
                img_url = img_tag.get_attribute("src")
                img_url_hd = get_high_res_img(img_url)

                products.append({
                    'asin': asin,
                    'url': full_url,
                    'image_url': img_url_hd
                })
            except NoSuchElementException:
                logging.debug(
                    f"        [Bỏ qua] Sản phẩm ASIN {asin} không có cấu trúc link/ảnh chuẩn (có thể là quảng cáo).")
                continue

        # Loại bỏ trùng lặp trong nội bộ trang (bằng ASIN)
        unique_products = list({v['asin']: v for v in products}.values())
        logging.info(f"    [Hoàn tất] Trích xuất thành công {len(unique_products)} sản phẩm (Đã loại trùng).")
        return unique_products

    except Exception as e:
        logging.error(f"    [Lỗi hệ thống] Lỗi không xác định trong quá trình trích xuất: {str(e)}")
        return []


def scrape_pages(category_url, start_page, end_page):
    init()
    all_data = []
    driver = get_chrome_driver()
    logging.info(f"BẮT ĐẦU CẠO DỮ LIỆU TỪ TRANG {start_page} ĐẾN {end_page}")

    try:
        for page in range(start_page, end_page + 1):
            url = f"{category_url}&page={page}"
            logging.info(f"--- Đang truy cập trang {page} ---")
            logging.info(f"URL: {url}")

            try:
                driver.get(url)
            except Exception as e:
                logging.error(f"    [Lỗi Kết Nối] Không thể tải trang {page}: {str(e)}")
                continue

            # Kiểm tra Captcha
            if "To discuss automated access" in driver.page_source or "captcha" in driver.current_url.lower():
                logging.error("!!! [CẢNH BÁO] TRÌNH DUYỆT BỊ CHẶN BỞI CAPTCHA !!! Dừng quá trình cạo.")
                break

            page_products = extract_product_data(driver)

            if not page_products:
                logging.warning(f"Trang {page} trả về danh sách rỗng. Có thể đã hết trang hoặc bị chặn ẩn.")
                break

            all_data.extend(page_products)
            logging.info(f"Đã thu thập tổng cộng {len(all_data)} sản phẩm sau trang {page}.")

            # Delay ngẫu nhiên một chút để giả lập người dùng thật
            time.sleep(5)

    finally:
        logging.info("Đang đóng trình duyệt và lưu dữ liệu...")
        driver.quit()
        save_to_csv(all_data)


def save_to_csv(data, filename="amazon_product_links.csv"):
    if not data:
        logging.warning("Không có dữ liệu nào để lưu.")
        return

    # Loại bỏ trùng lặp cuối cùng dựa trên ASIN
    unique_data = list({v['asin']: v for v in data}.values())
    file_path = os.path.join('data_amazon', filename)
    fieldnames = ['asin', 'url', 'image_url']

    try:
        file_exists = os.path.exists(file_path)
        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerows(unique_data)
        logging.info(f"SUCCESS: THÀNH CÔNG: Đã lưu {len(unique_data)} sản phẩm vào: {file_path}")
    except Exception as e:
        logging.error(f"ERROR: LỖI LƯU FILE: {str(e)}")

if __name__ == '__main__':
    # Link danh mục mục tiêu
    target_category = "https://www.amazon.com/s?k=iphone"
    # Chạy thử nghiệm
    scrape_pages(target_category, 1, 20)