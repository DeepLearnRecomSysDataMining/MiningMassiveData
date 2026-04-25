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

def init(output_dir='data_amazon_metadata'):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    file_handler = logging.FileHandler("amazon_review_scraper_2.log", encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(file_handler)
    logging.root.addHandler(stream_handler)

    abs_path = os.path.abspath(output_dir)
    os.makedirs(abs_path, exist_ok=True)
    logging.info(f"🚀 Hệ thống khởi tạo. Thư mục lưu trữ: {abs_path}")
    return abs_path

def get_chrome_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--incognito")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    driver = uc.Chrome(options=options, version_main=146)
    driver.set_page_load_timeout(60)
    return driver

def set_us_location(driver, zip_code="10001"):
    try:
        logging.info(f"📍 Đang thiết lập vùng US (Zip: {zip_code})")
        driver.get("https://www.amazon.com")
        wait = WebDriverWait(driver, 15)
        loc_btn = wait.until(EC.element_to_be_clickable((By.ID, "nav-global-location-popover-link")))
        loc_btn.click()
        zip_input = wait.until(EC.presence_of_element_located((By.ID, "GLUXZipUpdateInput")))
        zip_input.send_keys(zip_code)
        driver.find_element(By.ID, "GLUXZipUpdate").click()
        time.sleep(2)
        driver.refresh()
        logging.info("✅ Đã chuyển vùng thành công.")
    except Exception as e:
        logging.warning(f"⚠️ Không thể đổi vùng: {str(e)}")

def safe_get_text(parent, selector):
    try:
        return parent.find_element(By.CSS_SELECTOR, selector).get_attribute("innerText").strip()
    except:
        return None

def extract_reviews(driver, url, main_category):
    flat_reviews = []
    try:
        driver.get(url)
        if "captcha" in driver.current_url.lower() or "automated access" in driver.page_source:
            return "CAPTCHA"
        wait = WebDriverWait(driver, 15)

        asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
        current_asin = asin_match.group(1) if asin_match else "Unknown"

        logging.info(f"   🔎 Đang phân tích ASIN: {current_asin}")

        # Cuộn trang dần dần xuống khu vực Review (Thường ở giữa/cuối trang)
        for part in [0.3, 0.6, 0.8]:
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {part});")
            time.sleep(1)

        # Nhắm trực tiếp vào container review ngang hàng với dp-container
        try:
            # Chờ widget review xuất hiện
            review_container = wait.until(EC.presence_of_element_located((By.ID, "customer-reviews_feature_div")))
        except:
            logging.warning(f"      [!] Không thấy widget review cho {current_asin}. Có thể sp chưa có đánh giá.")
            return []

        # Tìm các thẻ review đơn lẻ (li.review)
        cards = review_container.find_elements(By.CSS_SELECTOR, "li[data-hook='review']")
        logging.info(f"      📊 Tìm thấy {len(cards)} thẻ review trên trang.")

        for card in cards:
            try:
                # 1. Thông tin User
                author = safe_get_text(card, ".a-profile-name")
                user_id = "Anonymous"
                try:
                    p_link = card.find_element(By.CSS_SELECTOR, "a.a-profile").get_attribute("href")
                    if "/profile/" in p_link:
                        user_id = p_link.split("/profile/")[1].split("/")[0]
                except:
                    pass

                # 2. Rating & Title
                raw_rating = safe_get_text(card, "i[data-hook='review-star-rating'] span.a-icon-alt")
                rating_val = float(raw_rating.split()[0]) if raw_rating else 0.0

                title = safe_get_text(card, "a[data-hook='review-title'] span:last-child") or ""

                # 3. Nội dung & Ngày tháng
                date_str = safe_get_text(card, "span[data-hook='review-date']")
                content = safe_get_text(card, "span[data-hook='review-body'] span")

                # 4. Kiểm tra Verified Purchase
                is_verified = False
                try:
                    if card.find_elements(By.CSS_SELECTOR, "span[data-hook='avp-badge']"):
                        is_verified = True
                except:
                    pass

                # 5. Helpful Vote
                helpful_text = safe_get_text(card, "span[data-hook='helpful-vote-statement']")
                helpful_count = 0
                if helpful_text:
                    nums = re.findall(r'\d+', helpful_text.replace(',', ''))
                    helpful_count = int(nums[0]) if nums else 0

                # Tạo object "Phẳng" chuẩn Amazon Dataset
                flat_reviews.append({
                    "rating": rating_val,
                    "title": title,
                    "text": content,
                    "asin": current_asin,
                    "parent_asin": current_asin,  # Thường lấy từ metadata, tạm để bằng asin
                    "user_id": user_id,
                    "user_name": author,
                    "timestamp_raw": date_str,
                    "helpful_vote": helpful_count,
                    "verified_purchase": is_verified,
                    "main_category": main_category,
                    "images": []  # Có thể mở rộng lấy link ảnh trong review ở đây
                })
            except Exception as e:
                logging.debug(f"         [!] Bỏ qua 1 card lỗi: {str(e)}")
                continue

        return flat_reviews

    except Exception as e:
        logging.error(f"      ❌ Lỗi nghiêm trọng tại {url}: {str(e)}")
        return None

def process_amazon_metadata(source_dir, categories_dict):
    init(source_dir)
    jsonl_path = os.path.abspath('reviews')
    os.makedirs(jsonl_path, exist_ok=True)

    driver = get_chrome_driver()
    set_us_location(driver)

    try:
        for manual_category, file_name in categories_dict.items():
            full_csv_path = os.path.join(source_dir, file_name)
            if not os.path.exists(full_csv_path):
                logging.warning(f"⚠️ Không thấy file {file_name}. Bỏ qua.")
                continue

            save_file = f"{manual_category.replace(' ', '_').lower()}_reviews.jsonl"
            full_json_path = os.path.join(jsonl_path, save_file)

            logging.info(f"📂 ĐANG XỬ LÝ: {manual_category} | File: {file_name}")

            df = pd.read_csv(full_csv_path, low_memory=False)
            urls = df['url'].dropna().unique().tolist()

            # Bạn có thể dùng slice để test: urls[:10]
            for idx, url in enumerate(urls):
                # Ví dụ bạn muốn resume từ vị trí nhất định

                retry_count = 0
                data_list = None

                while retry_count < 2:
                    logging.info(f"   🔹 [{idx + 1}/{len(urls)}] URL: {url} (Lần {retry_count + 1})")
                    data_list = extract_reviews(driver, url, manual_category)

                    if data_list == "CAPTCHA":
                        logging.warning("🛑 Dính CAPTCHA! Đang thay đổi trình duyệt...")
                        driver.quit()
                        time.sleep(random.uniform(10, 20))
                        driver = get_chrome_driver()
                        set_us_location(driver)
                        retry_count += 1
                    else:
                        break

                if isinstance(data_list, list) and len(data_list) > 0:
                    # Ghi ngay xuống file JSONL (mỗi review là 1 dòng)
                    with open(full_json_path, 'a', encoding='utf-8') as f:
                        for review in data_list:
                            f.write(json.dumps(review, ensure_ascii=False) + "\n")
                    logging.info(f"      💾 Đã ghi +{len(data_list)} reviews vào file.")
                # Delay an toàn để tránh bị ban
                time.sleep(random.uniform(2.0, 4.0))
            logging.info(f"✅ Hoàn tất danh mục: {manual_category}")
    finally:
        driver.quit()
        logging.info("🏁 QUY TRÌNH HOÀN TẤT.")


if __name__ == "__main__":
    # Cấu hình danh mục và file csv link tương ứng
    TARGET_CATEGORIES = {
        # "Computer": "computer_products.csv",
        # "CPU": "cpu_products.csv",
        # "Desktop": "desktop_products.csv",
        # "GPU": "gpu_products.csv",
        # "Headphone": "headphone_products.csv",
        # "Laptop": "laptop_products.csv",
        # "Monitor": "monitor_products.csv",
        # "PC": "pc_products.csv",
        # "Smartphone": "smartphone_products.csv",
        # "Tablets": "tablets_products.csv",

        # "Television": "television_products.csv",

        # "Computer": "computer_products_2.csv",
        # "CPU": "cpu_products_2.csv",
        "Desktop": "desktop_products_2.csv",
        "GPU": "gpu_products_2.csv",
        "Headphone": "headphone_products_2.csv",
        "Laptop": "laptop_products_2.csv",
        "Monitor": "monitor_products_2.csv",
        # "PC": "pc_products_2.csv",
        # "Smartphone": "smartphone_products_2.csv",
        # "Tablets": "tablets_products_2.csv",
        # "Television": "television_products_2.csv",
    }

    # Thư mục chứa các file csv link ban đầu
    process_amazon_metadata('data_amazon_links', TARGET_CATEGORIES)