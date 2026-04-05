import json
import time
import os
import logging
import sys
import re
import glob
import pandas as pd
from selenium import webdriver
from selenium.common import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
def init(output_dir='data_amazon_split'):
    # Đảm bảo log hiển thị sạch sẽ trên Console
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("amazon_review_scraper.log", encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    abs_path = os.path.abspath(output_dir)
    os.makedirs(abs_path, exist_ok=True)
    return abs_path


def get_interactive_driver():
    options = Options()
    # Mở trình duyệt có giao diện để đăng nhập
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Tùy chọn: Thêm profile để ghi nhớ đăng nhập cho lần sau
    # options.add_argument(f"user-data-dir={os.path.join(os.getcwd(), 'amazon_user_data')}")

    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(60)
        return driver
    except Exception as e:
        logging.error(f"Lỗi khởi tạo Driver: {e}")
        sys.exit(1)


# ==========================================
# 2. HÀM TRÍCH XUẤT DỮ LIỆU (DỰA TRÊN HTML BẠN GỬI)
# ==========================================
def safe_find_text(parent, selector):
    try:
        return parent.find_element(By.CSS_SELECTOR, selector).get_attribute("innerText").strip()
    except NoSuchElementException:
        return None


def extract_reviews_from_page(driver):
    """Trích xuất chi tiết từng <li> review dựa trên HTML cụ thể của bạn"""
    reviews_list = []
    try:
        # Đợi danh sách review xuất hiện
        wait = WebDriverWait(driver, 15)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "li[data-hook='review']")))

        # Tìm tất cả thẻ li review
        review_elements = driver.find_elements(By.CSS_SELECTOR, "li[data-hook='review']")

        for item in review_elements:
            try:
                # Trích xuất dữ liệu dựa trên cấu trúc bạn cung cấp
                author = safe_find_text(item, "span.a-profile-name")
                rating = safe_find_text(item, "i[data-hook='review-star-rating'] span.a-icon-alt")
                # Tiêu đề thường nằm trong span thứ 2 của link review-title
                title = safe_find_text(item, "a[data-hook='review-title'] span:not(.a-icon-alt)")
                date = safe_find_text(item, "span[data-hook='review-date']")
                content = safe_find_text(item, "span[data-hook='review-body'] span")

                # Cấu hình sản phẩm (Ví dụ: Size, Color...)
                format_strip = safe_find_text(item, "a[data-hook='format-strip']")

                # Kiểm tra Verified Purchase
                is_verified = len(item.find_elements(By.CSS_SELECTOR, "span[data-hook='avp-badge']")) > 0

                # Lấy ảnh đính kèm của người dùng (nếu có)
                images = []
                img_els = item.find_elements(By.CSS_SELECTOR, "img.review-image-tile")
                for img in img_els:
                    thumb_url = img.get_attribute("src")
                    if thumb_url:
                        # Chuyển đổi sang link ảnh gốc chất lượng cao
                        full_url = re.sub(r'\._SY88\._', '.', thumb_url)
                        images.append(full_url)

                reviews_list.append({
                    "author": author,
                    "rating": rating,
                    "title": title,
                    "date": date,
                    "verified": is_verified,
                    "format": format_strip,
                    "content": content,
                    "images": images
                })
            except Exception:
                continue
        return reviews_list
    except TimeoutException:
        return []


# ==========================================
# 3. QUY TRÌNH XỬ LÝ CHÍNH
# ==========================================
def get_processed_asins(json_path):
    processed = set()
    if os.path.exists(json_path):
        with open(json_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    processed.add(data['asin'])
                except:
                    continue
    return processed


def run_scraper(source_dir):
    abs_dir = init(source_dir)
    # Tìm tất cả file csv có tên amazon_batch_...
    csv_files = [f for f in os.listdir(abs_dir) if f.endswith('.csv') and 'amazon_batch_' in f]
    csv_files.sort(key=lambda f: int(re.findall(r'\d+', f)[0]))

    driver = get_interactive_driver()

    try:
        # --- BƯỚC ĐĂNG NHẬP ---
        logging.info(">>> Đang mở Amazon. Vui lòng đăng nhập tài khoản...")
        driver.get(
            "https://www.amazon.com/ap/signin?openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=usflex&openid.mode=checkid_setup&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0")

        print("\n" + "!" * 60)
        print("HÀNH ĐỘNG: Đăng nhập xong, vào tới trang chủ rồi hãy nhấn ENTER ở đây.")
        print("!" * 60 + "\n")
        input("Nhấn [ENTER] để bắt đầu quá trình cạo Review tự động...")

        for file_name in csv_files[0:10]:
            logging.info(f"=== ĐANG XỬ LÝ FILE: {file_name} ===")
            full_csv_path = os.path.join(abs_dir, file_name)
            json_path = os.path.join(abs_dir, file_name.replace(".csv", "_reviews.jsonl"))

            df = pd.read_csv(full_csv_path)
            if 'asin' not in df.columns:
                logging.error(f"    Bỏ qua {file_name}: Không tìm thấy cột 'asin'")
                continue

            asins = df['asin'].dropna().unique().tolist()
            processed_asins = get_processed_asins(json_path)
            asins_to_scrape = [a for a in asins if a not in processed_asins]

            logging.info(
                f"    Tổng: {len(asins)} | Đã xong: {len(processed_asins)} | Cần làm: {len(asins_to_scrape)}")

            for idx, asin in enumerate(asins_to_scrape):
                # Nhảy thẳng đến link review
                review_url = f"https://www.amazon.com/product-reviews/{asin}"
                logging.info(f"    [{idx + 1}/{len(asins_to_scrape)}] Đang lấy review cho ASIN: {asin}")

                try:
                    driver.get(review_url)
                    time.sleep(3)  # Chờ load và tránh bot

                    # Kiểm tra nếu bị Logout hoặc Captcha
                    if "signin" in driver.current_url or "captcha" in driver.page_source.lower():
                        logging.warning("    PHÁT HIỆN CHẶN/LOGOUT. Vui lòng xử lý trên trình duyệt.")
                        input("Nhấn [ENTER] sau khi đã xử lý xong để chạy tiếp...")
                        driver.get(review_url)

                    # Trích xuất dữ liệu trang 1
                    reviews = extract_reviews_from_page(driver)

                    # Lưu nối đuôi vào tệp JSONL của Batch đó
                    if reviews:
                        with open('data_reviews/reviews.jsonl', 'a', encoding='utf-8') as f:
                            result_entry = {
                                "asin": asin,
                                "total_on_page": len(reviews),
                                "reviews": reviews,
                                "scrape_date": time.strftime("%Y-%m-%d")
                            }
                            f.write(json.dumps(result_entry, ensure_ascii=False) + "\n")
                        logging.info(f"      Đã lưu {len(reviews)} review.")
                    else:
                        logging.info(f"      ASIN này hiện không có review hiển thị.")

                except Exception as ex:
                    logging.error(f"      Lỗi tại ASIN {asin}: {ex}")

                time.sleep(2)  # Nghỉ giữa các sản phẩm

    finally:
        driver.quit()
        logging.info("TOÀN BỘ TIẾN TRÌNH HOÀN TẤT.")


if __name__ == "__main__":
    # Cung cấp thư mục chứa các file amazon_batch_X.csv
    process_csv_files_dir = 'data_amazon_split'
    run_scraper(process_csv_files_dir)