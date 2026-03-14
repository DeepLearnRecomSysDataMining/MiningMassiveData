import json
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


# ==========================================
# 1. CẤU HÌNH LOGGING
# ==========================================
def init(output_dir='data_amazon_split'):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
    file_handler = logging.FileHandler("amazon_metadata_scraper.log", encoding='utf-8')
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

# ==========================================
# 2. KHỞI TẠO DRIVER & THIẾT LẬP VÙNG (NEW)
# ==========================================
def get_chrome_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
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

def safe_get_text(parent, selector):
    try:
        return parent.find_element(By.CSS_SELECTOR, selector).get_attribute("innerText").strip()
    except:
        return None


def get_identifiers(container, page_source, url):
    """Tách ASIN và Parent ASIN"""
    current_asin = container.get_attribute("data-asin")
    if not current_asin:
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', url)
        current_asin = asin_match.group(1) if asin_match else "Unknown"

    parent_asin = current_asin
    parent_match = re.search(r'\"parentAsin\":\"([A-Z0-9]{10})\"', page_source)
    if parent_match:
        parent_asin = parent_match.group(1)
    return current_asin, parent_asin


def get_images(page_source, container):
    """Trích xuất danh sách hình ảnh (Hi-res và Gallery)"""
    images = []
    # Nguồn 1: JavaScript JSON (Hi-res)
    img_match = re.search(r'\'colorImages\':\s*\{.*\'initial\':\s*(\[.*?\])\},', page_source, re.DOTALL)
    if img_match:
        try:
            raw_imgs = json.loads(img_match.group(1))
            for im in raw_imgs:
                images.append({
                    "thumb": im.get("thumb"), "large": im.get("large"),
                    "variant": im.get("variant"), "hi_res": im.get("hiRes")
                })
        except:
            pass

    # Nguồn 2: Fallback từ DOM nếu JS fail
    if not images:
        thumbs = container.find_elements(By.CSS_SELECTOR, "#altImages ul li img")
        for t in thumbs:
            src = t.get_attribute("src")
            if src:
                hi_res = re.sub(r'\._[A-Z0-9, ]+_\.', '.', src)
                images.append({"thumb": src, "large": hi_res, "variant": "MAIN", "hi_res": hi_res})
    return images


def get_product_description(container):
    """Lấy nội dung mô tả sản phẩm"""
    description = []
    desc_els = container.find_elements(By.CSS_SELECTOR, "#productDescription p span")
    for d in desc_els:
        if d.text.strip():
            description.append(d.text.strip())
    return description


def get_technical_details(driver, container):
    try:
        # 1. Cuộn đến khu vực thông số
        driver.execute_script( "var el = document.getElementById('prodDetails'); if(el) el.scrollIntoView({block: 'center'});")
        time.sleep(1)
        # 2. Click VẬT LÝ vào tất cả các nút mở rộng. Nhắm vào span chứa hành động hoặc thẻ a có aria-expanded='false'
        expanders = container.find_elements(By.CSS_SELECTOR, "#prodDetails [data-action='a-expander-toggle'], #prodDetails .a-expander-header")

        for exp in expanders:
            try:
                # Kiểm tra trạng thái thực tế qua aria-expanded của thẻ con <a> nếu có
                try:
                    is_expanded = exp.find_element(By.TAG_NAME, "a").get_attribute("aria-expanded")
                except:
                    is_expanded = exp.get_attribute("aria-expanded")

                if is_expanded == "false" or not is_expanded:
                    driver.execute_script("arguments[0].click();", exp)
                    time.sleep(0.4)  # Chờ AJAX load data
            except:
                continue
    except Exception as e:
        logging.warning(f"      [!] Lỗi khi ép mở rộng: {str(e)}")

    # --- PHẦN TRÍCH XUẤT DETAILS (QUÉT TOÀN BỘ SAU KHI ÉP HIỂN THỊ) ---
    details = {}
    try:
        # Tìm tất cả các bảng prodDetTable nằm trong prodDetails
        # Bây giờ tất cả đã hiển thị nên Selenium sẽ lấy được hết
        prod_details_div = container.find_element(By.ID, "prodDetails")
        rows = prod_details_div.find_elements(By.TAG_NAME, "tr")

        for row in rows:
            try:
                th = row.find_elements(By.TAG_NAME, "th")
                td = row.find_elements(By.TAG_NAME, "td")

                if th and td:
                    k = th[0].get_attribute("innerText").strip()
                    v = td[0].get_attribute("innerText").strip()

                    if k and v:
                        # Làm sạch string chuẩn Dataset Amazon
                        clean_key = " ".join(k.split()).replace("\u200e", "")
                        clean_val = " ".join(v.split()).replace("\u200e", "")

                        # Loại bỏ thông tin thừa không cần thiết cho Recommendation
                        if "Customer Reviews" not in clean_key:
                            details[clean_key] = clean_val
            except:
                continue
    except NoSuchElementException:
        pass

    return details

def extract_product_details(driver, url, main_category):
    try:
        driver.get(url)
        if "captcha" in driver.current_url.lower() or "automated access" in driver.page_source:
            return "CAPTCHA"
        wait = WebDriverWait(driver, 15)
        try:
            container = wait.until(EC.presence_of_element_located((By.ID, "dp-container")))
            driver.execute_script("window.scrollTo(0, 1200);")
            time.sleep(1)
        except:
            logging.warning(f"      [!] Không tìm thấy dp-container tại: {url}")
            return None

        page_source = driver.page_source
        current_asin, parent_asin = get_identifiers(container, page_source, url)
        details = get_technical_details(driver, container)
        images = get_images(page_source, container)
        description = get_product_description(container)

        title = safe_get_text(container, "#productTitle")
        store = safe_get_text(container, "#bylineInfo")
        price_str = safe_get_text(container, "#corePrice_feature_div .a-offscreen") or \
                    safe_get_text(container, "#price_inside_buybox") or \
                    safe_get_text(container, ".a-price .a-offscreen")

        avg_rating = safe_get_text(container, "#averageCustomerReviews .a-icon-alt")
        avg_rating = avg_rating.split()[0] if avg_rating else None
        rating_num = safe_get_text(container, "#acrCustomerReviewText")
        rating_num = re.sub(r'\D', '', rating_num) if rating_num else None

        features = [f.text.strip() for f in
                    container.find_elements(By.CSS_SELECTOR, "#feature-bullets ul li span.a-list-item") if
                    f.text.strip()]

        categories = [c.text.strip() for c in
                      driver.find_elements(By.CSS_SELECTOR, "#wayfinding-breadcrumbs_container ul li span a")]

        logging.info(f"      ✅ DONE: {current_asin} | {len(images)} ảnh | {len(details)} specs")

        return {
            "main_category": main_category,
            "title": title,
            "average_rating": avg_rating,
            "rating_number": rating_num,
            "features": features,
            "description": description,
            "price": price_str,
            "images": images,
            "store": store,
            "categories": categories,
            "details": details,
            "asin": current_asin,
            "parent_asin": parent_asin,
            "url": url,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logging.error(f"      ❌ Lỗi trích xuất tại {url}: {str(e)[:100]}")
        return None

# ==========================================
# 4. TIẾN TRÌNH XỬ LÝ (THEO CHUNK 20)
# ==========================================
def process_amazon_metadata(source_dir, manual_category, start_file_index=0):
    abs_dir = init(source_dir)
    all_files = os.listdir(abs_dir)
    csv_files = [f for f in all_files if f.lower().endswith('.csv') and 'amazon_batch_' in f]
    csv_files.sort(key=lambda f: int(re.findall(r'\d+', f)[0]))

    jsonl_path = os.path.abspath('metadata')
    os.makedirs(jsonl_path, exist_ok=True)
    logging.info(f"Hệ thống khởi tạo thành công. Thư mục: {jsonl_path}")

    if not csv_files:
        logging.error("❌ Không tìm thấy file CSV!")
        return

    driver = get_chrome_driver()

    # THIẾT LẬP VÙNG US NGAY SAU KHI MỞ TRÌNH DUYỆT
    set_us_location(driver)

    CHUNK_SIZE = 20

    try:
        for idx_file, file_name in enumerate(csv_files[start_file_index:]):
            current_file_pos = start_file_index + idx_file
            full_csv_path = os.path.join(abs_dir, file_name)

            save_name = f"{manual_category.replace(' ', '_').lower()}_details.jsonl"
            full_json_path = os.path.join(jsonl_path, save_name)

            logging.info(f"📂 [FILE {current_file_pos}/{len(csv_files) - 1}] {file_name}")
            df = pd.read_csv(full_csv_path, low_memory=False)
            urls = df['url'].dropna().unique().tolist()

            batch_results = []
            for idx, url in enumerate(urls):
                logging.info(f"   🔹 [{idx + 1}/{len(urls)}] Cào URL: {url}")
                data = extract_product_details(driver, url, manual_category)

                if data == "CAPTCHA":
                    if batch_results:
                        with open(full_json_path, 'a', encoding='utf-8') as f:
                            for item in batch_results:
                                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                    logging.critical("🛑 DỪNG DO CAPTCHA.")
                    return

                if data:
                    batch_results.append(data)

                if len(batch_results) >= CHUNK_SIZE or idx == len(urls) - 1:
                    if batch_results:
                        with open(full_json_path, 'a', encoding='utf-8') as f:
                            for item in batch_results:
                                f.write(json.dumps(item, ensure_ascii=False) + "\n")
                        logging.info(f"💾 Đã xả {len(batch_results)} items xuống file.")
                        batch_results = []

                time.sleep(1)

    finally:
        driver.quit()
        logging.info("🏁 HOÀN TẤT.")


if __name__ == "__main__":
    MY_CAT = "Smartphone"
    process_amazon_metadata('data_amazon_split', MY_CAT, start_file_index=2)