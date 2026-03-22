import time
import os
import logging
import re
import pandas as pd
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ==========================================
# 1. CẤU HÌNH HỆ THỐNG
# ==========================================
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


# ==========================================
# 2. LOGIC TRÍCH XUẤT NÂNG CAO
# ==========================================
def extract_asins_from_visible_dom(container, category_name):
    """Quét mọi phần tử hiện có trong DOM và logging chi tiết nguồn gốc"""
    items = {}
    count_data_asin = 0
    count_csa_id = 0

    # Tìm tất cả phần tử có thuộc tính liên quan đến ASIN
    elements = container.find_elements(By.XPATH, ".//*[ @data-asin or contains(@data-csa-c-item-id, 'asin.') ]")

    for el in elements:
        asin = None
        source = ""

        # 1. Thử lấy từ data-asin trực tiếp
        val_asin = el.get_attribute("data-asin")
        if val_asin and len(val_asin) == 10:
            asin = val_asin
            source = "data-asin"
        else:
            # 2. Thử bóc tách từ data-csa-c-item-id (ví dụ: amzn1.asin.B0...)
            val_csa = el.get_attribute("data-csa-c-item-id")
            if val_csa:
                match = re.search(r'asin\.([A-Z0-9]{10})', val_csa)
                if match:
                    asin = match.group(1)
                    source = "data-csa-c-item-id"

        if asin and asin not in items:
            try:
                # Tìm link và ảnh
                link_tag = el.find_element(By.XPATH, ".//a[contains(@href, '/dp/')]")
                href = link_tag.get_attribute("href")
                img_tag = el.find_element(By.TAG_NAME, "img")
                src = img_tag.get_attribute("src")

                if href and src and "media-amazon.com" in src:
                    if source == "data-asin":
                        count_data_asin += 1
                    else:
                        count_csa_id += 1

                    clean_url = href.split("/ref=")[0]
                    if clean_url.startswith("/"): clean_url = "https://www.amazon.com" + clean_url

                    items[asin] = {
                        'asin': asin,
                        'url': clean_url,
                        'image_url': get_high_res_img(src),
                        'category': category_name
                    }
            except:
                continue

    return items, count_data_asin, count_csa_id


def collect_related_links(driver, url, category_name):
    all_page_items = {}
    total_data_asin = 0
    total_csa_id = 0

    try:
        driver.get(url)
        if "captcha" in driver.current_url.lower(): return "CAPTCHA"

        wait = WebDriverWait(driver, 10)
        container = wait.until(EC.presence_of_element_located((By.ID, "dp-container")))

        # --- BƯỚC 1: TÌM TẤT CẢ CAROUSEL ---
        carousel_rows = container.find_elements(By.CSS_SELECTOR, ".a-carousel-row-inner")
        logging.info(f"        [Carousel] Phát hiện {len(carousel_rows)} cụm trượt ngang trên trang.")

        for idx, row in enumerate(carousel_rows):
            try:
                # Cuộn đến vị trí của Carousel này để nó hiển thị trong khung hình (Viewport)
                # block: 'center' giúp nút Next không bị banner che khuất
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", row)
                time.sleep(1)

                # Tìm nút Next cụ thể của Carousel này
                try:
                    next_btn = row.find_element(By.CSS_SELECTOR, "a.a-carousel-goto-nextpage")
                except NoSuchElementException:
                    next_btn = None

                # Bấm tối đa 5 trang mỗi carousel
                for p in range(5):
                    # Quét dữ liệu hiện có ở trang hiện tại của carousel
                    items, c_asin, c_csa = extract_asins_from_visible_dom(row, category_name)
                    all_page_items.update(items)
                    total_data_asin += c_asin
                    total_csa_id += c_csa

                    # Nếu có nút Next và nút đó đang hiển thị thì mới bấm
                    if next_btn and next_btn.is_displayed():
                        driver.execute_script("arguments[0].click();", next_btn)
                        # Chờ hiệu ứng trượt (Slide animation) hoàn tất
                        time.sleep(1.2)
                    else:
                        break  # Hết trang hoặc không có nút Next

                logging.info(f"          + Hoàn tất Carousel #{idx + 1}")

            except Exception as e:
                logging.warning(f"          ! Lỗi khi xử lý Carousel #{idx + 1}: {str(e)[:50]}")
                continue

        # --- BƯỚC 2: VÉT TOÀN BỘ PHẦN CÒN LẠI CỦA TRANG (DIV LẺ, SPONSORED KHÁC) ---
        # Cuộn xuống cuối trang một lần nữa để vét những phần footer
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        final_items, c_asin, c_csa = extract_asins_from_visible_dom(container, category_name)
        all_page_items.update(final_items)
        total_data_asin += c_asin
        total_csa_id += c_csa

        logging.info(f"        ✅ Tổng kết trang: {len(all_page_items)} ASIN duy nhất "
                     f"(data-asin: {total_data_asin}, csa-id: {total_csa_id})")

        return list(all_page_items.values())

    except Exception as e:
        logging.error(f"      ❌ Lỗi xử lý trang {url}: {str(e)[:100]}")
        return []

# ==========================================
# 3. QUẢN LÝ FILE VÀ LƯU TRỮ
# ==========================================
def save_and_clean(file_path, new_data_list):
    if not new_data_list: return
    df_new = pd.DataFrame(new_data_list)

    if os.path.exists(file_path) and os.stat(file_path).st_size > 0:
        df_old = pd.read_csv(file_path)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_combined = df_new

    # Lọc trùng tuyệt đối bằng ASIN
    initial_count = len(df_combined)
    df_combined.drop_duplicates(subset=['asin'], keep='first', inplace=True)
    final_count = len(df_combined)

    df_combined.to_csv(file_path, index=False, encoding='utf-8')
    logging.info(f"💾 FILE UPDATE: {os.path.basename(file_path)} | "
                 f"Thêm mới: {final_count - (initial_count - len(df_new))} | Tổng: {final_count}")


def expand_amazon_links(source_dir, categories_dict):
    init_logging()
    driver = get_chrome_driver()
    CHUNK_SIZE = 20

    try:
        for cat_name, file_name in categories_dict.items():
            if not file_name.lower().endswith('.csv'):
                file_name += '.csv'

            file_path = os.path.join(source_dir, file_name)

            if not os.path.exists(file_path):
                logging.warning(f"⚠️ Bỏ qua {cat_name}: File {file_path} không tồn tại.")
                continue

            logging.info(f"📂 DANH MỤC: {cat_name.upper()}")
            df_original = pd.read_csv(file_path)
            urls = df_original['url'].dropna().unique().tolist()

            temp_items = []
            for idx, url in enumerate(urls):
                logging.info(f"🔹 [{idx + 1}/{len(urls)}] Đang vét từ: {url}...")
                results = collect_related_links(driver, url, cat_name)

                if results == "CAPTCHA":
                    logging.critical("🛑 DỪNG DO CAPTCHA")
                    return

                if results:
                    temp_items.extend(results)

                # Lưu định kỳ
                if (idx + 1) % CHUNK_SIZE == 0 and temp_items:
                    save_and_clean(file_path, temp_items)
                    temp_items = []

                time.sleep(1)

            if temp_items:
                save_and_clean(file_path, temp_items)

    finally:
        driver.quit()
        logging.info("🏁 HOÀN TẤT TOÀN BỘ.")


if __name__ == "__main__":
    SOURCE_FOLDER = 'data_amazon_links'

    # Dictionary cấu hình Category và File tương ứng
    TARGET_CATEGORIES = {
        "Smartphone": "smartphone_products.csv",
        "Tablets": "tablets_products.csv",
        "Headphone": "headphone_products.csv",
        "Laptop": "laptop_products.csv",
        "Computer": "computer_products.csv"
    }

    expand_amazon_links(SOURCE_FOLDER, TARGET_CATEGORIES)