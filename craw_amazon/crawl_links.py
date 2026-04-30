import os
import time
import logging
import re
import sys
import json
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
                logging.FileHandler("amazon_integrated_scraper.log", encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
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

def get_category_array(driver, base_category_list):
    """Lấy cây danh mục (Breadcrumbs) trên trang chi tiết"""
    categories = [cat for cat in base_category_list]
    try:
        # Tìm thanh Breadcrumb của Amazon
        breadcrumbs = driver.find_elements(By.CSS_SELECTOR, "#wayfinding-breadcrumbs_feature_div a.a-link-normal")
        for b in breadcrumbs:
            text = b.text.strip()
            if text:
                categories.append(text)
    except:
        pass
    # Nếu không tìm thấy, dùng list category truyền vào làm mảng gốc (VD: ["Smartphone", "Apple"])
    if not categories:
        categories = base_category_list.copy()
    return categories

def extract_asins_from_visible_dom(container, category_array):
    """Hàm Helper: Quét các phần tử có data_small-asin trong một vùng DOM cụ thể"""
    items = {}
    elements = container.find_elements(By.XPATH, ".//*[@data_small-asin]")

    for el in elements:
        asin = el.get_attribute("data_small-asin")
        if asin and len(asin) == 10 and asin not in items:
            try:
                # 1. Lấy Link
                href = None
                try:
                    link_tag = el.find_element(By.XPATH, ".//a[contains(@href, '/dp/')]")
                    href = link_tag.get_attribute("href")
                except:
                    pass

                clean_url = href.split("/ref=")[0] if href else f"https://www.amazon.com/dp/{asin}"
                if clean_url.startswith("/"):
                    clean_url = "https://www.amazon.com" + clean_url

                # 2. Lấy Ảnh
                img_url = None
                try:
                    img_tag = el.find_element(By.TAG_NAME, "img")
                    src = img_tag.get_attribute("src")
                    if src and "media-amazon.com" in src:
                        img_url = get_high_res_img(src)  # Cần đảm bảo hàm get_high_res_img đã có
                except:
                    pass

                items[asin] = {
                    'asin': asin,
                    'url': clean_url,
                    'image_url': img_url,
                    'categories': category_array  # Áp dụng mảng category cho biến thể
                }
            except Exception as e:
                continue
    return items

def extract_detail_page(driver, url, base_category_list):
    """Vào trang chi tiết, lấy Category Array và tập trung cào biến thể trong vùng PPD"""
    items = {}
    try:
        driver.get(url)
        time.sleep(2)  # Chờ DOM load nhẹ
        if "captcha" in driver.current_url.lower() or "automated access" in driver.page_source.lower():
            return "CAPTCHA"
        wait = WebDriverWait(driver, 8)
        try:
            container = wait.until(EC.presence_of_element_located((By.ID, "dp-container")))
        except:
            container = driver.find_element(By.TAG_NAME, "body")
        # 1. Lấy Category Array (Từ thanh Breadcrumb)
        category_array = get_category_array(driver, base_category_list)
        logging.info(f"        - Cây danh mục: {category_array}")
        # 2. Lấy ASIN của sản phẩm gốc (Sản phẩm đang xem)
        try:
            main_asin = driver.find_element(By.ID, "ASIN").get_attribute("value")
            if main_asin and len(main_asin) == 10:
                items[main_asin] = {'asin': main_asin, 'url': url, 'image_url': None, 'categories': category_array}
        except:
            pass
        # 3. CHỈ QUÉT SÂU TRONG VÙNG PPD (Tìm các biến thể sản phẩm)
        try:
            ppd_section = container.find_element(By.ID, "ppd")
            logging.info(f"        Đang quét sâu trong vùng PPD (Product Product Detail)...")
            ppd_items = extract_asins_from_visible_dom(ppd_section, category_array)
            items.update(ppd_items)
        except NoSuchElementException:
            logging.warning(f"        Không tìm thấy vùng id='ppd' bên trong dp-container.")
        # In log danh sách ASIN thu được giống như code của bạn
        asin_list = list(items.keys())
        logging.info(f"        Lấy được {len(asin_list)} ASIN: {', '.join(asin_list)}")
        return list(items.values())
    except Exception as e:
        logging.error(f"        Lỗi xử lý chi tiết trang {url}: {str(e)[:100]}")
        return []

def save_batch_to_csv(data_list, output_file):
    """Lưu 1 mẻ (batch) vào file CSV và lọc trùng ASIN"""
    if not data_list:
        return

    df_new = pd.DataFrame(data_list)

    # Ép kiểu danh sách category thành chuỗi JSON để lưu trữ vào CSV dễ dàng
    df_new['categories'] = df_new['categories'].apply(lambda x: json.dumps(x, ensure_ascii=False))

    if os.path.exists(output_file) and os.stat(output_file).st_size > 0:
        try:
            # Đọc ASIN cũ để lọc trùng
            existing_asins = pd.read_csv(output_file, usecols=['asin'], dtype={'asin': str})['asin'].unique()
            df_new = df_new[~df_new['asin'].isin(existing_asins)]
        except Exception as e:
            logging.error(f"        [Lỗi lọc trùng] {str(e)}")

    if df_new.empty:
        logging.info("        - Không có sản phẩm/biến thể mới nào trong mẻ này (đã trùng).")
        return

    try:
        file_exists = os.path.exists(output_file) and os.stat(output_file).st_size > 0
        df_new.to_csv(output_file, mode='a', index=False, header=not file_exists, encoding='utf-8')
        logging.info(f"        ✅ Đã lưu mẻ {len(df_new)} sản phẩm vào {os.path.basename(output_file)}")
    except Exception as e:
        logging.error(f"        ❌ LỖI GHI FILE: {str(e)}")

def process_search_page(driver, search_url, base_category_list, output_file):
    """Quét trang tìm kiếm, lấy các URL và đi sâu vào từng URL đó"""
    driver.get(search_url)
    if "captcha" in driver.current_url.lower() or "automated access" in driver.page_source.lower():
        return "CAPTCHA_SEARCH"
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data_small-asin]")))
        for i in range(1, 4):
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {i / 3});")
            time.sleep(1)
        # ==========================================
        # SỬA Ở ĐÂY: BẮT LINK NEXT NGAY TRƯỚC KHI ĐI VÀO TRANG CHI TIẾT
        # ==========================================
        next_url = None
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "a.s-pagination-next")
            next_url = next_btn.get_attribute("href")
        except NoSuchElementException:
            logging.info("    [Hết trang] Không tìm thấy nút Next trên trang Search.")

        items = driver.find_elements(By.CSS_SELECTOR, "div[data_small-asin]")
        product_urls_to_visit = []
        # Lấy danh sách URL từ kết quả tìm kiếm
        for item in items:
            asin = item.get_attribute("data_small-asin")
            if not asin: continue
            try:
                link_tag = item.find_element(By.CSS_SELECTOR, "a.a-link-normal[href*='/dp/']")
                href = link_tag.get_attribute("href")
                clean_href = href.split("/ref=")[0]
                if clean_href.startswith("/"):
                    clean_href = "https://www.amazon.com" + clean_href
                product_urls_to_visit.append(clean_href)
            except NoSuchElementException:
                continue
        product_urls_to_visit = list(set(product_urls_to_visit))
        logging.info(f"    Tìm thấy {len(product_urls_to_visit)} URL sản phẩm trên trang search.")
        for idx, p_url in enumerate(product_urls_to_visit):
            logging.info(f"    [{idx + 1}/{len(product_urls_to_visit)}] Đào sâu: {p_url}")
            variants_data = extract_detail_page(driver, p_url, base_category_list)
            if variants_data == "CAPTCHA":
                logging.warning("        Dính CAPTCHA ở trang chi tiết! Báo hiệu khởi động lại...")
                return "CAPTCHA_DETAIL"
            if variants_data:
                logging.info(f"        - Bóc tách được {len(variants_data)} biến thể/ASIN.")
                save_batch_to_csv(variants_data, output_file)
            time.sleep(1)
        # Trả về URL của trang tiếp theo đã lưu từ trước
        return next_url
    except TimeoutException:
        logging.error("    [Lỗi Timeout] Không tải được danh sách tìm kiếm.")
        return None

def run_integrated_scraper(categories_config, max_pages=10, output_dir='data_integrated'):
    init_logging()
    os.makedirs(output_dir, exist_ok=True)
    driver = get_chrome_driver()

    try:
        for cat_name, brands_data in categories_config.items():
            safe_name = cat_name.replace(' ', '_').lower()
            output_file = os.path.join(output_dir, f"{safe_name}_integrated.csv")
            logging.info(f"BẮT ĐẦU DANH MỤC: {cat_name.upper()}")

            # Duyệt qua từng Brand trong Category
            for brand_name, urls in brands_data.items():
                # Chuyển URL thành list để xử lý đồng nhất (hỗ trợ trường hợp 1 hãng có nhiều link)
                url_list = urls if isinstance(urls, list) else [urls]

                # Tạo list Category cơ sở: Ví dụ ["Smartphone", "Apple"]
                base_category_list = [cat_name, brand_name]

                for base_url in url_list:
                    current_url = base_url
                    page_count = 1
                    while current_url and page_count <= max_pages:
                        logging.info(f"TRANG SEARCH {page_count} | BRAND: {brand_name} | URL: {current_url}")
                        # Truyền base_category_list xuống process_search_page
                        status = process_search_page(driver, current_url, base_category_list, output_file)
                        if status in ["CAPTCHA_SEARCH", "CAPTCHA_DETAIL"]:
                            logging.warning("CAPTCHA phát hiện. Đang tái khởi động Driver...")
                            driver.quit()
                            time.sleep(5)
                            driver = get_chrome_driver()
                            continue
                        current_url = status  # status lúc này là next_url hoặc None
                        page_count += 1
                        time.sleep(3)  # Nghỉ ngơi khi sang trang Search mới
    finally:
        driver.quit()
        logging.info("🏁 HOÀN TẤT TOÀN BỘ QUÁ TRÌNH CÀO TÍCH HỢP.")

if __name__ == '__main__':
    # Đã format lại toàn bộ các Category theo cấu trúc Dictionary {Hãng: URL}
    # Với các Hãng có nhiều URL, URL được gom vào mảng (List) để không bị ghi đè key.
    categories_config = {
        # "Smartphone": {
        #     "Apple": [
        #         "https://www.amazon.com/s?k=iphone",
        #         "https://www.amazon.com/s?k=smartphone+apple"
        #     ],
        #     "Samsung": [
        #         "https://www.amazon.com/s?k=smartphone+samsung",
        #         "https://www.amazon.com/s?k=samsung+z+fold"
        #     ],
        #     "Motorola": "https://www.amazon.com/s?k=smartphones+motorola",
        #     "Xiaomi": "https://www.amazon.com/s?k=smartphones+xiaomi",
        #     "Blackberry": "https://www.amazon.com/s?k=blackberry+smartphones",
        #     "Redmi": "https://www.amazon.com/s?k=smartphones+redmi",
        #     "Realme": "https://www.amazon.com/s?k=smartphones+realme",
        #     "Oppo": "https://www.amazon.com/s?k=smartphones+oppo",
        #     "Vivo": "https://www.amazon.com/s?k=vivo+smartphones"
        # },
        "Tablets": {
            # "General": "https://www.amazon.com/s?k=tablets",
            # "Samsung": "https://www.amazon.com/s?k=tablets+samsung",
            # "Apple": [
            #     "https://www.amazon.com/s?k=tablets+apple",
            #     "https://www.amazon.com/s?k=ipad"
            # ],
            # "Amazon": "https://www.amazon.com/s?k=tablets+amazon",
            # "Lenovo": "https://www.amazon.com/s?k=tablets+lenovo",
            "Microsoft": "https://www.amazon.com/s?k=tablets+microsoft",
            "Google": "https://www.amazon.com/s?k=tablets+google",
            "Asus": "https://www.amazon.com/s?k=tablets+asus",
            "HP": "https://www.amazon.com/s?k=tablets+hp",
            "Dell": "https://www.amazon.com/s?k=tablets+dell",
            "Xiaomi": "https://www.amazon.com/s?k=tablets+xiaomi",
            "Acer": "https://www.amazon.com/s?k=tablets+acer",
            "LG": "https://www.amazon.com/s?k=tablets+lg",
            "Sony": "https://www.amazon.com/s?k=tablets+sony",
            "MSI": "https://www.amazon.com/s?k=tablets+msi"
        },
        "Headphone": {
            "General": [
                "https://www.amazon.com/s?k=headphones",
                "https://www.amazon.com/s?k=earphones",
                "https://www.amazon.com/s?k=earphones+wireless"
            ],
            "Apple": "https://www.amazon.com/s?k=airpods"
        },
        "Laptop": {
            "General": "https://www.amazon.com/s?k=laptops",
            "HP": "https://www.amazon.com/s?k=laptops+hp",
            "Dell": "https://www.amazon.com/s?k=laptops+dell",
            "Lenovo": "https://www.amazon.com/s?k=laptops+lenovo",
            "Microsoft": "https://www.amazon.com/s?k=surface+laptop",
            "Apple": "https://www.amazon.com/s?k=macbook+apple",
            "Asus": "https://www.amazon.com/s?k=laptops+asus",
            "Acer": "https://www.amazon.com/s?k=laptops+acer",
            "Samsung": "https://www.amazon.com/s?k=laptops+samsung",
            "MSI": "https://www.amazon.com/s?k=laptops+msi",
            "LG": "https://www.amazon.com/s?k=laptops+LG"
        },
        "Computer": {
            "HP": "https://www.amazon.com/s?k=computers+hp",
            "Dell": "https://www.amazon.com/s?k=computers+dell",
            "Lenovo": "https://www.amazon.com/s?k=computers+lenovo",
            "Asus": "https://www.amazon.com/s?k=computers+asus",
            "Acer": "https://www.amazon.com/s?k=computers+acer",
            "Microsoft": "https://www.amazon.com/s?k=computers+microsoft",
            "Samsung": "https://www.amazon.com/s?k=computers+samsung",
            "MSI": "https://www.amazon.com/s?k=computers+msi",
            "Razer": "https://www.amazon.com/s?k=computers+razer",
            "LG": "https://www.amazon.com/s?k=computers+lg",
            "Gigabyte": "https://www.amazon.com/s?k=computers+gigabyte",
            "Panasonic": "https://www.amazon.com/s?k=computers+panasonic",
            "Fujitsu": "https://www.amazon.com/s?k=computers+fujitsu",
            "Intel": "https://www.amazon.com/s?k=computers+intel"
        },
        "Desktop": {
            "General": "https://www.amazon.com/s?k=desktop+computer",
            "Dell": "https://www.amazon.com/s?k=desktop+computer+dell",
            "HP": "https://www.amazon.com/s?k=desktop+computer+hp",
            "Lenovo": "https://www.amazon.com/s?k=desktop+computer+lenovo",
            "Apple": "https://www.amazon.com/s?k=desktop+computer+apple",
            "Asus": "https://www.amazon.com/s?k=desktop+computer+asus",
            "Acer": "https://www.amazon.com/s?k=desktop+computer+acer",
            "MSI": "https://www.amazon.com/s?k=desktop+computer+msi",
            "Intel": "https://www.amazon.com/s?k=desktop+computer+intel"
        },
        "CPU": {
            "General": "https://www.amazon.com/s?k=cpu",
            "AMD": "https://www.amazon.com/s?k=cpu+amd",
            "Intel": "https://www.amazon.com/s?k=cpu+intel",
            "Apple": "https://www.amazon.com/s?k=cpu+apple",
            "Samsung": "https://www.amazon.com/s?k=cpu+samsung",
            "Seagate": "https://www.amazon.com/s?k=cpu+seagate",
            "Dell": "https://www.amazon.com/s?k=cpu+dell",
            "HP": "https://www.amazon.com/s?k=cpu+hp",
            "Asus": "https://www.amazon.com/s?k=cpu+asus",
            "Lenovo": "https://www.amazon.com/s?k=cpu+lenovo",
            "Corsair": "https://www.amazon.com/s?k=cpu+corsair",
            "MSI": "https://www.amazon.com/s?k=cpu+msi",
            "Thermalright": "https://www.amazon.com/s?k=cpu+thermalright",
            "Acer": "https://www.amazon.com/s?k=cpu+acer",
            "Gigabyte": "https://www.amazon.com/s?k=cpu+gigabyte",
            "IBM": "https://www.amazon.com/s?k=cpu+ibm",
            "Razer": "https://www.amazon.com/s?k=cpu+razer"
        },
        "PC": {
            "General": "https://www.amazon.com/s?k=PCs",
            "Dell": "https://www.amazon.com/s?k=PCs+dell",
            "HP": "https://www.amazon.com/s?k=PCs+hp",
            "Lenovo": "https://www.amazon.com/s?k=PCs+lenovo",
            "Asus": "https://www.amazon.com/s?k=PCs+asus",
            "Apple": "https://www.amazon.com/s?k=PCs+apple",
            "MSI": "https://www.amazon.com/s?k=PCs+msi",
            "Acer": "https://www.amazon.com/s?k=PCs+acer",
            "Cyberpowerpc": "https://www.amazon.com/s?k=PCs+cyberpowerpc",
            "Intel": "https://www.amazon.com/s?k=PCs+intel",
            "Gigabyte": "https://www.amazon.com/s?k=PCs+gigabyte"
        },
        "GPU": {
            "General": "https://www.amazon.com/s?k=gpu",
            "Asus": "https://www.amazon.com/s?k=gpu+asus",
            "MSI": "https://www.amazon.com/s?k=gpu+msi",
            "Gigabyte": "https://www.amazon.com/s?k=gpu+gigabyte",
            "AMD": "https://www.amazon.com/s?k=gpu+amd",
            "Intel": "https://www.amazon.com/s?k=gpu+intel",
            "HP": "https://www.amazon.com/s?k=gpu+hp",
            "Nvidia": "https://www.amazon.com/s?k=gpu+nvidia"
        },
        "Monitor": {
            "General": "https://www.amazon.com/s?k=monitor",
            "Dell": "https://www.amazon.com/s?k=monitor+dell",
            "KTC": "https://www.amazon.com/s?k=monitor+ktc",
            "Samsung": "https://www.amazon.com/s?k=monitor+samsung",
            "Asus": "https://www.amazon.com/s?k=monitor+asus",
            "LG": "https://www.amazon.com/s?k=monitor+lg",
            "Acer": "https://www.amazon.com/s?k=monitor+acer",
            "HP": "https://www.amazon.com/s?k=monitor+hp",
            "MSI": "https://www.amazon.com/s?k=monitor+msi",
            "BenQ": "https://www.amazon.com/s?k=monitor+benq",
            "Sceptre": "https://www.amazon.com/s?k=monitor+sceptre",
            "AOC": "https://www.amazon.com/s?k=monitor+aoc",
            "Lenovo": "https://www.amazon.com/s?k=monitor+lenovo",
            "Viewsonic": "https://www.amazon.com/s?k=monitor+viewsonic",
            "Gigabyte": "https://www.amazon.com/s?k=monitor+gigabyte",
            "Sony": "https://www.amazon.com/s?k=monitor+sony",
            "Xiaomi": "https://www.amazon.com/s?k=monitor+xiaomi",
            "Philips": "https://www.amazon.com/s?k=monitor+philips"
        },
        "Television": {
            "Samsung": "http://amazon.com/s?k=television&rh=n%3A21513596011%2Cp_123%3A46655&dc&ds=v1%3A%2F%2FossqlT2SOmGRGMBTF89Cg8X8GapsisZDKTK6DZtQ8&crid=1FCI4YSERO5S4&qid=1775986498&rnid=85457740011&sprefix=%2Caps%2C481&ref=sr_nr_p_123_1",
            "LG": "https://www.amazon.com/s?k=television&rh=n%3A21513596011%2Cp_123%3A46658&dc&ds=v1%3AIMIwX8bqzUcKp6lUdHFnJ4uapcln5Ujvnyrjb9yZlHI&crid=T927QM2MXJP5&qid=1774530087&rnid=85457740011&sprefix=tele%2Caps%2C378",
            "Sony": "https://www.amazon.com/s?k=television&rh=n%3A21513596011%2Cp_123%3A237204&dc&crid=T927QM2MXJP5&qid=1774530138&rnid=85457740011&sprefix=tele%2Caps%2C378&ds=v1%3A5OcjXVqGn3tg%2F4MWu1%2FGEX1qhPT%2FepRxMMprKURqfF8",
            "Panasonic": "https://www.amazon.com/s?k=television&rh=n%3A21513596011%2Cp_123%3A254407&dc&ds=v1%3ACejlLSBokrVArhL8yvADQX%2B7k%2B3cxVcUmt5B%2F82ik6w&crid=T927QM2MXJP5&qid=1774530235&rnid=85457740011&sprefix=tele%2Caps%2C378"
        }
    }
    # Đã sửa lại lỗi gọi nhầm target_configs thành categories_config
    run_integrated_scraper(categories_config, max_pages=3)