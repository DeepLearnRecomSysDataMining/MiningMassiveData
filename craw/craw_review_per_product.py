import csv
import time
import os
import logging
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException


def init(output_dir='data_amazon'):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("product_review_scraper.log", encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    os.makedirs(output_dir, exist_ok=True)


def get_chrome_driver():
    options = Options()
    # Amazon rất dễ nhận diện headless, nếu bị chặn hãy comment dòng headless lại
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=options)
    return driver


def scrape_amazon_reviews(asin_list):
    driver = get_chrome_driver()
    all_reviews = []

    try:
        for asin in asin_list:
            url = f"https://www.amazon.com/product-reviews/{asin}"
            logging.info(f"Đang xử lý ASIN: {asin}")

            driver.get(url)

            # Đợi danh sách review xuất hiện
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-hook="review"]'))
                )
            except TimeoutException:
                logging.warning(f"Không tìm thấy review cho {asin} (Có thể bị Captcha hoặc chưa có review)")
                continue

            # Lấy danh sách các thẻ review
            review_elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-hook="review"]')

            for el in review_elements:
                data = {
                    'asin': asin,
                    'author': safe_extract(el, 'span.a-profile-name'),
                    'rating': safe_extract(el, 'i[data-hook="review-star-rating"] span.a-icon-alt'),
                    'title': safe_extract(el, 'a[data-hook="review-title"] span'),
                    'date': safe_extract(el, 'span[data-hook="review-date"]'),
                    'content': safe_extract(el, 'span[data-hook="review-body"]'),
                    'verified': "Yes" if el.find_elements(By.CSS_SELECTOR, 'span[data-hook="avp-badge"]') else "No"
                }
                all_reviews.append(data)
                logging.info(f" -> Lấy được review của: {data['author']}")

            time.sleep(3)  # Nghỉ ngắn để tránh bot detection

    finally:
        driver.quit()
        save_to_csv(all_reviews)


def safe_extract(element, selector):
    """Hàm bổ trợ trích xuất text an toàn từ WebElement"""
    try:
        return element.find_element(By.CSS_SELECTOR, selector).get_attribute("innerText").strip()
    except NoSuchElementException:
        return None


def save_to_csv(data, filename="amazon_reviews.csv"):
    if not data: return
    file_path = os.path.join('data_amazon', filename)
    keys = data[0].keys()
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    logging.info(f"✅ Đã lưu {len(data)} review vào {file_path}")


if __name__ == "__main__":
    init()
    # Danh sách ASIN ví dụ
    asins = ['B088NHSVJN']
    scrape_amazon_reviews(asins)