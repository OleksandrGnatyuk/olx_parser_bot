import csv
import random
import time
import requests
from bs4 import BeautifulSoup
import logging
from dotenv import load_dotenv
import os

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан в .env")

TELEGRAM_CHAT_ID_OLEKSANDR = os.getenv("TELEGRAM_CHAT_ID_OLEKSANDR")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_CHAT_ID не задан в .env")

CSV_FILE_PATH = "csv/all_ad.csv"

logging.basicConfig(level=logging.INFO)

urls = [
    "https://www.olx.ua/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/kiev/?currency=UAH&search%5Bfilter_float_total_area:to%5D=60&search%5Bfilter_enum_furnish%5D%5B0%5D=yes&search%5Bfilter_enum_number_of_rooms_string%5D%5B0%5D=dvuhkomnatnye",
    ]


# Список User-Agent заголовків для імітації реальних браузерів
headers_list = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"}
]


def send_telegram_message_oleksandr(message):
    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {
        "chat_id": TELEGRAM_CHAT_ID_OLEKSANDR,
        "text": message,
        "parse_mode": "HTML",
    }
    requests.get(telegram_url, params=params)


# Затримка між запитами для зниження навантаження на сервер
def delay():
    sleep_time = random.randint(5, 10)  # Випадкова затримка від 5 до 10 секунд
    time.sleep(sleep_time)


# Функція для отримання HTML контенту сторінки
def get_html(url):
    headers = random.choice(headers_list)  # Випадковий заголовок
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Помилка з'єднання: {e}")
        return None


# Функція для парсингу даних зі сторінки
def parse_page(url):
    logging.info(f"Парсимо сторінку: {url}")
    html_content = get_html(url)

    if html_content:
        soup = BeautifulSoup(html_content, "lxml")

        list_of_ap = []

        all_apartments = soup.select('div.css-l9drzq')

        for items in all_apartments:
            name_of_ad = items.select_one("h4").text.strip()
            local_time = items.select_one("p.css-vbz67q").text.split(" - ")
            time = local_time[1]
            local = local_time[0]
            price = items.select_one("p.css-uj7mm0").text.strip()
            square = items.select_one("span.css-6as4g5").text.strip()
            link_tag = items.select_one("div.css-u2ayx9").select_one("a")
            link = f"https://www.olx.ua{link_tag.get('href')}" if link_tag.get("href").startswith("/") else link_tag.get(
                "href")
            list_of_ap.append([time, name_of_ad, local, price, square, link])

        try:
            with open(CSV_FILE_PATH, "r", encoding="utf8") as file:
                reader = csv.reader(file)
                existing_names = {row[1] for row in reader}
        except FileNotFoundError:
            existing_names = set()

        new_ads = []

        with open(CSV_FILE_PATH, "a", encoding="utf8", newline="") as file:
            writer = csv.writer(file)
            for row in list_of_ap:
                if row[1] not in existing_names:
                    writer.writerow(row)
                    new_ads.append(row)

        if new_ads:
            for ad in new_ads:
                msg = f"🏠 <b>{ad[1]}</b>\n📍 {ad[2]}\n💰 {ad[3]}\n📏 {ad[4]}\n⏰ {ad[0]}\n🔗 <a href='{ad[5]}'>Посилання</a>"
                send_telegram_message_oleksandr(msg)
            logging.info(f"📨 Відправлено {len(new_ads)} нових оголошень в Telegram!")
        else:
            logging.info("❌ Нових оголошень немає.")
            send_telegram_message_oleksandr("❌ Нових оголошень немає.")

    else:
        logging.info(f"Не вдалося отримати контент сторінки: {url}")


async def start_parsing():
    for url in urls:
        parse_page(url)
        delay()  # Викликаємо затримку між запитами

# for url in urls:
#     parse_page(url)
#     delay()