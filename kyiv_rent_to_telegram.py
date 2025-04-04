import csv
import random
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from logger_ import get_logger
from dotenv import load_dotenv
import os
import urllib.parse # Потрібно для генерації URL та urljoin

# Завантаження змінних середовища
load_dotenv()

# Словник українських місяців у родовому відмінку для форматування дати
UKRAINIAN_MONTHS_GENITIVE = {
    1: "січня", 2: "лютого", 3: "березня", 4: "квітня", 5: "травня",
    6: "червня", 7: "липня", 8: "серпня", 9: "вересня", 10: "жовтня",
    11: "листопада", 12: "грудня"
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN не заданий в .env")

TELEGRAM_CHAT_ID_OLEKSANDR = os.getenv("TELEGRAM_CHAT_ID_OLEKSANDR")
if not TELEGRAM_CHAT_ID_OLEKSANDR:
    raise ValueError("TELEGRAM_CHAT_ID_OLEKSANDR не заданий в .env")

# Шлях до CSV файлу (краще використовувати os.path.join для сумісності)
CSV_FILE_PATH = os.path.join("csv", "all_ad.csv")

# Створення директорії csv, якщо вона не існує
os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)

logger = get_logger(__name__)

# Стартовий URL
start_url = "https://www.olx.ua/uk/nedvizhimost/kvartiry/dolgosrochnaya-arenda-kvartir/kiev/?currency=UAH&search%5Bfilter_float_total_area:to%5D=60&search%5Bfilter_enum_furnish%5D%5B0%5D=yes&search%5Bfilter_enum_number_of_rooms_string%5D%5B0%5D=dvuhkomnatnye"

# Список User-Agent заголовків
headers_list = [
    {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"},
    {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"}
]

# --- Функції-хелпери ---
def send_telegram_message_oleksandr(message):
    """Відправляє повідомлення в Telegram."""
    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {
        "chat_id": TELEGRAM_CHAT_ID_OLEKSANDR,
        "text": message,
        "parse_mode": "HTML", # Дозволяє форматування HTML у повідомленнях
        "disable_web_page_preview": True # Можна додати, щоб прибрати прев'ю посилань
    }
    try:
        response = requests.get(telegram_url, params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"Повідомлення успішно відправлено в Telegram.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Помилка відправки повідомлення в Telegram: {e}")


def delay():
    """Робить випадкову затримку між запитами."""
    sleep_time = random.uniform(5, 10) # Використовуємо uniform для нецілих значень
    logger.info(f"Затримка на {sleep_time:.2f} секунд...")
    time.sleep(sleep_time)


def get_html(url):
    """Отримує HTML контент сторінки з випадковим User-Agent."""
    headers = random.choice(headers_list)
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() # Перевірка на HTTP помилки (4xx, 5xx)
        logger.info(f"Успішно отримано HTML з {url}")
        return response.text
    except requests.exceptions.Timeout:
        logger.error(f"Помилка Timeout при запиті до {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Помилка з'єднання з {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Неочікувана помилка при отриманні HTML з {url}: {e}")
        return None

# --- Функції для генерації URL пагінації ---


def generate_olx_pagination_urls(base_url: str, total_pages: int) -> list[str]:
    """
    Генерує список URL-адрес для всіх сторінок пагінації OLX.
    Використовує простий метод додавання '&page=N'.
    """
    if total_pages < 1:
        return []

    urls = [base_url] # Перша сторінка

    if total_pages > 1:
        # Визначаємо, чи є вже '?' у URL
        separator = '&' if '?' in base_url else '?'
        # Базовий URL для додавання параметрів (може бути той самий, що й start_url)
        url_root = base_url

        for page_num in range(2, total_pages + 1):
            page_url = f"{url_root}{separator}page={page_num}"
            # Якщо ми додали перший параметр з '&', замінюємо його на '?'
            # Цей блок потрібен, якщо базовий URL не мав параметрів взагалі
            if separator == '?' and page_num == 2:
                separator = '&' # Наступні параметри будуть додаватися через '&'
            urls.append(page_url)
    return urls


def get_all_olx_urls(start_url: str) -> list[str]:
    """
    Знаходить загальну кількість сторінок і генерує всі URL пагінації.
    """
    logger.info(f"Пошук загальної кількості сторінок, починаючи з: {start_url}")
    first_page_html = get_html(start_url)

    if not first_page_html:
        logger.error("Не вдалося отримати HTML першої сторінки. Повернення порожнього списку.")
        return []

    soup = BeautifulSoup(first_page_html, "lxml")

    pagination_items = soup.select('ul[data-testid="pagination-list"] li[data-testid^="pagination-list-item"]')
    last_page_num = 1

    if pagination_items:
        try:
            last_item_li = pagination_items[-1]
            last_page_anchor = last_item_li.find('a')
            if last_page_anchor and last_page_anchor.text.strip().isdigit():
                 last_page_num = int(last_page_anchor.text.strip())
                 logger.info(f"Знайдено номер останньої сторінки: {last_page_num}")
            else:
                 aria_label = last_item_li.get('aria-label', '')
                 if 'Page' in aria_label:
                     last_page_num = int(aria_label.split()[-1])
                     logger.info(f"Знайдено номер останньої сторінки з aria-label: {last_page_num}")
                 elif len(pagination_items) <= 1 and not soup.find("a", {'data-testid':"pagination-forward"}):
                      last_page_num = 1 # Якщо тільки 1 елемент і немає кнопки "вперед"
                      logger.info("Знайдено лише одну сторінку.")
                 else:
                    logger.warning("Не вдалося точно визначити номер останньої сторінки. Перевірте селектори або структуру пагінації.")
                    # Як запасний варіант, можна спробувати перейти на наступну сторінку і подивитись, чи вона існує,
                    # але це ускладнить логіку. Поки що зупинимось на 1.
                    last_page_num = 1

        except (IndexError, ValueError, TypeError) as e:
            logger.warning(f"Помилка при визначенні останньої сторінки: {e}. Припускаємо, що сторінка одна.")
            last_page_num = 1
    else:
        if soup.find("div", {"data-testid": "no-search-results"}):
             logger.info("Оголошень за запитом не знайдено.")
             return []
        else:
             logger.warning("Блок пагінації не знайдено. Можливо, сторінка лише одна.")
             last_page_num = 1

    logger.info(f"Генерація URL для {last_page_num} сторінок...")
    all_urls = generate_olx_pagination_urls(start_url, last_page_num)
    logger.info(f"Успішно згенеровано {len(all_urls)} URL.")
    return all_urls


# --- Функції для парсингу та обробки даних ---
def scrape_ads_from_page(page_url: str) -> list[dict]:
    """Парсить оголошення з однієї сторінки та повертає список словників."""
    logger.info(f"Парсинг сторінки: {page_url}")
    html_content = get_html(page_url)
    scraped_ads = []

    if not html_content:
        logger.warning(f"Пропуск сторінки через помилку завантаження: {page_url}")
        return scraped_ads

    soup = BeautifulSoup(html_content, "lxml")
    all_apartments_containers = soup.select('div.css-l9drzq')

    if not all_apartments_containers:
        logger.warning(
            f"Не знайдено контейнерів оголошень на сторінці: {page_url}. Перевірте селектор 'div.css-l9drzq'.")

    for item in all_apartments_containers:
        ad_data = {}
        try:
            # Назва
            name_tag = item.select_one("h4, h6")
            ad_data['name'] = name_tag.text.strip() if name_tag else "N/A"

            # --- Змінений блок для Локації та Часу ---
            location_time_tag = item.select_one("p.css-vbz67q, p[data-testid='location-date']")
            time_value = "N/A"  # Значення за замовчуванням
            location_value = "N/A"  # Значення за замовчуванням

            if location_time_tag:
                location_time_text = location_time_tag.text.split(" - ")
                location_value = location_time_text[0].strip() if len(location_time_text) > 0 else "N/A"

                # Обробка часу
                if len(location_time_text) > 1:
                    original_time_text = location_time_text[1].strip()
                    # Перевіряємо, чи текст містить слово "Сьогодні" (ігноруючи регістр)
                    if 'сьогодні' in original_time_text.lower():
                        try:
                            # Отримуємо поточну дату
                            today_date = datetime.now().date()
                            day = today_date.day
                            month_num = today_date.month
                            year = today_date.year
                            # Форматуємо у потрібний рядок з українським місяцем
                            month_name = UKRAINIAN_MONTHS_GENITIVE.get(month_num, f"({month_num})")  # Резервний варіант
                            time_value = f"{day:02d} {month_name} {year} р."  # :02d для дня типу 01, 02...
                            logger.debug(f"Замінено 'Сьогодні' на '{time_value}' для оголошення '{ad_data['name']}'")
                        except Exception as e:
                            logger.warning(
                                f"Не вдалося відформатувати 'Сьогодні' в дату: {e}. Використовується оригінальний текст.")
                            time_value = original_time_text  # Повертаємось до оригінального тексту при помилці
                    else:
                        # Якщо не "Сьогодні", використовуємо оригінальний текст
                        time_value = original_time_text
                # Якщо в split не було другого елемента, time_value залишиться "N/A"
            # Присвоюємо отримані значення
            ad_data['location'] = location_value
            ad_data['time'] = time_value

            # Ціна
            price_tag = item.select_one("p.css-uj7mm0, p[data-testid='price']")
            ad_data['price'] = price_tag.text.strip() if price_tag else "N/A"

            # Площа
            square_tag = item.select_one("span.css-6as4g5")
            ad_data['square'] = square_tag.text.strip() if square_tag else ""

            # Посилання
            link_anchor = item.select_one("a")
            if link_anchor and link_anchor.get('href'):
                relative_link = link_anchor.get('href')
                ad_data['link'] = urllib.parse.urljoin(page_url, relative_link)
            else:
                ad_data['link'] = "N/A"

            if ad_data['name'] != "N/A" and ad_data['link'] != "N/A":
                scraped_ads.append(ad_data)

        except Exception as e:
            logger.error(f"Помилка парсингу окремого оголошення на {page_url}: {e}", exc_info=True)

    logger.info(f"Знайдено {len(scraped_ads)} оголошень на сторінці.")
    return scraped_ads


def read_existing_ad_links(csv_filepath: str) -> set[str]:
    """Читає існуючі посилання на оголошення з CSV файлу."""
    existing_links = set()
    try:
        with open(csv_filepath, "r", encoding="utf-8", newline="") as file:
            reader = csv.reader(file)
            header = next(reader, None) # Пропустити заголовок, якщо він є
            if header: # Перевірка, чи файл не порожній
                 # Визначаємо індекс колонки з посиланням (припускаємо, що це остання колонка)
                link_index = len(header) - 1
                for row in reader:
                    if len(row) > link_index: # Перевірка, чи рядок має достатньо колонок
                        existing_links.add(row[link_index])
    except FileNotFoundError:
        logger.info("CSV файл ще не існує. Буде створено новий.")
    except Exception as e:
        logger.error(f"Помилка читання CSV файлу {csv_filepath}: {e}")
    return existing_links


def save_new_ads_to_csv(csv_filepath: str, new_ads: list[dict]):
    """Додає нові оголошення до CSV файлу."""
    # Визначаємо, чи існує файл і чи він порожній, щоб додати заголовок
    file_exists = os.path.isfile(csv_filepath)
    is_empty = not file_exists or os.path.getsize(csv_filepath) == 0

    try:
        with open(csv_filepath, "a", encoding="utf-8", newline="") as file:
            # Визначаємо порядок колонок (важливо!)
            fieldnames = ['time', 'name', 'location', 'price', 'square', 'link']
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            if is_empty:
                writer.writeheader() # Записуємо заголовок лише якщо файл новий/порожній

            # Записуємо дані нових оголошень
            for ad_dict in new_ads:
                 # Переконуємось, що словник містить всі ключі з fieldnames
                 row_to_write = {key: ad_dict.get(key, '') for key in fieldnames}
                 writer.writerow(row_to_write)
        logger.info(f"Успішно додано {len(new_ads)} нових оголошень до {csv_filepath}")
    except IOError as e:
        logger.error(f"Помилка запису до CSV файлу {csv_filepath}: {e}")
    except Exception as e:
        logger.error(f"Неочікувана помилка при записі до CSV: {e}")


# --- Головна функція ---

def main():
    """Основний процес парсингу."""
    logger.info("===== Запуск парсера OLX =====")

    # 1. Отримати список всіх URL сторінок для парсингу
    all_page_urls = get_all_olx_urls(start_url)
    if not all_page_urls:
        logger.warning("Не вдалося отримати URL сторінок для парсингу. Завершення роботи.")
        send_telegram_message_oleksandr("❌ Не вдалося отримати URL сторінок OLX для парсингу.")
        return

    logger.info(f"Буде оброблено {len(all_page_urls)} сторінок.")

    # 2. Прочитати існуючі посилання з CSV
    existing_links = read_existing_ad_links(CSV_FILE_PATH)
    logger.info(f"Знайдено {len(existing_links)} існуючих посилань у CSV.")

    # 3. Спарсити оголошення з усіх сторінок
    all_scraped_ads = []
    for page_url in all_page_urls:
        ads_from_page = scrape_ads_from_page(page_url)
        all_scraped_ads.extend(ads_from_page)
        if page_url != all_page_urls[-1]: # Не робимо затримку після останньої сторінки
             delay()

    logger.info(f"Всього зібрано {len(all_scraped_ads)} оголошень зі всіх сторінок.")

    # 4. Визначити нові оголошення
    new_ads_found = []
    processed_links_this_run = set() # Щоб уникнути дублів з поточного запуску

    for ad in all_scraped_ads:
        ad_link = ad.get('link')
        # Перевіряємо, чи є посилання, чи воно не N/A, чи його немає в існуючих і чи не обробляли його вже
        if ad_link and ad_link != "N/A" and ad_link not in existing_links and ad_link not in processed_links_this_run:
            new_ads_found.append(ad)
            processed_links_this_run.add(ad_link) # Додаємо до оброблених в цьому запуску

    logger.info(f"Знайдено {len(new_ads_found)} нових оголошень.")

    # 5. Зберегти нові оголошення та відправити в Telegram
    if new_ads_found:
        save_new_ads_to_csv(CSV_FILE_PATH, new_ads_found)
        logger.info(f"Відправка {len(new_ads_found)} нових оголошень в Telegram...")
        send_telegram_message_oleksandr(f"✅ Знайдено нових оголошень: {len(new_ads_found)}")
        for ad in new_ads_found:
            # Формуємо повідомлення з даних словника
            msg = (
                f"🏠 <b>{ad.get('name', 'Без назви')}</b>\n"
                f"📍 {ad.get('location', 'N/A')}\n"
                f"💰 {ad.get('price', 'N/A')}\n"
                f"📏 {ad.get('square', '')}\n" # Якщо square порожній, нічого не виведе
                f"⏰ {ad.get('time', 'N/A')}\n"
                f"🔗 <a href='{ad.get('link')}'>Переглянути на OLX</a>"
            )
            send_telegram_message_oleksandr(msg)
            time.sleep(1) # Невелика затримка між повідомленнями в Telegram
        logger.info("📨 Нові оголошення відправлено.")
    else:
        logger.info("❌ Нових оголошень немає.")
        send_telegram_message_oleksandr("ℹ️ Нових оголошень не знайдено.")

    logger.info("===== Парсер OLX завершив роботу =====")


def start_parsing():
    main()


# --- Точка входу ---
if __name__ == "__main__":
    main()