import csv
import requests
from bs4 import BeautifulSoup

TELEGRAM_BOT_TOKEN = "7393596141:AAGxT554RzqwyNhM6vYs-aEuMD_y4esHIrA"
TELEGRAM_CHAT_ID = "1758404196"


CSV_FILE_PATH = "csv/all_ad.csv"


main_url = "https://www.olx.pl/nieruchomosci/mieszkania/sprzedaz/lodz/?search%5Bfilter_enum_rooms%5D%5B0%5D=two&search%5Bfilter_enum_rooms%5D%5B1%5D=three&search%5Bfilter_float_m%3Afrom%5D=40&search%5Bfilter_float_price%3Afrom%5D=100000&search%5Bfilter_float_price%3Ato%5D=350000&search%5Border%5D=created_at%3Adesc"


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Content-Type": "text/html",
}


response = requests.get(main_url, headers=headers)
soup = BeautifulSoup(response.text, "lxml")


list_of_ap = []
all_apartments = soup.find_all("div", class_="css-l9drzq")

for items in all_apartments:
    name_of_ad = items.find("h4").text.strip()
    local_time = items.find("p", class_="css-1mwdrlh").text.split(" - ")
    time = local_time[1]
    local = local_time[0]
    price = items.find("p", class_="css-6j1qjp").text.strip()
    square = items.find("span", class_="css-6as4g5").text.strip()

    link_tag = items.find("div", class_="css-u2ayx9").find("a")
    link = f"https://olx.pl{link_tag.get('href')}" if link_tag.get("href").startswith("/") else link_tag.get("href")

    list_of_ap.append([time, name_of_ad, local, price, square, link])


try:
    with open(CSV_FILE_PATH, "r", encoding="utf8") as file:
        reader = csv.reader(file)
        existing_rows = list(reader)
except FileNotFoundError:
    existing_rows = []


new_ads = []

with open(CSV_FILE_PATH, "a", encoding="utf8", newline="") as file:
    writer = csv.writer(file)

    for row in list_of_ap:
        if row not in existing_rows:
            writer.writerow(row)
            new_ads.append(row)



def send_telegram_message(message):
    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }
    requests.get(telegram_url, params=params)



if new_ads:
    for ad in new_ads:
        msg = f"🏠 <b>{ad[1]}</b>\n📍 {ad[2]}\n💰 {ad[3]}\n📏 {ad[4]}\n⏰ {ad[0]}\n🔗 <a href='{ad[5]}'>Ссылка</a>"
        send_telegram_message(msg)

    print(f"📨 Отправлено {len(new_ads)} новых объявлений в Telegram!")
else:
    print("❌ Новых объявлений нет.")
