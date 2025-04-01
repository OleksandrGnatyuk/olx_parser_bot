import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.types import BotCommand
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import os
from dotenv import load_dotenv

# Завантажуємо токен із .env файлу
load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_OLEKSANDRпше")  # ID адресата

# Логування
logging.basicConfig(level=logging.INFO)

# Ініціалізація бота та диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# Функція для відправлення щоденного повідомлення
async def send_daily_message():
    message = "Добрий ранок! Це ваше щоденне повідомлення."
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
        logging.info(f"Повідомлення надіслано до {CHAT_ID}.")
    except Exception as e:
        logging.error(f"Помилка при відправленні повідомлення: {e}")

# Налаштування планувальника
@dp.startup
async def on_startup():
    # Команди для бота
    await bot.set_my_commands([BotCommand(command="start", description="Запуск бота")])

    # Налаштування планувальника
    scheduler.add_job(send_daily_message, CronTrigger(hour=8, minute=0))  # Щодня о 8:00
    scheduler.start()
    logging.info("Планувальник запущено!")

@dp.shutdown
async def on_shutdown():
    # Вимкнення планувальника
    scheduler.shutdown(wait=False)
    logging.info("Планувальник вимкнено.")

# Обробник для команди /start
@dp.message(commands=["start"])
async def start_command_handler(message: types.Message):
    await message.reply("Бот запущено. Щоденні повідомлення о 8:00 налаштовані!")

# Головна функція для запуску
async def main():
    await dp.start_polling(bot)

# Запуск події
if __name__ == "__main__":
    asyncio.run(main())
