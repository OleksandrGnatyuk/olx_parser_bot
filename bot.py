import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties # Для парсингу HTML
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
# Можна також використовувати IntervalTrigger для інтервалів або DateTrigger для конкретної дати
# from apscheduler.triggers.interval import IntervalTrigger

from main import start_parsing


load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID_OLEKSANDR = os.getenv("TELEGRAM_CHAT_ID_OLEKSANDR")

if not BOT_TOKEN:
    logging.critical("Помилка: Не встановлено змінну оточення BOT_TOKEN!")
    sys.exit(1)
if not TELEGRAM_CHAT_ID_OLEKSANDR:
    logging.warning("Увага: Не встановлено TELEGRAM_CHAT_ID_OLEKSANDR. Заплановані повідомлення не надсилатимуться.")
    # Можна завершити роботу, якщо ADMIN_CHAT_ID обов'язковий
    sys.exit(1)
else:
    try:
        TELEGRAM_CHAT_ID_OLEKSANDR = int(TELEGRAM_CHAT_ID_OLEKSANDR)
    except ValueError:
        logging.critical(f"Помилка: ADMIN_CHAT_ID '{TELEGRAM_CHAT_ID_OLEKSANDR}' не є дійсним цілим числом!")
        sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Вказуємо parse_mode за замовчуванням для всіх повідомлень бота
bot_properties = DefaultBotProperties(parse_mode=ParseMode.HTML)
bot = Bot(token=BOT_TOKEN, default=bot_properties)
dp = Dispatcher()

# Вказуємо часову зону для коректної роботи CronTrigger
scheduler = AsyncIOScheduler(timezone="Europe/Kiev")


# --- Запланована функція ---
async def send_scheduled_message(bot_instance: Bot, chat_id: int):
    """
    Функція, яка буде виконуватися за розкладом.
    """

    try:
        await start_parsing()
        logging.info(f"Звіт про наявність нових оголошень надіслано до чату {chat_id}")
    except Exception as e:
        # Обробка можливих помилок (наприклад, бот заблокований користувачем)
        logging.error(f"Не вдалося надіслати заплановане повідомлення до {chat_id}: {e}")


async def on_startup(bot: Bot): # Приймаємо аргумент типу Bot
    logging.info("Бот запускається...")
    # Видаляємо вебхук, якщо він був встановлений раніше
    await bot.delete_webhook(drop_pending_updates=True) # Використовуємо 'bot'
    logging.info("Попередній вебхук (якщо був) видалено.")

    if TELEGRAM_CHAT_ID_OLEKSANDR:
        # Додаємо завдання до планувальника
        try:
            scheduler.add_job(
                send_scheduled_message,
                trigger=CronTrigger(hour=13, minute=30, timezone="Europe/Kiev"),
                # Передаємо саме 'bot' в kwargs
                kwargs={'bot_instance': bot, 'chat_id': TELEGRAM_CHAT_ID_OLEKSANDR},
                id='daily_message_job',
                replace_existing=True,
                misfire_grace_time=60
            )
            logging.info(f"Заплановано надсилання щоденного повідомлення до чату {TELEGRAM_CHAT_ID_OLEKSANDR} об 13:30.")
        except Exception as e:
             logging.error(f"Помилка при додаванні завдання до планувальника: {e}")

        # Запускаємо планувальник
        if not scheduler.running: # Перевірка, чи не запущено вже
             scheduler.start()
             logging.info("Планувальник завдань запущено.")
        else:
             logging.info("Планувальник вже запущено.")
    else:
        logging.warning("ADMIN_CHAT_ID не вказано, заплановані завдання не додано.")


async def on_shutdown(bot: Bot): # Приймаємо аргумент типу Bot
    logging.warning('Зупинка бота...')
    # Зупиняємо планувальник
    if scheduler.running:
        scheduler.shutdown(wait=True)
        logging.info("Планувальник завдань зупинено.")
    # Не потрібно явно закривати сесію bot при polling, aiogram це зробить
    logging.info("Бот зупинено.")


@dp.message(CommandStart())
async def start_command_handler(message: types.Message):
    await message.reply("Бот запущено. Щоденні повідомлення о 8:00 налаштовані!")


async def main():
    # Реєструємо функції startup/shutdown
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Запускаємо long polling
    logging.info("Запуск polling...")
    # Передаємо екземпляр бота до start_polling
    # Також передаємо будь-які інші аргументи, які ми хочемо мати доступними
    # в обробниках startup/shutdown через `dispatcher['key']` або як аргументи функцій
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот зупинено.")
    except Exception as e:
        logging.critical(f"Критична помилка під час роботи бота: {e}", exc_info=True)
