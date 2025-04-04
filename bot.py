import asyncio
from logger_ import get_logger
import os
import sys

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart

from aiogram.webhook.aiohttp_server import SimpleRequestHandler

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from kyiv_rent_to_telegram import start_parsing
from aiohttp import web

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID_OLEKSANDR = os.getenv("TELEGRAM_CHAT_ID_OLEKSANDR")
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN")

logger = get_logger(__name__)

if not BOT_TOKEN:
    logger.critical("Помилка: Не встановлено змінну оточення BOT_TOKEN!")
    sys.exit(1)
if not TELEGRAM_CHAT_ID_OLEKSANDR:
    logger.warning("Увага: Не встановлено TELEGRAM_CHAT_ID_OLEKSANDR. Заплановані повідомлення не надсилатимуться.")
    sys.exit(1)
else:
    try:
        TELEGRAM_CHAT_ID_OLEKSANDR = int(TELEGRAM_CHAT_ID_OLEKSANDR)
    except ValueError:
        logger.critical(f"Помилка: CHAT_ID '{TELEGRAM_CHAT_ID_OLEKSANDR}' не є дійсним цілим числом!")
        sys.exit(1)

if not RAILWAY_PUBLIC_DOMAIN:
    logger.critical("Помилка: Не встановлено змінну оточення RAILWAY_PUBLIC_DOMAIN!")
    sys.exit(1)

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"https://{RAILWAY_PUBLIC_DOMAIN}{WEBHOOK_PATH}"

bot_properties = DefaultBotProperties(parse_mode=ParseMode.HTML)
bot = Bot(token=BOT_TOKEN, default=bot_properties)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="Europe/Kiev")


async def send_scheduled_message(bot_instance: Bot, chat_id: int):
    """
    Функція, яка буде виконуватися за розкладом.
    """
    try:
        await start_parsing()
        logger.info(f"Звіт про наявність нових оголошень надіслано до чату {chat_id}")
    except Exception as e:
        logger.error(f"Не вдалося надіслати заплановане повідомлення до {chat_id}: {e}")


async def on_startup(bot: Bot):
    logger.info("Бот запускається...")
    try:
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url != WEBHOOK_URL:
            await bot.set_webhook(WEBHOOK_URL)
            logger.info(f"Вебхук встановлено на: {WEBHOOK_URL}")
        else:
            logger.info("Вебхук вже встановлено.")
    except Exception as e:
        logger.error(f"Помилка при встановленні вебхука: {e}")

    if TELEGRAM_CHAT_ID_OLEKSANDR:
        try:
            scheduler.add_job(
                send_scheduled_message,
                trigger=CronTrigger(hour=13, minute=30, timezone="Europe/Kiev"),
                kwargs={'bot_instance': bot, 'chat_id': TELEGRAM_CHAT_ID_OLEKSANDR},
                id='daily_message_job',
                replace_existing=True,
                misfire_grace_time=60
            )
            logger.info(f"Заплановано надсилання щоденного повідомлення до чату {TELEGRAM_CHAT_ID_OLEKSANDR} об 13:30.")
            if not scheduler.running:
                scheduler.start()
                logger.info("Планувальник завдань запущено.")
            else:
                logger.info("Планувальник вже запущено.")
        except Exception as e:
            logger.error(f"Помилка при додаванні завдання до планувальника: {e}")
    else:
        logger.warning("ADMIN_CHAT_ID не вказано, заплановані завдання не додано.")


async def on_shutdown(bot: Bot):
    logger.warning('Зупинка бота...')
    if scheduler.running:
        scheduler.shutdown(wait=True)
        logger.info("Планувальник завдань зупинено.")
    try:
        await bot.delete_webhook()
        logger.info("Вебхук видалено.")
    except Exception as e:
        logger.error(f"Помилка при видаленні вебхука: {e}")
    logger.info("Бот зупинено.")


@dp.message(CommandStart())
async def start_command_handler(message: types.Message):
    await message.reply("Бот запущено. Щоденні повідомлення о 13:30 налаштовані!")


async def main():
    # Реєструємо функції startup/shutdown
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Створюємо веб-застосунок для обробки вебхуків
    app = web.Application()
    webhook_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
    )
    # Реєструємо обробник для вебхука за вказаним шляхом
    webhook_handler.register(app, path=WEBHOOK_PATH)

    # Запускаємо веб-сервер
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=int(os.environ.get("PORT", 8080))) # Railway надає порт через змінну оточення PORT
    await site.start()
    logger.info(f"Веб-сервер запущено на порту {os.environ.get('PORT', 8080)}")

    # Встановлюємо вебхук при старті бота (перенесено сюди)
    await dp.emit_startup(bot) # Спробуємо залишити тут

    # Тримаємо застосунок запущеним (замість polling)
    try:
        while True:
            await asyncio.sleep(3600) # Спимо годину, щоб тримати main task живою
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        await dp.emit_shutdown()
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот зупинено.")
    except Exception as e:
        logger.critical(f"Критична помилка під час роботи бота: {e}", exc_info=True)