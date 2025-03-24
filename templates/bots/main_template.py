import sys
import time
from utils.logger import setup_logger
from ws.{{BOT_MODULE}}_bot import {{BOT_CLASS}}
import config.{{CONFIG_MODULE}} as config

if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

logger = setup_logger("{{LOGGER_NAME}}.log")

if __name__ == "__main__":
    open(config.LOG_FILENAME, 'w').close()
    logger.info("🚀 Starting {{LOGGER_NAME}}...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting {{LOGGER_NAME}}...")

    bot = {{BOT_CLASS}}(logger)
    bot.run()
