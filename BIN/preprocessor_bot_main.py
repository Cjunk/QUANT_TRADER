import sys
import time
from utils.logger import setup_logger
from ws.preprocessor_bot_bot import Preprocessor_bot
import config.config_auto_preprocessor_bot as config

if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

logger = setup_logger("PREPROCESSOR_BOT.log")

if __name__ == "__main__":
    open(config.LOG_FILENAME, 'w').close()
    logger.info("🚀 Starting PREPROCESSOR_BOT...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting PREPROCESSOR_BOT...")

    bot = Preprocessor_bot(logger)
    bot.run()
