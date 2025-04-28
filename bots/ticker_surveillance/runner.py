import sys
import time
from utils.logger import setup_logger
from ws.ticker_surveillance_bot_bot import TickerSurveillanceBot
import config.config_auto_tickersurveillancebot as config

if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

#logger = setup_logger("TICKER SURVEILLANCE BOT.log")

if __name__ == "__main__":
    #open(config.LOG_FILENAME, 'w').close()
    logger.info("🚀 Starting TICKER SURVEILLANCE BOT...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting TICKER SURVEILLANCE BOT...")

    bot = TickerSurveillanceBot(logger)
    bot.run()