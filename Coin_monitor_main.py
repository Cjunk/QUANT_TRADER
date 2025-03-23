import sys
import time
from utils.logger import setup_logger
from ws.coin_monitor_bot import CoinMonitor
import config.config_ts as config
if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

logger = setup_logger("coin_monitor_bot.log")
if __name__ == "__main__":
    open(config.LOG_FILENAME, 'w').close()
    logger.info("🚀 Starting Coin Monitor Bot...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting Coin Monitor Bot...")

    bot = CoinMonitor(logger)
    bot.run()