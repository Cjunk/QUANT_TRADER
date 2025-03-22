import sys
import time
from utils.logger import setup_logger
from ws.trader_supervisor_bot import TradeSupervisor
import config.config_ts as config
if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

logger = setup_logger("trader_supervisor.log")
if __name__ == "__main__":
    open(config.LOG_FILENAME, 'w').close()
    logger.info("🚀 Starting Trader Supervisor Bot...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting Trader Supervisor Bot...")

    bot = TradeSupervisor(logger)
    bot.run()