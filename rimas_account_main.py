import sys
import time
from utils.logger import setup_logger
from ws.rimas_account_bot import RimasAccount
import config.config_auto_rimasaccount as config

if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

logger = setup_logger("RIMAS ACCOUNT.log")

if __name__ == "__main__":
    open(config.LOG_FILENAME, 'w').close()
    logger.info("🚀 Starting RIMAS ACCOUNT...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting RIMAS ACCOUNT...")

    bot = RimasAccount(logger)
    bot.run()
