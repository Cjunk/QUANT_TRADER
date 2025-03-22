import sys
import time
from utils.logger import setup_logger
from ws.Wallet_service_bot import WalletService
import config.config_ts as config
if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

logger = setup_logger("wallet service.log")
if __name__ == "__main__":
    open(config.LOG_FILENAME, 'w').close()
    logger.info("🚀 Starting Wallet Service Bot...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting Wallet Service Bot...")

    bot = WalletService(logger)
    bot.run()