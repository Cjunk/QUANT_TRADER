import sys
import time
from utils.logger import setup_logger
from ws.macro_metrics_services_bot import Macro_metrics_services
import config.config_auto_macro_metrics_services as config

if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

logger = setup_logger("MACRO_METRICS_SERVICES.log")

if __name__ == "__main__":
    open(config.LOG_FILENAME, 'w').close()
    logger.info("🚀 Starting MACRO_METRICS_SERVICES...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting MACRO_METRICS_SERVICES...")

    bot = Macro_metrics_services(logger)
    bot.run()
