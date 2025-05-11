import sys
import time
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
#from utils.logger import setup_logger
from macro_metrics_services_core import Macro_metrics_services
import config.config_auto_macro_metrics_services as config


if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

#logger = setup_logger("MACRO_METRICS_SERVICES.log")

if __name__ == "__main__":
    #open(config.LOG_FILENAME, 'w').close()
    #logger.info("🚀 Starting MACRO_METRICS_SERVICES...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting MACRO_METRICS_SERVICES...")

    bot = Macro_metrics_services()
    bot.run()
