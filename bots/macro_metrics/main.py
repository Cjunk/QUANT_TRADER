import sys
import time
import os
# Patch sys.path so 'bots' is importable as a package when running as a script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from macro_metrics_services_core import MacroMetricsServices

if __name__ == "__main__":

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting MACRO_METRICS_SERVICES...")

    bot = MacroMetricsServices()
    bot.run()
