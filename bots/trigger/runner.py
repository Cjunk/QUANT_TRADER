# --- runner.py ---

import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from trigger.trigger_core import TriggerBot  # Updated to reflect your new structure

if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is NOT activated! Please activate it first.")
        sys.exit(1)

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting TRIGGER_BOT...")
    bot = TriggerBot()

    try:
        bot.run()
    except KeyboardInterrupt:
        print("🛑 Keyboard interrupt received. Stopping Trigger Bot.")
        bot.running = False
