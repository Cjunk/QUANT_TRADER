# --- runner.py ---

import sys
import os
import time
import threading
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from trigger_core import TriggerBot  # Updated to reflect your new structure

if __name__ == "__main__":
    #if sys.prefix == sys.base_prefix:
       # print("❌ Virtual environment is NOT activated. Please activate it first.")
       # sys.exit(1)

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting TRIGGER_BOT...")

    # Instantiate both bots
    bot_linear = TriggerBot("linear")
    bot_spot = TriggerBot("spot")

    # Start each in its own thread
    t1 = threading.Thread(target=bot_linear.run)
    t2 = threading.Thread(target=bot_spot.run)

    t1.start()
    t2.start()

    # Wait for both to complete (blocks forever unless stopped)
    t1.join()
    t2.join()
