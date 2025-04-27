# --- runner.py ---

import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from preprocessor_bot.preprocessor_core import PreprocessorBot

if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is NOT activated. Please activate it first.")
        sys.exit(1)

    open('logs/PREPROCESSOR_BOT.log', 'w').close()
    bot = PreprocessorBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.logger.info("🛑 Keyboard interrupt received. Stopping Preprocessor Bot.")
        bot._stop()
