import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from preprocessor.preprocessor_core import PreprocessorBot

if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is not activated. Please activate it first.")
        sys.exit(1)

    #open('logs/DB_BOT.log', 'w').close()
    bot = PreprocessorBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.logger.info("🛑 Keyboard interrupt received. Stopping DB Bot.")
        bot.stop()
        bot.close()