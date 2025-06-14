import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from preprocessor_core import PreprocessorBot

if __name__ == "__main__":

    #open('logs/DB_BOT.log', 'w').close()
    print("About to create PreprocessorBot")
    bot = PreprocessorBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.logger.info("🛑 Keyboard interrupt received. Stopping DB Bot.")
        bot.stop()
        bot.close()