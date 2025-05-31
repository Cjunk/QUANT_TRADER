import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from trade_core import TradeSupervisor

if __name__ == "__main__":
    bot = TradeSupervisor()
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.logger.info("🛑 Keyboard interrupt received. Stopping DB Bot.")
        bot.stop()
        bot.close()
