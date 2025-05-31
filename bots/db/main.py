# --- runner.py ---
import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import signal
signal.signal(signal.SIGINT, signal.SIG_DFL)
from db_core import PostgresDBBot

if __name__ == "__main__":

    #open('logs/DB_BOT.log', 'w', encoding='utf-8').close()
    bot = PostgresDBBot("DB_BOT.log")
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.logger.info("🛑 Keyboard interrupt received. Stopping DB Bot.")
        bot.stop()
        bot.close()

