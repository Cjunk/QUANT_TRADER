# --- runner.py ---
import sys, os, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from db_core import PostgresDBBot

if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is not activated. Please activate it first.")
        sys.exit(1)

    #open('logs/DB_BOT.log', 'w', encoding='utf-8').close()
    bot = PostgresDBBot("DB_BOT.log")
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.logger.info("🛑 Keyboard interrupt received. Stopping DB Bot.")
        bot.stop()
        bot.close()

