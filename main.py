import sys
import time
from utils.redis_client import get_latest_trade, get_order_book
from ws.websocket_bot import WebSocketBot
from utils.logger import setup_logger
import config.config_ws as config
# ✅ Ensure script runs in venv
if sys.prefix == sys.base_prefix:
    print("❌ Virtual environment is NOT activated! Please activate it first.")
    sys.exit(1)

# ✅ Initialize logging
#logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(message)s")
logger = setup_logger("main.log")
if __name__ == "__main__":
    logger.info("🚀 Starting WebSocket Bot...")
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}🚀 Starting WebSocket Bot...")
    bot = WebSocketBot()  # ✅ Initialize WebSocket bot
    bot.start()  # 🚀 Start WebSocket connection
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')}🚀 Websocket bot is running...")
    try:
        while True:
            # ✅ Example: Fetch and display latest BTC price every 10 seconds
            #btc_price = get_latest_trade("BTCUSDT")
            #print(f"📊 Latest BTC Trade Price: {btc_price}")

            if config.DEBUG_ORDER_BOOK:
                order_book = get_order_book("BTCUSDT")
                if order_book:
                    logger.info(f"📜 BTC Order Book (Top XX Levels): {order_book}")

            time.sleep(1)  # ✅ Fetch data every X seconds
    except KeyboardInterrupt:
        logger.info("🛑 Stopping WebSocket Bot...")
        bot._stop()  # ⏹ Stop the bot on Ctrl+C

