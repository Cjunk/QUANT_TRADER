import sys, time, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from websocket_bot.ws_core import WebSocketBot

if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Please activate virtual environment.")
        sys.exit(1)

    bot = WebSocketBot()
    bot.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        bot.stop()