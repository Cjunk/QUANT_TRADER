import signal, sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from bots.websocket_bot.ws_core import WebSocketBot

def main():
    if sys.prefix == sys.base_prefix:
        print("❌ Activate your virtualenv first.")
        sys.exit(1)

    # create and start the bots (threads)
    linear_bot = WebSocketBot(market="linear")
    spot_bot = WebSocketBot(market="spot")

    linear_bot.start()
    spot_bot.start()

    # handle Ctrl+C properly for both bots
    def handle_sigint(sig, frame):
        linear_bot.stop()
        spot_bot.stop()
        linear_bot.join()
        spot_bot.join()
        print("✅ Bots stopped gracefully.")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    linear_bot.join()
    spot_bot.join()

if __name__ == "__main__":
    main()


