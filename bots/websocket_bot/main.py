import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from ws_core import WebSocketBot

def main():


    # create and start the bots (threads)
    linear_bot = WebSocketBot(market="linear")
    spot_bot = WebSocketBot(market="spot")
    #macro_websocket_bot = WebSocketBot(market="spot")

    linear_bot.start()
    spot_bot.start()
    #macro_websocket_bot.start()

    try:
        while linear_bot.is_alive() or spot_bot.is_alive(): #or macro_websocket_bot.is_alive():
            linear_bot.join(timeout=1)
            spot_bot.join(timeout=1)
            #macro_websocket_bot.join(timeout=1)

    except KeyboardInterrupt:
        print("Stopping bots...")
        linear_bot.stop()
        spot_bot.stop()
        #macro_websocket_bot.stop()
        linear_bot.join()
        spot_bot.join()
        #macro_websocket_bot.join()
        print("✅ Bots stopped gracefully.")

if __name__ == "__main__":
    main()


