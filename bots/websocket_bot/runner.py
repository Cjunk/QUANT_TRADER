import signal, sys, time, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from ws_core import WebSocketBot

def main():
    if sys.prefix == sys.base_prefix:
        print("❌ Activate your virtualenv first.")
        sys.exit(1)

    bot = WebSocketBot()

    # Catch Ctrl-C and call bot.stop()
    def handle_sigint(sig, frame):
        bot.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    bot.run()   # blocks until bot.stop() sets running = False

if __name__ == "__main__":
    bot = WebSocketBot()
    try:
        bot.run()
    finally:
        bot.stop()          # in case we exited via exception
