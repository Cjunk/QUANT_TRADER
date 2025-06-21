import redis
import json

# === CONFIG ===
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
QUEUE_NAME_MAP = {
    "spot": "spot_coin_subscriptions",
    "linear": "linear_coin_subscriptions"
}
OWNER = "dev_cli"

# === SETUP REDIS ===
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

def send_command(action, market, symbols, topics):
    queue_name = QUEUE_NAME_MAP.get(market, "spot_coin_subscriptions")
    msg = {
        "action": action,
        "owner": OWNER,
        "market": market,
        "symbols": symbols,
        "topics": topics
    }
    r.lpush(queue_name, json.dumps(msg))
    print(f"✅ Sent to {queue_name}: {msg}")

def show_menu():
    while True:
        print("\n=== Subscription CLI ===")
        print("1. SET symbols")
        print("2. ADD symbol")
        print("3. REMOVE symbol")
        print("4. Quit")

        choice = input("Select an option: ").strip()

        if choice in ["1", "2", "3"]:
            market = input("Enter market type (spot/linear) [default: spot]: ").strip().lower() or "spot"
            symbols_input = input("Enter symbol(s) (comma-separated): ").upper()
            symbols = [s.strip() for s in symbols_input.split(",")]

            topics_input = input("Enter topics (comma-separated, e.g. trade,kline.1) [default: all]: ").strip()
            topics = [t.strip() for t in topics_input.split(",")] if topics_input else ["kline.1", "kline.5", "kline.60", "kline.D", "orderbook.200", "trade"]

            action_map = {"1": "set", "2": "add", "3": "remove"}
            send_command(action_map[choice], market, symbols, topics)

        elif choice == "4":
            print("👋 Exiting CLI")
            break
        else:
            print("❌ Invalid option")

if __name__ == "__main__":
    show_menu()

