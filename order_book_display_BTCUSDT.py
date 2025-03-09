import os
import time
import redis

redis_client = redis.Redis(host="localhost", 
                port=6379,  # ✅ Fixed!
                db=0, decode_responses=True)


def get_order_book(symbol):
    redis_key_bids = f"orderbook:{symbol}:bids"
    redis_key_asks = f"orderbook:{symbol}:asks"

    bids = redis_client.zrevrange(redis_key_bids, 0, 9, withscores=True)
    asks = redis_client.zrange(redis_key_asks, 0, 9, withscores=True)

    return {
        "bids": [(float(price), volume) for price, volume in bids],
        "asks": [(float(price), volume) for price, volume in asks]
    }

def display_order_book(symbol):
    while True:
        

        order_book = get_order_book(symbol)

        if not order_book["bids"] or not order_book["asks"]:
            print(f"⚠️ No order book data available for {symbol}")
            time.sleep(1)
            continue
        os.system('cls' if os.name == 'nt' else 'clear')  # ✅ Clear screen before displaying
        bids = sorted(order_book["bids"], key=lambda x: x[0], reverse=True)[:20]
        asks = sorted(order_book["asks"], key=lambda x: x[0])[:20]

        print(f"\n📊 Order Book for {symbol} (Top 10 Levels)")
        print("┌─────────────── BUY ORDERS ───────────────┐ | ┌─────────────── SELL ORDERS ──────────────┐")
        print("│ Price (USD)       | Volume               │ | │ Price (USD)       | Volume               │")
        print("├───────────────────┼──────────────────────┤ | ├───────────────────┼──────────────────────┤")

        for i in range(10):
            bid_price = f"{bids[i][0]:,.2f}" if i < len(bids) else " "
            bid_volume = f"{bids[i][1]:,.6f}" if i < len(bids) else " "
            ask_price = f"{asks[i][0]:,.2f}" if i < len(asks) else " "
            ask_volume = f"{asks[i][1]:,.6f}" if i < len(asks) else " "
            print(f"│ {bid_price:<17} | {bid_volume:<20} │ | │ {ask_price:<17} | {ask_volume:<20} │")

        print("└──────────────┴───────────────────────────┘ | └──────────────┴───────────────────────────┘")
        print("\n🔄 Updating... (Press Ctrl+C to stop)")
        
        time.sleep(1)  # ✅ Refresh every second

display_order_book("BTCUSDT")
