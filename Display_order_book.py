import os
import time
import redis
import argparse
from utils.redis_client import get_latest_trade, get_order_book

# ✅ Redis connection
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

"""
This bot will display the current order book, constantly updating.
It pulls the order book and latest trade data from Redis.
"""

def display_order_book(symbol, depth=20, refresh_rate=0.2):
    """📜 Displays the live order book in the terminal."""
    while True:
        order_book = get_order_book(symbol, depth)

        if not order_book["bids"] or not order_book["asks"]:
            print(f"⚠️ No order book data available for {symbol}")
            time.sleep(1)
            continue

        # ✅ Sort bids descending, asks ascending
        bids = sorted(order_book["bids"], key=lambda x: x[0], reverse=True)
        asks = sorted(order_book["asks"], key=lambda x: x[0])

        # ✅ Clear screen before displaying (optimized for Windows & Linux)
        os.system('cls' if os.name == 'nt' else 'clear')
        latest_trade_price = get_latest_trade(symbol)
        print(f"\n📊 Live Order Book for {symbol} (Top {depth} Levels) | Current Latest Trade: {latest_trade_price}")
        print("┌─────────────── BUY ORDERS ───────────────┐ | ┌─────────────── SELL ORDERS ──────────────┐")
        print("│ Price (USD)       | Volume               │ | │ Price (USD)       | Volume               │")
        print("├───────────────────┼──────────────────────┤ | ├───────────────────┼──────────────────────┤")

        for i in range(depth):
            bid_price = f"{bids[i][0]:,.2f}" if i < len(bids) else " "
            bid_volume = f"{bids[i][1]:,.6f}" if i < len(bids) else " "
            ask_price = f"{asks[i][0]:,.2f}" if i < len(asks) else " "
            ask_volume = f"{asks[i][1]:,.6f}" if i < len(asks) else " "
            print(f"│ {bid_price:<17} | {bid_volume:<20} │ | │ {ask_price:<17} | {ask_volume:<20} │")

        print("└───────────────────┴──────────────────────┘ | └───────────────────┴──────────────────────┘")
        print("\n🔄 Updating... (Press Ctrl+C to stop)")

        time.sleep(refresh_rate)  # ✅ Adjust refresh rate dynamically

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display the live order book for a given symbol.")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Symbol to display (default: BTCUSDT)")
    parser.add_argument("--depth", type=int, default=200, help="Number of order book levels to display (default: 200)")
    parser.add_argument("--refresh_rate", type=float, default=0.5, help="Refresh rate in seconds (default: 0.5)")
    args = parser.parse_args()

    display_order_book(args.symbol, depth=args.depth, refresh_rate=args.refresh_rate)
