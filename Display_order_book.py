import os
import time
import redis
import argparse
from utils.redis_client import get_latest_trade, get_order_book

# ✅ Redis connection
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

def display_order_book(symbol, depth=200, refresh_rate=0.2):
    """📜 Displays the live order book in the terminal."""
    while True:
        order_book = get_order_book(symbol, depth)

        #if not order_book["bids"] or not order_book["asks"]:
            #print(f"⚠️ No order book data available for {symbol}")
            #time.sleep(1)
            #continue

        # ✅ Sort bids descending, asks ascending
        bids = sorted(order_book["bids"], key=lambda x: x[0], reverse=True)
        asks = sorted(order_book["asks"], key=lambda x: x[0])

        # ✅ Clear screen
        os.system('cls' if os.name == 'nt' else 'clear')
        latest_trade_price = get_latest_trade(symbol)

        # 1) Compute the total volume for bids & asks
        total_bid_volume = sum(b[1] for b in bids)
        total_ask_volume = sum(a[1] for a in asks)
        total_volume = total_bid_volume + total_ask_volume

        if total_volume > 0:
            bid_ratio = (total_bid_volume / total_volume) * 100
            ask_ratio = (total_ask_volume / total_volume) * 100
        else:
            bid_ratio = ask_ratio = 0

        # 2) Compute price spread: best ask - best bid
        best_bid = bids[0][0] if bids else 0
        best_ask = asks[0][0] if asks else 0
        spread = best_ask - best_bid

        # Print summary at the top
        print(f"\n📊 Live Order Book for {symbol} (Top {depth} Levels) | "
              f"Latest Trade: {latest_trade_price}")
        print(f"Spread: {spread:.2f} | Bid %: {bid_ratio:.2f}% | Ask %: {ask_ratio:.2f}%")

        print("┌─────────────── BUY ORDERS ───────────────┐ | ┌─────────────── SELL ORDERS ──────────────┐")
        print("│ Price (USD)       | Volume               │ | │ Price (USD)       | Volume               │")
        print("├───────────────────┼──────────────────────┤ | ├───────────────────┼──────────────────────┤")

        # Now display each row of the book
        for i in range(depth):
            bid_price = f"{bids[i][0]:,.2f}" if i < len(bids) else " "
            bid_volume = f"{bids[i][1]:,.6f}" if i < len(bids) else " "
            ask_price = f"{asks[i][0]:,.2f}" if i < len(asks) else " "
            ask_volume = f"{asks[i][1]:,.6f}" if i < len(asks) else " "
            print(f"│ {bid_price:<17} | {bid_volume:<20} │ | │ {ask_price:<17} | {ask_volume:<20} │")

        print("└───────────────────┴──────────────────────┘ | └───────────────────┴──────────────────────┘")
        print("\n🔄 Updating... (Press Ctrl+C to stop)")

        time.sleep(refresh_rate)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display the live order book for a given symbol.")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Symbol to display (default: BTCUSDT)")
    parser.add_argument("--depth", type=int, default=200, help="Number of order book levels to display (default: 200)")
    parser.add_argument("--refresh_rate", type=float, default=0.5, help="Refresh rate in seconds (default: 0.5)")
    args = parser.parse_args()

    display_order_book(args.symbol, depth=args.depth, refresh_rate=args.refresh_rate)

