import time
import os
from utils.redis_client import get_order_book, get_latest_trade

def bucket_orders(orders, bucket_size=10):
    """
    Group orders into buckets of size `bucket_size`.
    E.g. price=86213 => bucket floor=86210 if bucket_size=10.
    Returns a dict {bucket_floor: total_volume}.
    """
    bucketed = {}
    for price, volume in orders:
        floor_price = int(price // bucket_size) * bucket_size
        bucketed[floor_price] = bucketed.get(floor_price, 0) + volume
    return bucketed

def display_book_summary(symbol, bucket_size=10, refresh=2):
    """
    - Fetches all bids/asks (depth=9999).
    - Buckets them in `bucket_size` increments.
    - Combines all floors from both sides (so they align side-by-side).
    - Skips rows with zero volume on both sides (so no 'pure zero' rows appear).
    - Prints from highest to lowest bucket.
    """
    while True:
        # Get *all* levels
        order_book = get_order_book(symbol, depth=9999)
        bids = order_book["bids"]
        asks = order_book["asks"]
        
        if not bids and not asks:
            print(f"No order book data for {symbol}. Retrying...")
            time.sleep(refresh)
            continue
        
        # Bucket them
        bid_buckets = bucket_orders(bids, bucket_size)
        ask_buckets = bucket_orders(asks, bucket_size)
        
        # Combine all floors to ensure alignment
        all_floors = sorted(set(bid_buckets.keys()) | set(ask_buckets.keys()), reverse=True)
        
        # Clear screen
        os.system("cls" if os.name == "nt" else "clear")
        
        # Summaries
        latest_price = get_latest_trade(symbol)
        total_bids = sum(bid_buckets.values())
        total_asks = sum(ask_buckets.values())
        grand_total = total_bids + total_asks if (total_bids + total_asks) else 1
        
        print(f"--- {symbol} Book Summary (Buckets of ${bucket_size}) ---")
        print(f"Latest Trade Price: {latest_price}")
        print(f"Total Bids: {total_bids:.4f} | Total Asks: {total_asks:.4f}")
        print(f"Bids vs Asks %: {100 * total_bids / grand_total:.1f}% vs {100 * total_asks / grand_total:.1f}%\n")
        
        # Header
        print(f"{'BUCKET RANGE':>20} | {'BID VOL':>12} || {'ASK VOL':>12}")
        print("-" * 56)
        
        # Go through each bucket (descending)
        for floor_price in all_floors:
            bvol = bid_buckets.get(floor_price, 0)
            avol = ask_buckets.get(floor_price, 0)
            
            # If both sides are zero, skip printing (no pure-zero rows)
            if bvol == 0 and avol == 0:
                continue
            
            bucket_range = f"[{floor_price}–{floor_price+bucket_size-1}]"
            print(f"{bucket_range:>20} | {bvol:>12.4f} || {avol:>12.4f}")
        
        print("\n(Press Ctrl+C to stop)")
        time.sleep(refresh)

if __name__ == "__main__":
    display_book_summary(symbol="BTCUSDT", bucket_size=10, refresh=2)




