
import redis

# Connect to Redis
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

symbol = "BTCUSDT"  # Change if necessary

# Retrieve top bid and ask
top_bid = redis_client.zrevrange(f"orderbook:{symbol}:bids", 0, 0, withscores=True)
top_ask = redis_client.zrange(f"orderbook:{symbol}:asks", 0, 0, withscores=True)

# Retrieve full order book (adjust range for size limits)
bids = redis_client.zrevrange(f"orderbook:{symbol}:bids", 0, 9, withscores=True)
asks = redis_client.zrange(f"orderbook:{symbol}:asks", 0, 9, withscores=True)

print(f"🔹 Top Bid: {top_bid}")
print(f"🔹 Top Ask: {top_ask}")

print("\n🔹 Order Book Snapshot (Top 10 Levels)")
print("📈 Bids (Buy Orders):")
for price, volume in bids:
    print(f"Price: {price}, Volume: {volume}")

print("\n📉 Asks (Sell Orders):")
for price, volume in asks:
    print(f"Price: {price}, Volume: {volume}")
