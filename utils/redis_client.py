import redis
import json
import config.config_ws as config

# ✅ Connect to Redis
redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True)

def get_latest_trade(symbol):
    """🔍 Fetch the latest trade price from Redis."""
    return redis_client.get(f"latest_trade:{symbol}")

def get_order_book(symbol, depth=200):
    """📡 Fetch the latest order book from Redis (Top N levels)."""
    #redis_client.ping()  # ✅ Ensure connection is alive

    redis_key_bids = f"orderbook:{symbol}:bids"
    redis_key_asks = f"orderbook:{symbol}:asks"

    # ✅ Fetch from Redis
    bids = redis_client.zrevrange(redis_key_bids, 0, depth - 1, withscores=True)
    asks = redis_client.zrange(redis_key_asks, 0, depth - 1, withscores=True)

    # ✅ Ensure data is formatted correctly
    bids = sorted([(float(price), float(volume)) for price, volume in bids if float(volume) > 0], reverse=True)
    asks = sorted([(float(price), float(volume)) for price, volume in asks if float(volume) > 0])
    return {"bids": bids, "asks": asks}