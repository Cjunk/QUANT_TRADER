import config.config_ws as config
import config.config_redis as config_redis
import redis
import json
channel_name = config_redis.COIN_CHANNEL
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "DOTUSDT",
    "DOGEUSDT", "XRPUSDT", "BNBUSDT", "LTCUSDT", "LINKUSDT",
    "TRXUSDT", "XLMUSDT", "ATOMUSDT", "ALGOUSDT", "PEPEUSDT",
    "AVAXUSDT", "UNIUSDT", "SUSDT", "NEARUSDT","ONDOUSDT"
]

SYMBOLS2 = ["BTCUSDT", "ONDOUSDT"]
redis_client = redis.Redis(
            host=config_redis.REDIS_HOST,
            port=config_redis.REDIS_PORT,
            db=config_redis.REDIS_DB,
            decode_responses=True
        )

print(f"✅ Publishing messages to Redis channel: {channel_name}")
redis_client.publish(channel_name, json.dumps({"symbols": SYMBOLS}))



