import config.config_ws as config
import redis
import json
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT", "DOTUSDT",
    "DOGEUSDT", "XRPUSDT", "BNBUSDT", "LTCUSDT", "LINKUSDT",
    "TRXUSDT", "XLMUSDT", "ATOMUSDT", "ALGOUSDT", "PEPEUSDT",
    "AVAXUSDT", "UNIUSDT", "SUSDT", "NEARUSDT", "ICPUSDT"
]
redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True
        )
channel_name = config.COIN_CHANNEL
print(f"✅ Publishing messages to Redis channel: {channel_name}")
redis_client.publish(channel_name, json.dumps({"symbols": SYMBOLS}))



