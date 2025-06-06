import redis
import json
import time
import config.config_redis as config_redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
ps = r.pubsub()
HEARTBEAT_CHANNEL = "heartbeat_channel"
# List of channels to subscribe to (uncomment as needed)
channels = [
    #config_redis.SERVICE_STATUS_CHANNEL,
    #config_redis.WALLET_SYNC_CHANNEL,
    config_redis.REDIS_CHANNEL["spot.kline_out"],
    config_redis.REDIS_CHANNEL["linear.kline_out"],
    config_redis.PRE_PROC_TRADE_CHANNEL,
    config_redis.ORDER_BOOK_UPDATES,
    #config_redis.TRADE_CHANNEL,
    #config_redis.ORDER_BOOK_UPDATES,
    config_redis.PRE_PROC_KLINE_UPDATES,
    #config_redis.MACRO_METRICS_CHANNEL,
    #config_redis.TRIGGER_QUEUE_CHANNEL,
    config_redis.KLINE_QUEUE_CHANNEL,
    #config_redis.HEARTBEAT_CHANNEL,
    #"current_coins",
    #config_redis.REDIS_CHANNEL["spot.trade_out"],
    #config_redis.REDIS_CHANNEL["linear.trade_out"]
]
ps.subscribe(*channels)

print(f"Listening to channels: {channels} (Press Ctrl+C to stop)...")

try:
    while True:
        message = ps.get_message()
        if message and message["type"] == "message":
            channel = message["channel"]
            data = json.loads(message["data"])
            # Prefer market from payload if present
            market = data.get("market")
            if not market:
                # Fallback to channel name if not present in payload
                if channel == config_redis.REDIS_CHANNEL["spot.kline_out"]:
                    market = "spot"
                elif channel == config_redis.REDIS_CHANNEL["linear.kline_out"]:
                    market = "linear"
                elif channel == config_redis.REDIS_CHANNEL["spot.trade_out"]:
                    market = "spot"
                elif channel == config_redis.REDIS_CHANNEL["linear.trade_out"]:
                    market = "linear"
                else:
                    market = "unknown"
            print(f"[{market.upper()}]", data)
        time.sleep(0.01)  # Prevents 100% CPU usage
except KeyboardInterrupt:
    print("\nInterrupted. Exiting cleanly.")
    ps.close()


