import redis
import json
import time
import config.config_redis as config_redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
ps = r.pubsub()
ps.subscribe(
    #config_redis.SERVICE_STATUS_CHANNEL,
    #config_redis.WALLET_SYNC_CHANNEL,

    #config_redis.KLINE_UPDATES,
    #config_redis.PRE_PROC_TRADE_CHANNEL,
    #config_redis.ORDER_BOOK_UPDATES,
    config_redis.TRADE_CHANNEL,
    #config_redis.ORDER_BOOK_UPDATES,
    #config_redis.PRE_PROC_KLINE_UPDATES,
    #config_redis.MACRO_METRICS_CHANNEL,
    #config_redis.TRIGGER_QUEUE_CHANNEL,
    #config_redis.KLINE_QUEUE_CHANNEL
    #config_redis.HEARTBEAT_CHANNEL,
    #"current_coins",
)

print("Listening (Press Ctrl+C to stop)...")

try:
    while True:
        message = ps.get_message()
        if message and message["type"] == "message":
            print(json.loads(message["data"]))
        time.sleep(0.01)  # Prevents 100% CPU usage
except KeyboardInterrupt:
    print("\nInterrupted. Exiting cleanly.")
    ps.close()


