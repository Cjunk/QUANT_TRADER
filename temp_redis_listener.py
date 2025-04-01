import redis
import json
import config.config_redis as config_redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
ps = r.pubsub()
ps.subscribe(
    config_redis.SERVICE_STATUS_CHANNEL,
    config_redis.HEARTBEAT_CHANNEL,
    config_redis.COIN_FEED_AUTO,
    config_redis.COIN_CHANNEL,
    config_redis.TRADE_CHANNEL,
    config_redis.ORDER_BOOK_UPDATES,
    config_redis.KLINE_UPDATES
)

print("Listening...")
try:
    for message in ps.listen():
        if message["type"] == "message":
            print(json.loads(message["data"]))
except KeyboardInterrupt:
    print("\nInterrupted. Unsubscribing and exiting...")
    ps.close()

