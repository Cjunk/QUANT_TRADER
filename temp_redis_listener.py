# debug_redis_listener.py
import redis
import json
import config.config_redis as config_redis

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
ps = r.pubsub()
ps.subscribe(config_redis.SERVICE_STATUS_CHANNEL, "heartbeat_channel")

print("Listening...")
for message in ps.listen():
    if message["type"] == "message":
        print(json.loads(message["data"]))
