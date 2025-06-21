import redis
import time
import logging

class RedisHandler:
    def __init__(self, config, logger: logging.Logger, service_name="RedisHandler", debug=False):
        self.config = config
        self.logger = logger
        self.client = None
        self.pubsub = None
        self.service_name = service_name
        self.debug = debug

    def connect(self):
        if self.debug:
            self.logger.debug(f"[{self.service_name}] 🔌 Attempting Redis connection to {self.config.REDIS_HOST}:{self.config.REDIS_PORT}/{self.config.REDIS_DB}")
        while True:
            try:
                
                self.client = redis.Redis(
                    host=self.config.REDIS_HOST,
                    port=self.config.REDIS_PORT,
                    db=self.config.REDIS_DB,
                    decode_responses=True,
                    socket_keepalive=True,
                    retry_on_timeout=True
                )
                self.client.ping()  # ✅ This forces a real connection attempt
                self.pubsub = self.client.pubsub()
                self.logger.info(f"[{self.service_name}] ✅ Connected to Redis at {self.config.REDIS_HOST}:{self.config.REDIS_PORT}")
                break
            except redis.ConnectionError as e:
                self.logger.warning(f"[{self.service_name}] ❌ Redis unavailable at {self.config.REDIS_HOST}:{self.config.REDIS_PORT}, retrying in 5 seconds... ({e})")
                time.sleep(5)

    def subscribe(self, channels):
        self.pubsub = self.client.pubsub()
        self.pubsub.subscribe(*channels)
        self.logger.info(f"[{self.service_name}] Subscribed to Redis channels: {channels}")
        if self.debug:
            self.logger.debug(f"[{self.service_name}] Subscribed to channels (debug): {channels}")

    def get_message(self, timeout=1):
        if self.debug:
            self.logger.debug(f"[{self.service_name}] Waiting for Redis message with timeout={timeout}")
        return self.pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)

    def publish(self, channel, message):
        if self.debug:
            self.logger.debug(f"[{self.service_name}] Publishing to Redis channel '{channel}': {message}")
        self.client.publish(channel, message)

    def close(self):
        if self.pubsub:
            if self.debug:
                self.logger.debug(f"[{self.service_name}] Closing Redis pubsub connection")
            self.pubsub.close()