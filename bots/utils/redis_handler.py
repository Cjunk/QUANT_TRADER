import redis
import time
import logging

class RedisHandler:
    def __init__(self, config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client = None
        self.pubsub = None

    def connect(self):
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
                self.client.ping()
                self.logger.info("Connected to Redis.")
                break
            except redis.ConnectionError:
                self.logger.warning("Redis unavailable, retrying in 5 seconds...")
                time.sleep(5)

    def subscribe(self, channels):
        self.pubsub = self.client.pubsub()
        self.pubsub.subscribe(*channels)
        self.logger.info(f"Subscribed to Redis channels: {channels}")

    def get_message(self, timeout=1):
        return self.pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)

    def publish(self, channel, message):
        self.client.publish(channel, message)

    def close(self):
        if self.pubsub:
            self.pubsub.close()