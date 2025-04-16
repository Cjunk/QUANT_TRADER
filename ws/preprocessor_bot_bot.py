# 📦 Authenticated Bot Template
# Location: templates/authenticated_bot_template/
# The purpose of this bot is to be the direct receiver and processor of the websocket data from the exchange.
# It will be used to process the data and send it to the database bot for storage.
# It will also be used to send the data to the Redis server for other bots to use.
# Created by Jericho Sharman on 2025-03-21

import time,os
import threading
import json
import redis
import datetime
import config.config_redis as config_redis
from utils.logger import setup_logger
from config.config_redis import PRE_PROC_KLINE_UPDATES,PRE_PROC_TRADE_CHANNEL,PRE_PROC_ORDER_BOOK_UPDATES,KLINE_UPDATES,TRADE_CHANNEL,ORDER_BOOK_UPDATES
from config.config_auto_preprocessor_bot import BOT_NAME, BOT_AUTH_TOKEN, HEARTBEAT_INTERVAL


class Preprocessor_bot:
    def __init__(self, logger):
        self.bot_name = BOT_NAME
        self.auth_token = BOT_AUTH_TOKEN
        self.logger = logger
        self.redis = redis.Redis(
            host=config_redis.REDIS_HOST,
            port=config_redis.REDIS_PORT,
            db=config_redis.REDIS_DB,
            decode_responses=True
        )
        self.running = True
        self.heartbeat_interval = HEARTBEAT_INTERVAL  # seconds

    def register(self): # Register the bot with Redis
        payload = {
            "bot_name": self.bot_name,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": "1.0",
                "pid": os.getpid(),
                "description": "Preprocessor bot for processing websocket data."
            }
            
        }
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"🔐 Registered bot '{self.bot_name}' with status 'started'.")

    def heartbeat(self):
        while self.running:
            try:
                payload = {
                    "bot_name": self.bot_name,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat()
                }
                self.redis.publish(config_redis.HEARTBEAT_CHANNEL, json.dumps(payload))
                self.logger.debug("❤️ Sent heartbeat.")
            except Exception as e:
                self.logger.warning(f"Heartbeat failed: {e}")
            time.sleep(self.heartbeat_interval)

    def stop(self):
        self.running = False
        self.logger.info("🛑 Bot is shutting down...")
        payload = {
            "bot_name": self.bot_name,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"✅ Sent shutdown status for bot '{self.bot_name}'.")

    def run(self):
        self.logger.info(f"🚀 {self.bot_name} is running...")
        self.register()
        self._setup_redis()  # <--- Add this line!
        threading.Thread(target=self.heartbeat, daemon=True).start()
        threading.Thread(target=self._listen_redis, daemon=True).start()  # <--- Add this line!

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("👋 Bot process exited.")

    def _setup_redis(self):
        while True:
            try:
                self.redis_client = redis.Redis(
                    host=config_redis.REDIS_HOST,
                    port=config_redis.REDIS_PORT,
                    db=config_redis.REDIS_DB,
                    decode_responses=True,
                    socket_keepalive=True,
                    retry_on_timeout=True
                )
                self.redis_client.ping()
                self.logger.info("Connected to Redis.")
                break
            except redis.ConnectionError:
                self.logger.warning("Redis unavailable, retrying in 5 seconds...")
                time.sleep(5)

        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(
            KLINE_UPDATES,
            TRADE_CHANNEL,
            ORDER_BOOK_UPDATES
        )
        self.logger.info("Subscribed to Redis Pub/Sub channels.")

    def _listen_redis(self):
        for message in self.pubsub.listen():
            if message["type"] != "message":
                continue

            channel = message["channel"]
            data = message["data"]
            self.logger.debug(f"📨 Message received on {channel}: {data}")

            try:
                payload = json.loads(data)
                self._handle_channel_message(channel, payload)
            except Exception as e:
                self.logger.error(f"Failed to process message: {e}")
    def _handle_channel_message(self, channel, payload):
        if channel == KLINE_UPDATES:
            self._process_kline(payload)
        elif channel == TRADE_CHANNEL:
            self._process_trade(payload)
        elif channel == ORDER_BOOK_UPDATES:
            self._process_orderbook(payload)
    def _process_kline(self, payload):
        self.redis_client.publish(PRE_PROC_KLINE_UPDATES, json.dumps(payload))
        # Example: Normalize and send to DB or another channel

    def _process_trade(self, payload):
        self.redis_client.publish(PRE_PROC_TRADE_CHANNEL, json.dumps(payload))
        # Example: Check anomalies, publish refined version

    def _process_orderbook(self, payload):
        self.redis_client.publish(PRE_PROC_ORDER_BOOK_UPDATES, json.dumps(payload))
        # Example: Cache in Redis or stream to analysis bot
