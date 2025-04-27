"""
📦 Preprocessor Bot for Quant Trading Platform

Purpose:
- Subscribe to raw kline, trade, and order book channels
- Compute technical indicators (RSI, MACD, Bollinger Bands) for incoming klines
- Publish enriched kline data for DB bot and other services

Maintains:
- Rolling kline windows per symbol and interval
- Heartbeat system for health monitoring

Author: Jericho Sharman
"""

import os
import sys
import time
import json
import redis
import pandas as pd
import datetime
import threading
import logging
import pytz  # For timezone handling
from collections import deque
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Config & Setup
from config.config_redis import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    KLINE_UPDATES, TRADE_CHANNEL, ORDER_BOOK_UPDATES,
    PRE_PROC_KLINE_UPDATES, PRE_PROC_TRADE_CHANNEL, PRE_PROC_ORDER_BOOK_UPDATES,
    SERVICE_STATUS_CHANNEL, HEARTBEAT_CHANNEL
)
from config.config_auto_preprocessor_bot import (LOG_LEVEL,WINDOW_SIZE, LOG_FILENAME,BOT_NAME, BOT_AUTH_TOKEN, HEARTBEAT_INTERVAL)
from utils.logger import setup_logger
from utils.global_indicators import GlobalIndicators
class PreprocessorBot:
    def __init__(self):
        self.logger = setup_logger(LOG_FILENAME,getattr(logging, LOG_LEVEL.upper(), logging.WARNING)) # Set up logger and retrieve the logger type from config file
        self.bot_name = BOT_NAME
        self.auth_token = BOT_AUTH_TOKEN      
        self.running = True
        self.GlobalIndicators = GlobalIndicators()
        self.kline_windows = {}  # {(symbol, interval): deque}
        
    def _connect_redis(self):
        #self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(KLINE_UPDATES, TRADE_CHANNEL, ORDER_BOOK_UPDATES)
        self.logger.info("✅ Connected to Redis and subscribed to data channels.")

    def _register(self):
        payload = {
            "bot_name": self.bot_name,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {"version": "1.0", "pid": os.getpid(), "description": "Preprocessor bot"}
        }
        self.redis_client.publish(SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"🔐 Registered bot '{self.bot_name}' with status 'started'.")

    def _heartbeat(self):
        while self.running:
            try:
                payload = {
                    "bot_name": self.bot_name,
                    "heartbeat": True,
                    "time": datetime.datetime.now(pytz.timezone("Australia/Sydney")).isoformat()
                }
                self.redis_client.publish(HEARTBEAT_CHANNEL, json.dumps(payload))
                self.logger.debug("❤️ Sent heartbeat.")
            except Exception as e:
                self.logger.warning(f"⚠️ Heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def run(self):
        self.logger.info("🚀 Preprocessor Bot is running...")
        self._connect_redis()
        self._register()
        threading.Thread(target=self._heartbeat, daemon=True).start()
        threading.Thread(target=self._listen_redis, daemon=True).start()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self._stop()

    def _stop(self):
        self.running = False
        payload = {
            "bot_name": self.bot_name,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis_client.publish(SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info("🛑 Preprocessor Bot shutdown.")

    def _listen_redis(self):
        for message in self.pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                channel = message["channel"]
                payload = json.loads(message["data"])
                self._route_message(channel, payload)
            except Exception as e:
                self.logger.error(f"❌ Failed to handle Redis message: {e}")

    def _route_message(self, channel, payload):
        if channel == KLINE_UPDATES:
            self._process_kline(payload)
        elif channel == TRADE_CHANNEL:
            self._process_trade(payload)
        elif channel == ORDER_BOOK_UPDATES:
            self._process_orderbook(payload)

    def _process_kline(self, payload):
        # Calculate the indicators for each kline and publish the enriched kline
        symbol = payload["symbol"]
        interval = payload["interval"]
        key = (symbol, interval)
        redis_key = f"kline_window:{symbol}:{interval}"

        if key not in self.kline_windows:
            self.kline_windows[key] = deque(maxlen=WINDOW_SIZE)
            # Try load from Redis on first encounter
            stored = self.redis_client.lrange(redis_key, -WINDOW_SIZE, -1)
            for item in stored:
                self.kline_windows[key].append(json.loads(item))

        # Add and persist the new kline
        # 🔄 Deduplication check
        if self.kline_windows[key]:
            last = self.kline_windows[key][-1]
            if last["start_time"] == payload["start_time"] and last["close"] == payload["close"]:
                self.logger.debug("🔁 Duplicate kline detected, skipping republish.")
                return

        self.kline_windows[key].append(payload)
        self.redis_client.rpush(redis_key, json.dumps(payload))
        self.redis_client.ltrim(redis_key, -WINDOW_SIZE, -1)
        df = pd.DataFrame(self.kline_windows[key])
        try:
            df[["open", "close", "high", "low", "volume", "turnover"]] = df[["open", "close", "high", "low", "volume", "turnover"]].astype(float)
            enriched_df = self.GlobalIndicators.compute_indicators(df.copy())
            enriched_kline = enriched_df.iloc[-1].to_dict()
            enriched_kline["start_time"] = df.iloc[-1]["start_time"]
            self.redis_client.publish(PRE_PROC_KLINE_UPDATES, json.dumps(enriched_kline))
        except Exception as e:
            self.logger.error(f"❌ Error processing kline indicators: {e}")

    def _process_trade(self, payload): #TODO: Add trade processing logic
        self.redis_client.publish(PRE_PROC_TRADE_CHANNEL, json.dumps(payload))

    def _process_orderbook(self, payload):#TODO: Add order book processing logic
        self.redis_client.publish(PRE_PROC_ORDER_BOOK_UPDATES, json.dumps(payload))


if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is NOT activated! Please activate it first.")
        sys.exit(1)
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting PREPROCESSOR_BOT...")
    bot = PreprocessorBot()
    bot.run()
