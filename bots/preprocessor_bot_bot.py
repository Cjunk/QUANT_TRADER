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
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')
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
        self.trade_windows={}
        
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
    def _start_background_tasks(self):
        threading.Thread(target=self._heartbeat, daemon=True).start()
        threading.Thread(target=self._listen_redis, daemon=True).start()
        threading.Thread(target=self._flush_old_trades, daemon=True).start()
    def run(self):
        self.logger.info(" Preprocessor Bot is running...")
        self._connect_redis()
        self._register()
        self._start_background_tasks()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self._final_flush()
            self._stop()
    def _flush_old_trades(self):
        while self.running:
            try:
                current_minute = pd.Timestamp.utcnow().floor('min')
                expired_keys = [k for k in self.trade_windows if k[1] < current_minute]
                for key in expired_keys:
                    self._publish_trade_summary(key, self.trade_windows.pop(key))
            except Exception as e:
                self.logger.error(f"\u274c Error flushing old trades: {e}")
            time.sleep(1)
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

    def _publish_trade_summary(self, key, trades):
        symbol, minute_start = key
        total_volume = sum(t["volume"] for t in trades)
        vwap = (sum(t["price"] * t["volume"] for t in trades) / total_volume) if total_volume > 0 else 0
        trade_count = len(trades)
        max_trade = max(trades, key=lambda t: t["volume"], default={"volume": 0, "price": 0})

        summary = {
            "symbol": symbol,
            "minute_start": minute_start.isoformat(),
            "total_volume": total_volume,
            "vwap": vwap,
            "trade_count": trade_count,
            "largest_trade_volume": max_trade["volume"],
            "largest_trade_price": max_trade["price"]
        }
        self.redis_client.publish(PRE_PROC_TRADE_CHANNEL, json.dumps(summary))
        #self.logger.debug(f" Published summarized trade data: {summary}")

    def _process_trade(self, payload):
        try:
            symbol = payload["symbol"]
            trade_time = pd.to_datetime(payload["trade_time"], utc=True).floor('min')
            price = payload["price"]
            volume = payload["volume"]

            key = (symbol, trade_time)

            if key not in self.trade_windows:
                self.trade_windows[key] = []

            self.trade_windows[key].append({"price": price, "volume": volume})

        except Exception as e:
            self.logger.error(f"\u274c Error processing trade: {e}")

    def _final_flush(self):
        self.logger.info("\ud83d\udd04 Flushing all remaining trade windows at shutdown...")
        for key, trades in list(self.trade_windows.items()):
            self._publish_trade_summary(key, trades)
        self.trade_windows.clear()

    def _process_orderbook(self, payload):#TODO: Add order book processing logic
        self.redis_client.publish(PRE_PROC_ORDER_BOOK_UPDATES, json.dumps(payload))


if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is NOT activated! Please activate it first.")
        sys.exit(1)
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting PREPROCESSOR_BOT...")
    bot = PreprocessorBot()
    bot.run()
