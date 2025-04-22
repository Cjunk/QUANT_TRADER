# 📡 Ticker Surveillance Bot
# Description: Fetches tickers from Bybit (spot, linear, inverse), stores them in a DB,
# and publishes new tickers to Redis for analysis bots to pick up.

import time
import os
import json
import redis
import threading
import datetime
import logging
import requests
import psycopg2
from collections import defaultdict
from config.config_redis import SERVICE_STATUS_CHANNEL, HEARTBEAT_CHANNEL, REDIS_HOST, REDIS_PORT, REDIS_DB, NEW_COIN_CHANNEL
from config.config_auto_tickersurveillancebot import BOT_NAME, BOT_AUTH_TOKEN, HEARTBEAT_INTERVAL, LOG_FILENAME, LOG_LEVEL, FETCH_INTERVAL, DB_CONFIG
from utils.logger import setup_logger

class TickerSurveillanceBot:
    def __init__(self):
        self.running = True
        self.logger = setup_logger(LOG_FILENAME, getattr(logging, LOG_LEVEL.upper(), logging.INFO))
        self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        self.conn = psycopg2.connect(**DB_CONFIG)

    def _register(self):
        payload = {
            "bot_name": BOT_NAME,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": BOT_AUTH_TOKEN,
            "metadata": {"version": "1.0", "role": "ticker_monitor"}
        }
        self.redis.publish(SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info("Registered bot with service.")

    def _heartbeat(self):
        while self.running:
            try:
                payload = {
                    "bot_name": BOT_NAME,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat()
                }
                self.redis.publish(HEARTBEAT_CHANNEL, json.dumps(payload))
                self.logger.debug("Heartbeat sent.")
            except Exception as e:
                self.logger.warning(f"Heartbeat error: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def fetch_bybit_tickers(self):
        urls = {
            "spot": "https://api.bybit.com/v5/market/instruments-info?category=spot",
            "linear": "https://api.bybit.com/v5/market/instruments-info?category=linear",
            "inverse": "https://api.bybit.com/v5/market/instruments-info?category=inverse"
        }
        all_tickers = []
        for category, url in urls.items():
            try:
                res = requests.get(url).json()
                for item in res.get("result", {}).get("list", []):
                    all_tickers.append({
                        "symbol": item["symbol"],
                        "category": category,
                        "base_coin": item.get("baseCoin"),
                        "quote_coin": item.get("quoteCoin"),
                        "status": item.get("status")
                    })
            except Exception as e:
                self.logger.warning(f"Failed to fetch {category} tickers: {e}")
        return all_tickers

    def sync_to_database(self, tickers):
        cur = self.conn.cursor()
        new_tickers = []
        for t in tickers:
            try:
                cur.execute("SELECT 1 FROM exchange_tickers WHERE symbol = %s", (t["symbol"],))
                if cur.fetchone():
                    continue

                cur.execute("""
                INSERT INTO exchange_tickers (symbol, category, base_coin, quote_coin, status,
                    is_new, being_traded, last_analysed, added_on, extra_data)
                VALUES (%s, %s, %s, %s, %s, TRUE, FALSE, NULL, now(), '{}')
                """, (t["symbol"], t["category"], t["base_coin"], t["quote_coin"], t["status"]))

                new_tickers.append(t)
                self.logger.info(f"New ticker added: {t['symbol']}")

            except Exception as e:
                self.logger.error(f"DB Insert Error for {t['symbol']}: {e}")
                self.conn.rollback()
        self.conn.commit()
        cur.close()
        return new_tickers

    def publish_new_tickers(self, tickers):
        for t in tickers:
            msg = {
                "event": "new_ticker",
                "symbol": t["symbol"],
                "category": t["category"],
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            self.redis.publish(NEW_COIN_CHANNEL, json.dumps(msg))
            self.logger.info(f"Published new ticker: {t['symbol']}")

    def run(self):
        self.logger.info(f"🚀 {BOT_NAME} is running...")
        self._register()
        threading.Thread(target=self._heartbeat, daemon=True).start()
        while self.running:
            try:
                tickers = self.fetch_bybit_tickers()
                new_ones = self.sync_to_database(tickers)
                if new_ones:
                    self.publish_new_tickers(new_ones)
            except Exception as e:
                self.logger.error(f"Loop Error: {e}")
            time.sleep(FETCH_INTERVAL)

    def stop(self):
        self.running = False
        self.conn.close()
        self.logger.info("Bot stopped.")
        payload = {
            "bot_name": BOT_NAME,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": BOT_AUTH_TOKEN
        }
        self.redis.publish(SERVICE_STATUS_CHANNEL, json.dumps(payload))
