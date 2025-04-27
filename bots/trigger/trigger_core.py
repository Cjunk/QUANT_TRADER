# --- trigger_service.py ---

import os
import sys
import time
import json
import threading
import redis
import pandas as pd
import datetime
import logging
import psycopg2
import pytz
from collections import deque

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')

from config.config_redis import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    PRE_PROC_KLINE_UPDATES
)
from config.config_db import (
    DB_HOST, DB_PORT, DB_DATABASE, DB_USER, DB_PASSWORD
)
from config.config_trigger_bot import (
    BOT_NAME, LOG_FILENAME, LOG_LEVEL
)
from utils.logger import setup_logger

class TriggerBot:
    def __init__(self):
        self.logger = setup_logger(LOG_FILENAME, getattr(logging, LOG_LEVEL.upper(), logging.WARNING))
        self.running = True
        self.redis_client = None
        self.pubsub = None
        self.db_conn = None
        self.windows = {}  # {(symbol, interval): deque}
        self.WINDOW_SIZE = 5

    def connect_postgres(self):
        try:
            self.db_conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_DATABASE,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.logger.info("✅ Connected to PostgreSQL.")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Postgres: {e}")
            sys.exit(1)

    def preload_recent_klines(self):
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SET search_path TO trading;")

            cursor.execute("SELECT symbol FROM current_coins;")
            symbols = [row[0] for row in cursor.fetchall()]

            for symbol in symbols:
                for interval in ["1", "5", "60", "D"]:
                    cursor.execute(f"""
                        SELECT symbol, interval, start_time, close, rsi, macd, volume, volume_ma
                        FROM kline_data
                        WHERE symbol = %s AND interval = %s
                        ORDER BY start_time DESC
                        LIMIT {self.WINDOW_SIZE}
                    """, (symbol, interval))

                    rows = cursor.fetchall()
                    if rows:
                        rows.reverse()  # So oldest first
                        self.windows[(symbol, interval)] = [
                            {
                                "symbol": r[0],
                                "interval": r[1],
                                "start_time": r[2],
                                "close": r[3],
                                "RSI": r[4],
                                "MACD": r[5],
                                "volume": r[6],
                                "Volume_MA": r[7]
                            }
                            for r in rows
                        ]
                        self.logger.info(f"✅ Preloaded {len(rows)} candles for {symbol}-{interval}")

            cursor.close()

        except Exception as e:
            self.logger.error(f"❌ Preloading klines failed: {e}")
            sys.exit(1)

    def connect_redis(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(PRE_PROC_KLINE_UPDATES)
        self.logger.info("✅ Connected to Redis and subscribed to PRE_PROC_KLINE_UPDATES")

    def _start_background(self):
        threading.Thread(target=self.listen_redis, daemon=True).start()

    def listen_redis(self):
        for message in self.pubsub.listen():
            if message["type"] == "message":
                try:
                    payload = json.loads(message["data"])
                    self.process_kline(payload)
                except Exception as e:
                    self.logger.error(f"❌ Error handling kline message: {e}")

    def process_kline(self, payload):
        symbol = payload["symbol"]
        interval = payload["interval"]
        key = (symbol, interval)

        if key not in self.windows:
            self.windows[key] = []

        self.windows[key].append(payload)

        if len(self.windows[key]) > self.WINDOW_SIZE:
            self.windows[key].pop(0)

        if len(self.windows[key]) == self.WINDOW_SIZE:
            self.analyze_trend(key)

    def analyze_trend(self, key):
        symbol, interval = key
        df = pd.DataFrame(self.windows[key])

        try:
            df["close"] = pd.to_numeric(df["close"], errors='coerce')
            df["RSI"] = pd.to_numeric(df["RSI"], errors='coerce')
            df["MACD"] = pd.to_numeric(df["MACD"], errors='coerce')
            df["Volume_MA"] = pd.to_numeric(df["Volume_MA"], errors='coerce')
            df["volume"] = pd.to_numeric(df["volume"], errors='coerce')

            price_slope = (df["close"].iloc[-1] - df["close"].iloc[0]) / df["close"].iloc[0]
            rsi_slope = (df["RSI"].iloc[-1] - df["RSI"].iloc[0]) / df["RSI"].iloc[0]
            macd_slope = (df["MACD"].iloc[-1] - df["MACD"].iloc[0]) / abs(df["MACD"].iloc[0]) if df["MACD"].iloc[0] != 0 else 0
            volume_ratio = df["volume"].iloc[-1] / df["Volume_MA"].iloc[-1]

            trend_score = 0
            if price_slope > 0.002: trend_score += 1
            if rsi_slope > 0.01: trend_score += 1
            if macd_slope > 0.01: trend_score += 1
            if volume_ratio > 1.2: trend_score += 1

            if trend_score >= 2:
                direction = "📈 UP" if price_slope > 0 else "📉 DOWN"
                self.logger.info(f"\n⚡ Trigger detected!\n"
                                 f"Symbol: {symbol}\n"
                                 f"Interval: {interval}\n"
                                 f"Trend: {direction}\n"
                                 f"Price Δ: {price_slope:.2%}\n"
                                 f"RSI Δ: {rsi_slope:.2%}\n"
                                 f"MACD Δ: {macd_slope:.2%}\n"
                                 f"Volume/MA: {volume_ratio:.2f}x\n"
                                 f"Window: {df['start_time'].iloc[0]} ➔ {df['start_time'].iloc[-1]}")

        except Exception as e:
            self.logger.error(f"❌ Trend analysis failed: {e}")

    def run(self):
        self.logger.info("🚀 Trigger Bot starting...")
        self.connect_postgres()
        self.preload_recent_klines()
        self.connect_redis()
        self._start_background()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("🛑 Keyboard interrupt received. Stopping TriggerBot.")
            self.running = False

if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is NOT activated. Please activate it first.")
        sys.exit(1)

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting TRIGGER_BOT...")
    bot = TriggerBot()
    bot.run()

