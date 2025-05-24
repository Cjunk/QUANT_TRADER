# --- trigger_service.py ---

import os
import sys
import time
import json
import threading
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
    PRE_PROC_KLINE_UPDATES,TRIGGER_QUEUE_CHANNEL
)
from config.config_db import (
    DB_HOST, DB_PORT, DB_DATABASE, DB_USER, DB_PASSWORD
)
from config.config_trigger_bot import (
    BOT_NAME, LOG_FILENAME, LOG_LEVEL
)
from utils.logger import setup_logger
from bots.utils.redis_client import get_redis
from utils.heartbeat import send_heartbeat  # Import the shared heartbeat utility

class TriggerBot:
    def __init__(self):
        self.logger = setup_logger(LOG_FILENAME, getattr(logging, LOG_LEVEL.upper(), logging.WARNING))
        self.running = True
        self.redis_client = get_redis()
        self.pubsub = None
        self.db_conn = None
        self.windows = {}  # {(symbol, interval): deque}
        self.WINDOW_SIZE = 20

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
                for interval in ["1","5", "60", "D"]:
                    cursor.execute(f"""
                        SELECT symbol, interval, start_time, close, rsi, macd, macd_signal, macd_hist, ma, upper_band, lower_band, volume, volume_ma, volume_change, volume_slope, rvol,open, high, low, turnover, confirmed
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
                                "MACD_Signal": r[6],
                                "MACD_Hist": r[7],
                                "MA": r[8],
                                "UpperBand": r[9],
                                "LowerBand": r[10],
                                "volume": r[11],
                                "Volume_MA": r[12],
                                "Volume_Change": r[13],
                                "Volume_Slope": r[14],
                                "RVOL": r[15],
                                "open": r[16],
                                "high": r[17],
                                "low": r[18],
                                "turnover": r[19],
                                "confirmed": r[20]

                            }
                            for r in rows
                        ]
                        self.logger.info(f"✅ Preloaded {len(rows)} candles for {symbol}-{interval}")

            cursor.close()

        except Exception as e:
            self.logger.error(f"❌ Preloading klines failed: {e}")
            sys.exit(1)
    def _refresh_symbols_periodically(self):
        while self.running:
            try:
                time.sleep(1800)  # 30 minutes
                cursor = self.db_conn.cursor()
                cursor.execute("SET search_path TO trading;")
                cursor.execute("SELECT symbol FROM current_coins;")
                new_symbols = [row[0] for row in cursor.fetchall()]
                cursor.close()

                current_symbols = {key[0] for key in self.windows.keys()}

                for symbol in new_symbols:
                    if symbol not in current_symbols:
                        for interval in ["1", "5", "60", "D"]:
                            cursor = self.db_conn.cursor()
                            cursor.execute(f"""
                                SELECT symbol, interval, start_time, close, rsi, macd, volume, volume_ma
                                FROM kline_data
                                WHERE symbol = %s AND interval = %s
                                ORDER BY start_time DESC
                                LIMIT {self.WINDOW_SIZE}
                            """, (symbol, interval))

                            rows = cursor.fetchall()
                            if rows:
                                rows.reverse()
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
                                self.logger.info(f"✅ Dynamically loaded {len(rows)} candles for {symbol}-{interval}")
                            cursor.close()

            except Exception as e:
                self.logger.error(f"❌ Error refreshing symbols: {e}")

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
        #self.logger.warning(f"🧪 Incoming kline for {symbol}-{interval}: {payload}")
    def analyze_trend(self, key):
        symbol, interval = key
        DEV_MODE = True  # Set False for production
        MA_WINDOW = 5 if DEV_MODE else 200  # shorter MA in DEV
        MIN_CONFIDENCE = 20 if DEV_MODE else 65  # Lower confidence threshold in DEV

        try:
            # 📊 Clean raw DataFrame
            df_raw = pd.DataFrame(self.windows[key])
            df = df_raw.dropna()

            # 🕒 Ensure timestamps are tz-naive for DB
            df.loc[:, "start_time"] = pd.to_datetime(df["start_time"], utc=True).dt.tz_convert(None)

            # ❌ Skip if insufficient candles
            if len(df) < self.WINDOW_SIZE:
                return

            # 🔢 Convert numeric columns safely
            numeric_cols = ["close", "RSI", "MACD", "MACD_Signal", "Volume_MA", "volume", "UpperBand", "LowerBand"]
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

            # 📈 Latest candle metrics
            close = df["close"].iloc[-1]
            volume = df["volume"].iloc[-1]
            volume_ma = df["Volume_MA"].iloc[-1]
            macd_now = df["MACD"].iloc[-1]
            macd_signal_now = df["MACD_Signal"].iloc[-1]
            rsi_now = df["RSI"].iloc[-1]
            upper_band = df["UpperBand"].iloc[-1]
            lower_band = df["LowerBand"].iloc[-1]
            rvol = volume / (volume_ma + 1e-8)

            # 🔍 Calculate trend deltas
            price_slope = (df["close"].iloc[-1] - df["close"].iloc[0]) / df["close"].iloc[0]
            rsi_slope = (df["RSI"].iloc[-1] - df["RSI"].iloc[0]) / 100
            macd_slope = (df["MACD"].iloc[-1] - df["MACD"].iloc[0]) / (abs(df["MACD"].iloc[0]) + 1e-8)

            # 📊 Composite scoring
            price_score = min(max(price_slope, -0.05), 0.05) / 0.05
            rsi_score = min(max(rsi_slope, -0.5), 0.5) / 0.5
            macd_score = min(max(macd_slope, -1), 1)
            rvol_score = min(rvol / 3, 1)
            confidence = (price_score * 0.4 + rsi_score * 0.25 + macd_score * 0.2 + rvol_score * 0.15) * 100

            # 🗓 Window logging info
            window_start = pd.to_datetime(df["start_time"].min())
            window_end = pd.to_datetime(df["start_time"].max())
            local_tz = pytz.timezone("Australia/Sydney")
            window_info = f"{window_start.tz_localize('UTC').astimezone(local_tz)} ➔ {window_end.tz_localize('UTC').astimezone(local_tz)}"

            # 🔍 Daily Trend Bias Check (DEV MODE adjusted)
            daily_key = (symbol, "D")
            daily_df = pd.DataFrame(self.windows.get(daily_key, []))
            daily_bias = "neutral"
            if len(daily_df) >= MA_WINDOW:
                daily_df["close"] = pd.to_numeric(daily_df["close"], errors='coerce')
                daily_ma = daily_df["close"].rolling(window=MA_WINDOW).mean().iloc[-1]
                daily_close = daily_df["close"].iloc[-1]

                if not pd.isna(daily_ma):
                    daily_bias = "bullish" if daily_close > daily_ma else "bearish"
                else:
                    self.logger.warning(f"⚠️ MA{MA_WINDOW} NaN, defaulting daily bias to neutral.")
            else:
                self.logger.warning(f"⚠️ Insufficient daily data (<{MA_WINDOW}), using neutral daily bias.")

            # ✅ Refined Trend Trigger
            if confidence >= MIN_CONFIDENCE and abs(price_slope) > 0.003 and rvol > 0.8:
                direction = "long" if price_slope > 0 else "short"

                # 🚩 Emit triggers (allow neutral bias in DEV mode)
                if DEV_MODE or daily_bias == "neutral" or \
                (direction == "long" and daily_bias == "bullish") or \
                (direction == "short" and daily_bias == "bearish"):

                    # ✅ Restored detailed logging for triggers
                    self.logger.info(
                        f"\n⚡ {'[DEV MODE]' if DEV_MODE else ''} Trend Trigger\n"
                        f"Symbol: {symbol} | Interval: {interval}\n"
                        f"Trend: {'📈 UP' if direction == 'long' else '📉 DOWN'} (Daily Bias: {daily_bias})\n"
                        f"Confidence: {confidence:.2f}% | RVOL: {rvol:.2f}x\n"
                        f"Price Δ: {price_slope:.2%} | RSI Δ: {rsi_slope:.2%} | MACD Δ: {macd_slope:.2%}\n"
                        f"Latest Close: {close:.2f} | RSI: {rsi_now:.2f} | MACD: {macd_now:.4f} | MACD Signal: {macd_signal_now:.4f}\n"
                        f"Bollinger Bands: Upper={upper_band:.2f}, Lower={lower_band:.2f}\n"
                        f"Window: {window_info}"
                    )

                    self.emit_signal("trend_trigger", symbol, interval, df, value=confidence,
                                    direction=direction, confidence=confidence,
                                    window=(window_start, window_end))
                else:
                    self.logger.info(f"⚠️ Skipped trend trigger (daily bias mismatch: {daily_bias}).")

            # ✅ MACD + Bollinger crossover logic
            if len(df) >= 2:
                macd_prev, macd_signal_prev = df["MACD"].iloc[-2], df["MACD_Signal"].iloc[-2]
                bullish_cross = macd_prev < macd_signal_prev and macd_now > macd_signal_now and close > upper_band
                bearish_cross = macd_prev > macd_signal_prev and macd_now < macd_signal_now and close < lower_band

                if bullish_cross and rvol > 2 and rsi_now > 55 and (DEV_MODE or daily_bias == "bullish"):
                    self.logger.info(
                        f"\n🚀 {'[DEV MODE]' if DEV_MODE else ''} Strong Bullish MACD+Bollinger\n"
                        f"{symbol}-{interval} | RVOL: {rvol:.2f}x | Window: {window_info}"
                    )
                    self.emit_signal("strong_bullish_trigger", symbol, interval, df, value=macd_now,
                                    direction="long", confidence=75.0, window=(window_start, window_end))

                elif bearish_cross and rvol > 2 and rsi_now < 45 and (DEV_MODE or daily_bias == "bearish"):
                    self.logger.info(
                        f"\n⚠️ {'[DEV MODE]' if DEV_MODE else ''} Strong Bearish MACD+Bollinger\n"
                        f"{symbol}-{interval} | RVOL: {rvol:.2f}x | Window: {window_info}"
                    )
                    self.emit_signal("strong_bearish_trigger", symbol, interval, df, value=macd_now,
                                    direction="short", confidence=75.0, window=(window_start, window_end))

            # 📈 Volume Spike (informational only, no trades directly triggered)
            if rvol > 4:
                self.logger.info(
                    f"\n📈 Volume Spike Detected\nSymbol: {symbol} | Interval: {interval}\n"
                    f"RVOL: {rvol:.2f}x | Window: {window_info}"
                )

        except Exception as e:
            self.logger.error(f"❌ Trend analysis failed: {e}")


    def log_signal(self, symbol, interval, signal_type, value=None, context=None, direction=None, confidence=None, window=None):
        window_start, window_end = window if window else (None, None)
            # Ensure window times are JSON serializable (as ISO strings)
        window_start_str = window_start.isoformat() if window_start else None
        window_end_str = window_end.isoformat() if window_end else None
        signal = {
            "symbol": symbol,
            "interval": interval,
            "signal_type": signal_type,
            "value": value,
            "context": context or {},
            "confidence": confidence,
            "direction": direction,
            "window_start": window_start,
            "window_end": window_end
        }
        self.insert_signal_log(signal)        
        signal["window_start"] = window_start_str
        signal["window_end"] = window_end_str
        self.redis_client.rpush(TRIGGER_QUEUE_CHANNEL, json.dumps(signal))

    def emit_signal(self, signal_type, symbol, interval, df, value=None, direction=None, confidence=None, window=None):
        """
        Build and insert a signal with context from df automatically.
        """
        try:
            context = {
                "close": float(df["close"].iloc[-1]),
                "volume": float(df["volume"].iloc[-1]),
                "volume_ma": float(df["Volume_MA"].iloc[-1]),
                "rsi": float(df["RSI"].iloc[-1]),
                "macd": float(df["MACD"].iloc[-1]),
                "macd_signal": float(df["MACD_Signal"].iloc[-1]),
                "upper_band": float(df["UpperBand"].iloc[-1]),
                "lower_band": float(df["LowerBand"].iloc[-1]),
            }

            self.log_signal(
                symbol, interval,
                signal_type=signal_type,
                value=value,
                context=context,
                direction=direction,
                confidence=confidence,
                window=window
            )
            
        except Exception as e:
            self.logger.error(f"❌ emit_signal failed: {e}")

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
    def run(self):
        self.logger.info("🚀 Trigger Bot starting...")
        self.connect_postgres()
        self.preload_recent_klines()
        self.connect_redis()
        self._start_background()
        threading.Thread(target=self._refresh_symbols_periodically, daemon=True).start()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("🛑 Keyboard interrupt received. Stopping TriggerBot.")
            self.running = False
    def insert_signal_log(self, signal):
        cursor = self.db_conn.cursor()
        value = float(signal.get("value")) if signal.get("value") is not None else None
        confidence = float(signal.get("confidence")) if signal.get("confidence") is not None else None
        try:
            cursor.execute("""
                INSERT INTO trading.signal_log
                (symbol, interval, signal_type, value, context, confidence, direction, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            """, (
                signal["symbol"],
                signal["interval"],
                signal["signal_type"],
                value,
                json.dumps(signal.get("context", {})),
                confidence,
                signal.get("direction"),
                signal.get("window_start"),
                signal.get("window_end")
            ))
            self.db_conn.commit()
            self.logger.info(f"🧠 Logged signal: {signal['signal_type']} for {signal['symbol']}")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"❌ Failed to insert signal: {e}")
        finally:
            cursor.close()

    def heartbeat(self):
        # Heartbeat logic is now handled by the shared utility. This method is intentionally minimal.
        while self.running:
            try:
                payload = {
                    "bot_name": self.bot_name,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat(),
                    "auth_token": self.auth_token,
                    "metadata": {
                        "version": getattr(self, "version", "1.0.0"),
                        "pid": os.getpid(),
                        "strategy": getattr(self, "strategy", "-"),
                        "vitals": {}
                    }
                }
                send_heartbeat(payload, status="heartbeat")
                self.logger.debug("❤️ Sent heartbeat.")
            except Exception as e:
                self.logger.warning(f"Heartbeat failed: {e}")
            time.sleep(self.heartbeat_interval)

if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is NOT activated. Please activate it first.")
        sys.exit(1)

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting TRIGGER_BOT...")
    bot = TriggerBot()
    bot.run()

