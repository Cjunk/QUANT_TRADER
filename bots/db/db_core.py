"""
Institutional-Style Database Bot for Quant Trading
Now with Redis Pub/Sub to receive data from your WebSocket bot.
This bot is the Postgresql bot. Most functions to do with updating postgresql will be handled by this bot

✅  TODO LIST
    1.  Subscribe to redis kline channels. And Store the kline data in the kline_data table in Postgresql ✅
    2.  Subs to redis Trade channel. Store filtered Trade data in postgresql. ✅
    3.  Periodically aggregate Order book data and store.
    4.  Sub to new coin channel. Refresh the current master coin list (Perhaps keep a trailing pre list too) ✅
    5.  Retrieve Large Trade Thresholds from Postgresql ✅
    6.  Categorise functions
    7.  Externalise variables into config files
    8.  clean INIT
    9.  Optimise
    10.  send a redis signal to dedicated channel once kline and indicator data has been stored. signal will include the interval and symbol. 
"""

import json
import time
import threading
import datetime
import redis
import numpy as np
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pytz
import psycopg2
import hashlib
import logging
import pandas as pd
import psycopg2.extras
import config.config_db as db_config
import config.config_redis as config_redis
import config.config_ws as ws_config  # If you store Redis host/port here or wherever
from config.config_common import HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_TIMEOUT_SECONDS, HEARTBEAT_CHANNEL
from wallet_balance import store_wallet_balances
from sqlalchemy import create_engine
from utils.logger import setup_logger
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extras import execute_values

from utils.heartbeat import send_heartbeat
from gap_utils import fix_data_gaps
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')

# =============================
# Refactor/cleanup for maintainability and modularity
# =============================

# 1. Remove __main__ block (runner should handle startup)
# 2. Move Bybit/klines gap-fix logic to a separate module (e.g., gap_utils.py)
# 3. Keep only DB bot core and handlers in this file

# --- Remove __main__ block ---
# (No code needed here, just remove the block)

# --- Move gap-fix and Bybit klines logic to gap_utils.py ---
# (Replace methods with imports and calls to gap_utils)

# At the top, add:
# from gap_utils import fix_data_gaps

# Replace self._fix_data_gaps and self._fetch_bybit_klines with calls to gap_utils.fix_data_gaps

# In _initial_gap_fix and _start_gap_check, replace:
#     self._fix_data_gaps(symbol, interval)
# with:
#     fix_data_gaps(self, symbol, interval)

# Remove the _fix_data_gaps and _fetch_bybit_klines methods from this file.

# --- Optionally, move _insert_missing_klines to gap_utils as well ---
# (If it is only used by gap-fix logic)

# --- All other DB and handler logic remains in this file ---

# --- Add a comment at the top ---
# """
# This file contains the core PostgresDBBot class and all DB/Redis handlers.
# Gap-fix and Bybit klines logic is now in gap_utils.py for modularity.
# Startup is handled by a runner script, not by __main__ here.
# """

class PostgresDBBot:
    # === Class-level constants and variables ===
    # ------------------------------------------
    # Define constants and variables used across the bot.
    INTERVAL_MAP = {"1": 1, "5": 5, "60": 60, "D": 1440}
    # HEARTBEAT_TIMEOUT_SECONDS and HEARTBEAT_INTERVAL_SECONDS are imported from config_common

    # === Initialization ===
    # ----------------------
    # Initialize the bot with logging, database, and Redis configurations.
    def __init__(self, log_filename=db_config.LOG_FILENAME):
        # === All instance variables at the top ===
        self.logger = setup_logger(db_config.LOG_FILENAME, getattr(logging, db_config.LOG_LEVEL.upper(), logging.WARNING))
        self.host = db_config.DB_HOST
        self.port = db_config.DB_PORT
        self.database = db_config.DB_DATABASE
        self.trade_buffer = []
        self.status = {
            "bot_name": db_config.BOT_NAME,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": db_config.BOT_AUTH_TOKEN,
            "metadata": {
                "version": "1.2.0",
                "pid": os.getpid(),
                "strategy": "VWAP"
            }
        }
        self.conn = None
        self.redis_client = None
        self.pubsub = None
        self.running = True
        self.subscribed_channels = set()
        self.bot_status = {}
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT_SECONDS
        self.heartbeat_interval = HEARTBEAT_INTERVAL_SECONDS

        self.logger.info("Initializing PostgresDBBot...")
        print("Database Bot (Postgresql) is running .......")
        self._setup_postgres()
        self._setup_redis()
        # === Bot status/liveness logic FIRST ===
        self.bot_status = self._load_initial_bot_status()
        self._start_heartbeat_listener()
        self._start_self_heartbeat()  # <-- Ensure self-heartbeat is started
        self._start_archive_scheduler()  # <-- Restore archive scheduler
        # === Only now start periodic/gap/archive tasks ===
        threading.Thread(target=self._flush_trade_buffer, daemon=True).start()

    """
    =-=-=-=-=-=-=- Internal Bot operational functions
    """  

    def _start_archive_scheduler(self):
        """
        Starts a background thread that archives kline data at midnight (Sydney time) every day.
        """
        def scheduler():
            while self.running:
                now = datetime.datetime.now(pytz.timezone("Australia/Sydney"))
                target = now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
                sleep_duration = (target - now).total_seconds()
                self.logger.info(f"Next archive scheduled in {sleep_duration / 3600:.2f} hours.")
                time.sleep(sleep_duration)
                self._archive_kline_data()
        threading.Thread(target=scheduler, daemon=True).start()

    def _start_self_heartbeat(self):
        """
        Periodically send a heartbeat payload for this DB bot to the HEARTBEAT_CHANNEL.
        This ensures the bot is not marked as stopped by its own heartbeat listener.
        """
        def self_heartbeat():
            while self.running:
                try:
                    payload = {
                        "bot_name": db_config.BOT_NAME,
                        "heartbeat": True,
                        "time": datetime.datetime.utcnow().isoformat(),
                        "auth_token": db_config.BOT_AUTH_TOKEN,
                        "metadata": {
                            "version": "1.2.0",
                            "pid": os.getpid(),
                            "strategy": "VWAP"
                        }
                    }
                    self.redis_client.publish(HEARTBEAT_CHANNEL, json.dumps(payload))
                except Exception as e:
                    self.logger.warning(f"Failed to send self-heartbeat: {e}")
                time.sleep(self.heartbeat_interval)
        threading.Thread(target=self_heartbeat, daemon=True).start()

    #   -----POSTGRESQL SETUP
    def _setup_postgres(self):
        self.host = os.getenv("DB_HOST")
        self.port = int(os.getenv("DB_PORT"))
        self.database = os.getenv("DB_DATABASE")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.schema = os.getenv("DB_TRADING_SCHEMA")

        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        self.conn.autocommit = False

        # ✅ Set schema on connection
        with self.conn.cursor() as cur:
            cur.execute(f"SET search_path TO {self.schema}")

        self.logger.info(f"Connected to PostgreSQL @ {self.host}:{self.port}/{self.database}") 
   
    #   -----REDIS SETUP
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
            config_redis.COIN_CHANNEL,
            config_redis.PRE_PROC_KLINE_UPDATES,
            config_redis.PRE_PROC_TRADE_CHANNEL,
            config_redis.PRE_PROC_ORDER_BOOK_UPDATES,
            config_redis.RESYNC_CHANNEL,
            config_redis.SERVICE_STATUS_CHANNEL,
            config_redis.MACRO_METRICS_CHANNEL,
            config_redis.REQUEST_COINS,
            config_redis.DB_SAVE_SUBSCRIPTIONS
        )
        self.logger.info("Subscribed to Redis Pub/Sub channels.")

    def _listen_to_redis(self):
        self.logger.info("🧠 Redis listener thread started.")
        while self.running:
            try:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message:
                    channel = message['channel']
                    data_str = message['data']
                    try:
                        data_obj = json.loads(data_str)
                    except json.JSONDecodeError:
                        self.logger.error(f"Invalid JSON from channel={channel}: {data_str}")
                        continue

                    if channel == config_redis.PRE_PROC_KLINE_UPDATES:
                        self.handle_kline_update(data_obj)
                    elif channel == config_redis.DB_REQUEST_SUBSCRIPTIONS:
                        self.logger.info("Requesting subscriptions from the db")
                        self.handle_request_subscriptions(data_obj)
                    elif channel == config_redis.DB_SAVE_SUBSCRIPTIONS:
                        data = json.loads(message['data'])
                        self.save_subscription(data['market'], data['symbols'], data['topics'], data['owner'])
                    elif channel == config_redis.MACRO_METRICS_CHANNEL:
                        self.handle_macro_metrics(data_obj)
                    elif channel == config_redis.PRE_PROC_TRADE_CHANNEL:
                        self.handle_trade_update(data_obj)
                    elif channel == config_redis.PRE_PROC_ORDER_BOOK_UPDATES:
                        self.handle_orderbook_update(data_obj)
                    elif channel == config_redis.COIN_CHANNEL:
                        self.handle_coin_list_update(data_obj)
                    elif channel == config_redis.SERVICE_STATUS_CHANNEL:
                        self.handle_bot_status_update(data_obj)
                    elif channel == config_redis.REQUEST_COINS:
                        self.logger.info("Received request for coins list.")
                        self._publish_current_coin_list()
                    elif channel == config_redis.RESYNC_CHANNEL:
                        self.logger.info("Received resync signal, publishing coin list.")
                        self._publish_current_coin_list()
                    else:
                        self.logger.warning(f"Unrecognized channel: {channel}, data={data_obj}")
            except Exception as e:
                self.logger.error(f"❌ Redis listener error: {e}")
                time.sleep(2)
    def handle_request_subscriptions(self, data):
        """
        Retrieve and return saved subscriptions filtered by the market value in the data object.
        """
        market_filter = data.get("market")
        cur = self.conn.cursor()
        rows = []
        query = """
                SELECT * FROM trading.websocket_subscriptions WHERE market = %s;
                """
        try:
            cur.execute(query, (market_filter,))
            rows = cur.fetchall()
            self.logger.info(f"Retrieved {len(rows)} saved subscriptions for market '{market_filter}' from DB.")
        except Exception as e:
            self.logger.error(f"Error retrieving subscriptions from DB: {e}")   
        finally:
            cur.close()

        # Publish the retrieved subscriptions to the requesting bot
        if rows:
            subscriptions = []
            for row in rows:
                market, symbol, topic, owner = row
                subscriptions.append({
                    "market": market,
                    "symbol": symbol,
                    "topic": topic,
                    "owner": owner
                })
            payload = json.dumps(subscriptions)
            self.redis_client.publish(config_redis.DB_REQUEST_SUBSCRIPTIONS, payload)
            self.logger.info(f"Published {len(subscriptions)} subscriptions to {config_redis.DB_REQUEST_SUBSCRIPTIONS} channel.")
        else:
            self.logger.info(f"No saved subscriptions found for market '{market_filter}' in DB.")


    def save_subscription(self,market: str, symbols: list, topics: list, owner: str):
        if not owner:
            return  # Don't store subscriptions with no owner
        cur = self.conn.cursor()
        rows = []
        for symbol in symbols:
            for topic in topics:
                rows.append((market, symbol, topic, owner))

        query = """
        INSERT INTO trading.websocket_subscriptions (market, symbol, topic, owner)
        VALUES %s
        ON CONFLICT (market, symbol, topic, owner) DO NOTHING
        """
        execute_values(cur, query, rows)
        self.conn.commit()
        cur.close()
        #self.conn.close()
    def run(self):
        self.logger.info("DB Bot running, starting Redis listener thread...")

        # Start the Redis listener in a separate thread so it doesn't block
        threading.Thread(target=self._listen_to_redis, daemon=True).start()

        # Keep the main thread alive but responsive to CTRL+C
        while self.running:
            time.sleep(1)
  
    def close(self):
        """
        Close the database connection cleanly.
        """
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed.")
    def stop(self):
        """Stop the run loop and close DB connection."""
        self.running = False
        # Send stopped status update before shutting down
        stopped_status = {
            "bot_name": db_config.BOT_NAME,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": db_config.BOT_AUTH_TOKEN,
            "metadata": {
                "version": "1.2.0",
                "pid": os.getpid(),
                "strategy": "VWAP"
            }
        }
        try:
            self.logger.info("Sending stopped status update to DB...")
            self.handle_bot_status_update(stopped_status)
        except Exception as e:
            self.logger.error(f"Failed to update stopped status in DB: {e}")
        try:
            if self.pubsub:
                self.pubsub.close()
        except Exception as e:
            self.logger.error(f"Error closing Redis pubsub: {e}")
        try:
            if self.conn:
                self.conn.close()
        except Exception as e:
            self.logger.error(f"Error closing DB connection: {e}")
        self.logger.info("DB Bot Stopped.")
    def _publish_current_coin_list(self):
        coins = self._retrieve_coins()
        current_coin_list = [row[0] for row in coins]
        payload = json.dumps({"symbols": current_coin_list})
        self.redis_client.publish(config_redis.COIN_CHANNEL, payload)
        self.logger.info(f"Published {len(current_coin_list)} coins to COIN_CHANNEL.")
    def store_order_book(self, symbol, price, volume, side):
        """
        Insert one order book level into orderbook_data table.
        
        *A more institutional approach might store entire snapshots in a specialized structure
         or even only store aggregated stats instead of every single level.
        """
        insert_sql = f"""
        INSERT INTO {db_config.DB_TRADING_SCHEMA}.orderbook_data (symbol, price, volume, side)
        VALUES (%s, %s, %s, %s)
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(insert_sql, (symbol, price, volume, side))
            self.conn.commit()
            #self.logger.debug(f"Order book row inserted {symbol}: {price} / {volume} / {side}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting order book data: {e}")
        finally:
            cursor.close()

    def store_updated_coins_list(self, symbols):
        """
        Expect data as such:
        ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']

        Inserts each symbol into the trading.current_coins table with the current timestamp.
        """
        # Get the current UTC timestamp
        now = datetime.datetime.utcnow()
        
        # SQL Insert statement. Using placeholders for parameterized queries.
        
        
        try:
            cursor = self.conn.cursor()
            sql = f"DELETE FROM {db_config.DB_TRADING_SCHEMA}.current_coins;"
            cursor.execute(sql)
            #self.conn.commit()              
            sql = f"INSERT INTO {db_config.DB_TRADING_SCHEMA}.current_coins (symbol, timestamp) VALUES (%s, %s);"
            # Use a cursor from your database connection
            with self.conn.cursor() as cur:
                for symbol in symbols:
                    cur.execute(sql, (symbol, now))
            self.conn.commit()
            print("storing symbols")
        except Exception as e:
            self.logger.error(f"Error storing symbols: {e}")
            self.conn.rollback()  
    def handle_coin_list_update(self,cdata):
        """
            Expect data as such
            {'symbols': ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']}

        """
        conn = self.conn
        try:
            with conn:
                with conn.cursor() as cur:
                    # Option 1: Create a new table with the same data as the source table
                    cur.execute(f"TRUNCATE {db_config.DB_TRADING_SCHEMA}.previous_coins;")
                    cur.execute(f"INSERT INTO {db_config.DB_TRADING_SCHEMA}.previous_coins SELECT * FROM {db_config.DB_TRADING_SCHEMA}.current_coins;")

                    # Option 2: If new_table already exists, insert data from old_table into it:
                    # cur.execute("INSERT INTO new_table SELECT * FROM old_table;")
                    self.store_updated_coins_list(cdata['symbols'])  
        finally:
            print("Done")   
    def _update_bot_last_seen(self, bot_name, timestamp):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE {db_config.DB_TRADING_SCHEMA}.bots
                SET last_updated = %s
                WHERE bot_name = %s
            """, (timestamp, bot_name))
            self.conn.commit()
            self.logger.debug(f"✅ Updated last_seen for {bot_name} at {timestamp}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ Failed to update last_seen for {bot_name}: {e}")
        finally:
            cursor.close()
    def _start_heartbeat_listener(self):
        def listen_heartbeat():
            heartbeat_client = redis.Redis(
                host=config_redis.REDIS_HOST,
                port=config_redis.REDIS_PORT,
                db=config_redis.REDIS_DB,
                decode_responses=True
            )
            pubsub = heartbeat_client.pubsub()
            pubsub.subscribe(HEARTBEAT_CHANNEL)

            while self.running:
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                now = datetime.datetime.utcnow()
                if message and message["type"] == "message":
                    try:
                        payload = json.loads(message["data"])
                        bot_name = payload.get("bot_name")
                        timestamp = payload.get("time")
                        # If this is a new bot, add to bot_status and update DB to started
                        if bot_name and timestamp:
                            if bot_name not in self.bot_status or self.bot_status[bot_name]['status'] == 'stopped':
                                # Compose a status update object using payload fields
                                status_obj = {
                                    "bot_name": bot_name,
                                    "status": "started",
                                    "time": timestamp,
                                    "auth_token": payload.get("auth_token", ""),
                                    "metadata": payload.get("metadata", {})
                                }
                                self.handle_bot_status_update(status_obj)
                            self._update_bot_last_seen(bot_name, timestamp)
                            self.bot_status[bot_name] = {'last_seen': now, 'status': 'started'}
                    except Exception as e:
                        self.logger.error(f"Failed to handle heartbeat message: {e}")
                # Check for missed heartbeats for all bots in self.bot_status
                for bot, info in list(self.bot_status.items()):
                    last_seen = info['last_seen']
                    if not last_seen or (now - last_seen).total_seconds() > self.heartbeat_timeout:
                        if info['status'] != 'stopped':
                            self.logger.warning(f"No heartbeat from {bot} for over {self.heartbeat_timeout} seconds. Marking as stopped.")
                            self._mark_bot_stopped(bot, now)
                            self.bot_status[bot]['status'] = 'stopped'
                time.sleep(0.5)

        threading.Thread(target=listen_heartbeat, daemon=True).start()   
    def _mark_bot_stopped(self, bot_name, timestamp):
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE {db_config.DB_TRADING_SCHEMA}.bots
                SET status = %s, last_updated = %s
                WHERE bot_name = %s
            """, ("stopped", timestamp, bot_name))
            self.conn.commit()
            self.logger.info(f"🔴 Marked {bot_name} as stopped in DB at {timestamp}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ Failed to mark {bot_name} as stopped: {e}")
        finally:
            cursor.close()
    def _archive_kline_data(self):
        timestamp = datetime.datetime.now(pytz.timezone("Australia/Sydney")).strftime("%Y%m%d_%H%M%S")

        archive_table = f"{db_config.DB_TRADING_SCHEMA}.kline_data_{timestamp}"
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"CREATE TABLE {archive_table} AS SELECT * FROM {db_config.DB_TRADING_SCHEMA}.kline_data;")
            cursor.execute(f"""
                DELETE FROM {db_config.DB_TRADING_SCHEMA}.kline_data 
                WHERE id NOT IN (
                    SELECT id FROM {db_config.DB_TRADING_SCHEMA}.kline_data 
                    ORDER BY start_time DESC LIMIT 50
                );
            """)
            self.conn.commit()
            self.logger.info(f"✅ Archived kline_data to {archive_table} and preserved last 50 rows.")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ Failed to archive kline_data: {e}")
        finally:
            cursor.close()   

    """
    =-=-=-=-=-=-=- Data handlers
    """    
    def handle_kline_update(self, kdata):
        try:
            symbol = kdata["symbol"]
            interval = kdata["interval"]
            start_dt = pd.to_datetime(kdata["start_time"]).tz_localize('UTC') if pd.to_datetime(kdata["start_time"]).tzinfo is None else pd.to_datetime(kdata["start_time"]).tz_convert('UTC')

            open_price = float(kdata["open"])
            close_price = float(kdata["close"])
            high_price = float(kdata["high"])
            low_price = float(kdata["low"])
            volume = float(kdata["volume"])
            turnover = float(kdata["turnover"])
            confirmed = bool(kdata.get("confirmed", True))

            # Additional indicators
            rsi = kdata.get("RSI")
            macd = kdata.get("MACD")
            macd_signal = kdata.get("MACD_Signal")
            macd_hist = kdata.get("MACD_Hist")
            ma = kdata.get("MA")
            upper_band = kdata.get("UpperBand")
            lower_band = kdata.get("LowerBand")
            volume_ma = kdata.get("Volume_MA") or kdata.get("volume_ma")
            volume_change = kdata.get("Volume_Change") or kdata.get("volume_change")
            volume_slope = kdata.get("Volume_Slope") or kdata.get("volume_slope")
            rvol = kdata.get("RVOL") or kdata.get("rvol")
            market = kdata.get("market") or "unknown2"  # Default to a market if not provided
            #self.logger.debug(f"Inserting kline: {json.dumps(kdata,indent=2)}")
            insert_sql = f"""
            INSERT INTO {db_config.DB_TRADING_SCHEMA}.kline_data 
                (symbol, interval, start_time, open, close, high, low, volume, turnover, confirmed, 
                rsi, macd, macd_signal, macd_hist, ma, upper_band, lower_band, 
                volume_ma, volume_change, volume_slope, rvol,market)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s,%s) 
                        ON CONFLICT (symbol, interval, start_time) DO UPDATE SET
                            close = EXCLUDED.close,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            volume = EXCLUDED.volume,
                            turnover = EXCLUDED.turnover,
                            confirmed = EXCLUDED.confirmed,
                            rsi = EXCLUDED.rsi,
                            macd = EXCLUDED.macd,
                            macd_signal = EXCLUDED.macd_signal,
                            macd_hist = EXCLUDED.macd_hist,
                            ma = EXCLUDED.ma,
                            upper_band = EXCLUDED.upper_band,
                            lower_band = EXCLUDED.lower_band,
                            volume_ma = EXCLUDED.volume_ma,
                            volume_change = EXCLUDED.volume_change,
                            volume_slope = EXCLUDED.volume_slope,
                            rvol = EXCLUDED.rvol,
                            market=EXCLUDED.market
            """

            cursor = self.conn.cursor()
            cursor.execute(insert_sql, (
                symbol, interval, start_dt, open_price, close_price,
                high_price, low_price, volume, turnover, confirmed,
                rsi, macd, macd_signal, macd_hist, ma, upper_band, lower_band,
                volume_ma, volume_change, volume_slope, rvol,market
            ))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting enriched kline: {e}")   
    def handle_trade_update(self, tdata):
        try:
            symbol = tdata["symbol"]
            minute_start = pd.to_datetime(tdata["minute_start"], utc=True)
            total_volume = float(tdata["total_volume"])
            vwap = float(tdata["vwap"])
            trade_count = int(tdata["trade_count"])
            largest_trade_volume = float(tdata["largest_trade_volume"])
            largest_trade_price = float(tdata["largest_trade_price"])

            self.trade_buffer.append((
                symbol, minute_start, total_volume, vwap, trade_count, largest_trade_volume, largest_trade_price
            ))
        except Exception as e:
            self.logger.error(f"🚨 Error buffering summarized trade data: {e}")


    def _flush_trade_buffer(self):
        while self.running:
            if self.trade_buffer:
                try:
                    cursor = self.conn.cursor()
                    insert_sql = f"""
                    INSERT INTO {db_config.DB_TRADING_SCHEMA}.trade_summary_data
                    (symbol, minute_start, total_volume, vwap, trade_count, largest_trade_volume, largest_trade_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, minute_start) DO UPDATE
                    SET
                        total_volume = EXCLUDED.total_volume,
                        vwap = EXCLUDED.vwap,
                        trade_count = EXCLUDED.trade_count,
                        largest_trade_volume = EXCLUDED.largest_trade_volume,
                        largest_trade_price = EXCLUDED.largest_trade_price
                    """
                    cursor.executemany(insert_sql, self.trade_buffer)
                    self.conn.commit()
                    inserted = cursor.rowcount
                    #self.logger.info(f" Bulk inserted {inserted} summarized trades into DB")
                    self.trade_buffer.clear()
                except Exception as e:
                    self.conn.rollback()
                    self.logger.error(f" Error during bulk insert of trades: {e}")
                finally:
                    cursor.close()
            time.sleep(5)  # Adjust if needed


    def handle_orderbook_update(self, odata):
        """
        Expects something like:
        {
          "symbol": "BTCUSDT",
          "bids": [[price, volume], [price, volume], ...],
          "asks": [[price, volume], [price, volume], ...]
        }
        This might be big, so consider storing only aggregated or partial data.
        """
        try:
            symbol = odata["symbol"]
            bids = odata["bids"]
            asks = odata["asks"]

            # Possibly store partial or aggregated data. 
            # For example, just store top few or run some logic.
            for p, v in bids[:5]:  # store top 5 bids
                self.store_order_book(symbol, float(p), float(v), "bid")

            for p, v in asks[:5]:  # store top 5 asks
                self.store_order_book(symbol, float(p), float(v), "ask")

        except KeyError as e:
            self.logger.error(f"Missing key in orderbook update: {e}") 
    def handle_macro_metrics(self, data):
        """
        Insert macro metrics data into the macro_metrics table.
        Args:
            data (dict): Macro metrics report with keys matching table columns.
        """
        try:
            cursor = self.conn.cursor()
            insert_sql = f"""
            INSERT INTO {db_config.DB_TRADING_SCHEMA}.macro_metrics (
                timestamp, btc_dominance, eth_dominance, total_market_cap, total_volume,
                active_cryptos, markets, fear_greed_index, market_sentiment, btc_open_interest, us_inflation
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
            """
            cursor.execute(insert_sql, (
                data['timestamp'],
                data.get('btc_dominance'),
                data.get('eth_dominance'),
                data.get('total_market_cap'),
                data.get('total_volume'),
                data.get('active_cryptos'),
                data.get('markets'),
                data.get('fear_greed_index'),
                data.get('market_sentiment'),
                data.get('btc_open_interest'),
                data.get('us_inflation')
            ))
            self.conn.commit()
            cursor.close()
            self.logger.info(f"✅ Inserted macro metric @ {data['timestamp']}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"❌ Error inserting macro metric: {e}")

    def _load_initial_bot_status(self):
        """
        Load all bots from the bots table and initialize their status as 'stopped' with last_seen=None.
        Returns:
            dict: Mapping of bot_name to status and last_seen timestamp.
        """
        status = {}
        cursor = self.conn.cursor()
        try:
            cursor.execute(f"SELECT bot_name, status, last_updated FROM {db_config.DB_TRADING_SCHEMA}.bots")
            for bot_name, db_status, last_updated in cursor.fetchall():
                last_seen = last_updated if last_updated else None
                status[bot_name] = {'last_seen': last_seen, 'status': db_status or 'stopped'}
        except Exception as e:
            self.logger.error(f"Failed to load initial bot status: {e}")
        finally:
            cursor.close()
        return status

    def handle_bot_status_update(self, status_obj):
        """
        Update the status of a bot in the bots table based on a status object.
        Args:
            status_obj (dict): Contains bot_name, status, time, auth_token, and metadata.
        """
        cursor = self.conn.cursor()
        bot_name = status_obj.get("bot_name")
        status = status_obj.get("status")
        time_str = status_obj.get("time")
        meta = status_obj.get("metadata", {})
        auth_token = status_obj.get("auth_token")
        self.logger.info(f"📨 Received status update for bot: {bot_name}")
        self.logger.debug(f"Status object: {json.dumps(status_obj, indent=2)}")

        if not all([bot_name, status, time_str, auth_token]):
            self.logger.error(f"❌ Missing required fields in status_obj: {status_obj}")
            cursor.close()
            return

        hashed_auth_token = hashlib.sha256(auth_token.encode()).hexdigest()

        # ✅ Auth Check
        cursor.execute(f"""
            SELECT token FROM {db_config.DB_TRADING_SCHEMA}.bot_auth
            WHERE bot_name = %s
        """, (bot_name,))
        row = cursor.fetchone()

        if not row:
            self.logger.warning(f"❗ No auth record found for bot '{bot_name}'")
            cursor.close()
            return

        db_token = row[0]
        if db_token != hashed_auth_token:
            self.logger.warning(f"❌ Invalid auth token for bot '{bot_name}'")
            cursor.close()
            return

        # 🔄 Check if bot exists in bots table
        cursor.execute(f"""
            SELECT 1 FROM {db_config.DB_TRADING_SCHEMA}.bots
            WHERE bot_name = %s
        """, (bot_name,))
        exists = cursor.fetchone()

        if exists:
            self.logger.info(f"📝 Updating bot '{bot_name}' status to '{status}' at {time_str}")
            cursor.execute(f"""
                UPDATE {db_config.DB_TRADING_SCHEMA}.bots
                SET status = %s,
                    last_updated = %s,
                    metadata = %s
                WHERE bot_name = %s
            """, (status, time_str, json.dumps(meta), bot_name))
            self.conn.commit()
            self.logger.info(f"✅ Update committed for bot '{bot_name}'")
        else:
            self.logger.warning(f"⚠️ Bot '{bot_name}' not found in bots table. Skipping update.")

        cursor.close()