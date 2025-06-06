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
import pytz
import psycopg2
import hashlib
import logging
import pandas as pd
import psycopg2.extras
import config.config_db as db_config
import config.config_redis as config_redis
import config.config_ws as ws_config  # If you store Redis host/port here or wherever
from sqlalchemy import create_engine
from utils.logger import setup_logger
#from utils.global_indicators import GlobalIndicators # TODO Possible to delete ,central indicators script so all bots and services using the same ones
from psycopg2.extensions import register_adapter, AsIs
class PostgresDBBot:
    def __init__(self, log_filename=db_config.LOG_FILENAME):
        # Logger
        self.logger = setup_logger(db_config.LOG_FILENAME,getattr(logging, db_config.LOG_LEVEL.upper(), logging.WARNING)) # Set up logger and retrieve the logger type from config 
        self.logger.info("Initializing PostgresDBBot...")                
        #self.GlobalIndicators = GlobalIndicators() # TODO possible to delete
        self.host = db_config.DB_HOST
        self.port = db_config.DB_PORT
        
        self.status = {
                        "bot_name": db_config.BOT_NAME,
                        "status": "started",
                        "time": datetime.datetime.utcnow().isoformat(),
                        "auth_token": db_config.BOT_AUTH_TOKEN,  # each bot has its own
                        "metadata": {
                            "version": "1.2.0",
                            "pid": os.getpid(),
                            "strategy": "VWAP"
                        }
                    }
        self.database = db_config.DB_DATABASE
        # ✅ Use SQLAlchemy to create a database engine
        self.db_engine = create_engine(
            f"postgresql://{db_config.DB_USER}:{db_config.DB_PASSWORD}@{self.host}:{self.port}/{self.database}"
        )
        print("Database Bot (Postgresql) is running .......")
        self._setup_postgres()
        #self._initial_gap_fix()  # This is a ONE time run function only. mostly no need to uncomment. it was used to fill database gaps. 24/04/2025
        self._setup_redis()
        # Control flag for run loop
        self.running = True
        self._start_archive_scheduler()
        self._start_heartbeat_listener()  
        self._start_self_heartbeat() 
                   
        self._start_gap_check()



  
    """
    =-=-=-=-=-=-=- Internal Bot operational functions
    """  
    # TODO Add function to get all the bots from the table, send an 'are you alive' message to each redis channel per bot
    # TODO Have a time out for all bots, if no response mark as 'stopped', otherwise 'started'
    def _initial_gap_fix(self):
        symbols = [row[0] for row in self._retrieve_coins()]
        intervals = ["1", "5", "60", "D"]  # Adjust as needed
        for symbol in symbols:
            for interval in intervals:
                self._fix_data_gaps(symbol, interval)
    def _start_gap_check(self):
        def periodic_gap_check():
            while self.running:
                symbols = [row[0] for row in self._retrieve_coins()]
                intervals = ["1", "5", "15", "60","D"]  # Customize intervals as needed
                for symbol in symbols:
                    for interval in intervals:
                        self._fix_data_gaps(symbol, interval)
                self.logger.info("Completed gap checks for all symbols/intervals. Next check in 6 hours.")
                time.sleep(21600)  # Check every 6 hours
        threading.Thread(target=periodic_gap_check, daemon=True).start()
     #   -----POSTGRESQL SETUP
    def _setup_postgres(self):
        self.db_engine = create_engine(
            f"postgresql://{db_config.DB_USER}:{db_config.DB_PASSWORD}@{self.host}:{self.port}/{self.database}"
        )
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=db_config.DB_USER,
            password=db_config.DB_PASSWORD
        )
        self.conn.autocommit = False
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
            config_redis.REQUEST_COINS  # Used by websocket to indicate it needs coins. 
        )
        self.logger.info("Subscribed to Redis Pub/Sub channels.")
    def _connect_to_redis(self):
        while True:
            try:
                client = redis.Redis(
                    host=config_redis.REDIS_HOST,
                    port=config_redis.REDIS_PORT,
                    db=config_redis.REDIS_DB,
                    decode_responses=True,
                    socket_keepalive=True,
                    retry_on_timeout=True
                )
                client.ping()  # Check if Redis is responsive
                return client
            except redis.ConnectionError:
                print("Redis connection failed. Retrying in 5 seconds...")
                time.sleep(5)    
 

    def run(self):
        self.handle_bot_status_update(self.status)
        """Run loop listening to Redis Pub/Sub channels."""
        self.large_trade_thresholds = self.get_thresholds()
        cursor = self.conn.cursor()
        
        cursor.execute(f"SET search_path TO {db_config.DB_TRADING_SCHEMA};")
        self.logger.info("DB Bot running, listening to Redis Pub/Sub...")
        while self.running:
            try:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message:
                    # message['type'] = 'message'
                    # message['channel'] = 'kline_updates' or 'trade_updates'
                    # message['data'] = the actual published string
                    channel = message['channel']
                    data_str = message['data']

                    # Try parse JSON
                    try:
                        data_obj = json.loads(data_str)
                    except json.JSONDecodeError:
                        self.logger.error(f"Invalid JSON from channel={channel}: {data_str}")
                        continue

                    if channel == config_redis.PRE_PROC_KLINE_UPDATES:
                        self.handle_kline_update(data_obj)
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
            except redis.ConnectionError as e:
                self.logger.error(f"Redis connection lost: {e}. Reconnecting...")
                time.sleep(2)  # Wait before retrying
                self.pubsub = self.redis_client.pubsub()  # Reinitialize Redis Pub/Sub
                self.pubsub.subscribe(
                        config_redis.COIN_CHANNEL,
                        config_redis.PRE_PROC_KLINE_UPDATES,
                        config_redis.PRE_PROC_TRADE_CHANNEL,
                        config_redis.PRE_PROC_ORDER_BOOK_UPDATES,
                        config_redis.RESYNC_CHANNEL,
                        config_redis.SERVICE_STATUS_CHANNEL,
                        config_redis.REQUEST_COINS  # Used by websocket to indicate it needs coins. 
                    )

            time.sleep(0.1)   
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
        self.status["status"] = "stopped"
        self.status["time"] = datetime.datetime.utcnow().isoformat()    
        self.handle_bot_status_update(self.status)
        self.pubsub.close()
        if self.conn:
            self.conn.close()
        self.logger.info("DB Bot Stopped.")   
    def _start_archive_scheduler(self):
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
        def self_heartbeat():
            while self.running:
                try:
                    payload = {
                        "bot_name": db_config.BOT_NAME,
                        "heartbeat": True,
                        "time": datetime.datetime.utcnow().isoformat()
                    }
                    self.redis_client.publish(config_redis.HEARTBEAT_CHANNEL, json.dumps(payload))
                    self.logger.debug("❤️ Self-heartbeat sent.")
                except Exception as e:
                    self.logger.warning(f"Failed to send self-heartbeat: {e}")
                time.sleep(60)

        threading.Thread(target=self_heartbeat, daemon=True).start()    
    """
    =-=-=-=-=-=-=- Database operations
    """  
    def handle_bot_status_update(self, status_obj):
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

        if row[0] != hashed_auth_token:
            self.logger.warning(f"❌ Invalid auth token for bot '{bot_name}'")
            cursor.close()
            return

        self.logger.info(f"🔐 Authenticated bot '{bot_name}'")

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

        # Special case for websocket bot
        if bot_name == "websocket_bot" and status == "started":
            self.logger.info("🌐 WebSocket bot started – publishing current coin list.")
            self._publish_current_coin_list()
    def get_thresholds(self):
        """
        Retrieve large trade thresholds from the database.
        """
        sql = f"""
            SELECT * FROM {db_config.DB_TRADING_SCHEMA}.large_trade_thresholds
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            thresholds = {row[0]: row[1] for row in rows}
            self.logger.info(f"Retrieved large trade thresholds:")
            return thresholds
        except Exception as e:
            self.logger.error(f"Error retrieving thresholds: {e}")
            return {}
        finally:
            cursor.close()
    def _retrieve_coins(self):
        # The purpose of this function is to ensure that when websocket bot starts it firsts looks for any stored coins in the database and resubscribes.
        #   This helps in the case of a power outage or crash, websocket bot can resubscribe to current coins without prompting. 
        #   This should replace the coin_feeder.py for auto updates when websocket starts. 
        self.logger.info(f"retrieving current_coins")
        sql = f"""
            SELECT symbol FROM {db_config.DB_TRADING_SCHEMA}.current_coins
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            coins = cursor.fetchall()
            self.logger.info(f"Retrieved {len(coins)} coin(s) from DB")
            return coins
        except Exception as e:
            self.logger.error(f"Error retrieving coins: {e}")
            return []
        finally:
            cursor.close()
    def _publish_current_coin_list(self):
        coins = self._retrieve_coins()
        current_coin_list = [row[0] for row in coins]
        payload = json.dumps({"symbols": current_coin_list})
        self.redis_client.publish(config_redis.COIN_CHANNEL, payload)
        self.logger.info(f"Published {len(current_coin_list)} coins to COIN_CHANNEL.")
    def store_kline_data(self, symbol, interval, start_time, open_price, close_price,
                         high_price, low_price, volume, turnover, confirmed):
        """
        Insert or update a trading.kline row. 
        Typically you insert a new row each time a candle closes or to track partial updates.
        """
        insert_sql = f"""
        INSERT INTO {db_config.DB_TRADING_SCHEMA}.kline_data 
        (symbol, interval, start_time, open, close, high, low, volume, turnover, confirmed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, interval, start_time) DO NOTHING
        """

        cursor = self.conn.cursor()
        try:
            cursor.execute(insert_sql, (
                symbol,
                interval,
                start_time,  # Python datetime, or convert from epoch
                open_price,
                close_price,
                high_price,
                low_price,
                volume,
                turnover,
                confirmed
            ))
            self.conn.commit()

            #self.logger.info(f"Kline data inserted for {symbol}, interval={interval}, time={start_time}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting kline: {e}")
        finally:
            cursor.close()
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
    def store_trade_data(self, symbol, trade_time, price, volume):
        """
        Insert trade data into trade_data table.
        """
        insert_sql = f"""
        INSERT INTO {db_config.DB_TRADING_SCHEMA}.trade_data (symbol, trade_time, price, volume)
        VALUES (%s, %s, %s, %s)
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(insert_sql, (symbol, trade_time, price, volume))
            self.conn.commit()
            self.logger.debug(f"Trade inserted for {symbol} @ {price}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting trade data: {e}")
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
            sql = f"TRUNCATE TABLE {db_config.DB_TRADING_SCHEMA}.current_coins RESTART IDENTITY;"
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
                    cur.execute(f"DROP TABLE if EXISTS {db_config.DB_TRADING_SCHEMA}.previous_coins;")
                    cur.execute(f"CREATE TABLE {db_config.DB_TRADING_SCHEMA}.previous_coins AS SELECT * FROM current_coins;")
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
            pubsub.subscribe(config_redis.HEARTBEAT_CHANNEL)

            while self.running:
                message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message and message["type"] == "message":
                    try:
                        payload = json.loads(message["data"])
                        bot_name = payload.get("bot_name")
                        timestamp = payload.get("time")
                        if bot_name and timestamp:
                            self._update_bot_last_seen(bot_name, timestamp)
                    except Exception as e:
                        self.logger.error(f"Failed to handle heartbeat message: {e}")
                time.sleep(0.5)

        threading.Thread(target=listen_heartbeat, daemon=True).start()   
    def _archive_kline_data(self):
        timestamp = datetime.datetime.now(pytz.timezone("Australia/Sydney")).strftime("%d.%m.%Y %H:%M:%S")
        archive_table = f"kline_data_{timestamp}"
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
            start_dt = datetime.datetime.fromisoformat(kdata["start_time"])
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
            self.logger.debug(f"Inserting kline: {json.dumps(kdata,indent=2)}")
            insert_sql = f"""
            INSERT INTO {db_config.DB_TRADING_SCHEMA}.kline_data 
                (symbol, interval, start_time, open, close, high, low, volume, turnover, confirmed, 
                rsi, macd, macd_signal, macd_hist, ma, upper_band, lower_band, 
                volume_ma, volume_change, volume_slope, rvol)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s)
            """

            cursor = self.conn.cursor()
            cursor.execute(insert_sql, (
                symbol, interval, start_dt, open_price, close_price,
                high_price, low_price, volume, turnover, confirmed,
                rsi, macd, macd_signal, macd_hist, ma, upper_band, lower_band,
                volume_ma, volume_change, volume_slope, rvol
            ))
            self.conn.commit()
            cursor.close()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting enriched kline: {e}")   
    def handle_trade_update(self, tdata):
        """
        ✅ Store only large trades in PostgreSQL.
        """
        try:
            # ✅ Extract the actual trade information
            if "trade" in tdata and "data" in tdata["trade"] and len(tdata["trade"]["data"]) > 0:
                trade_info = tdata["trade"]["data"][0]  # ✅ Get the latest trade
            else:
                print(f"🚨 ERROR: Invalid trade format: {tdata}")  # Debugging
                return  # 🚨 Skip this trade

            symbol = tdata["symbol"]
            trade_time = trade_info["T"]  
            price = float(trade_info["p"])  
            volume = float(trade_info["v"])  

            trade_dt = datetime.datetime.utcfromtimestamp(trade_time / 1000)  

            # ✅ Set a threshold per coin (Modify as needed)
            threshold = self.large_trade_thresholds.get(symbol, 10)  # Default threshold = 10

            # ✅ Only store large trades
            if volume >= threshold:
                #print(f"✅ Storing large trade in DB: {symbol} | {volume} units @ {price}")  # Debugging
                self.store_trade_data(symbol, trade_dt, price, volume)

        except KeyError as e:
            self.logger.error(f"🚨 Missing key in trade update: {e}")
            print(f"🚨 ERROR: Missing key {e} in trade data: {tdata}")  # Debugging    
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
        try:
            cursor = self.conn.cursor()
            insert_sql = """
            INSERT INTO macro_metrics (
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

    def adapt_numpy_int64(self,numpy_int64):
        return AsIs(numpy_int64)

    def adapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)   
      
    def _fix_data_gaps(self, symbol, interval, lookback=5000):
        cursor = self.conn.cursor()
        try:
            self.logger.info(f"Checking gaps in {symbol}-{interval}")
            sql = f"""
                SELECT start_time FROM {db_config.DB_TRADING_SCHEMA}.kline_data
                WHERE symbol = %s AND interval = %s
                ORDER BY start_time DESC LIMIT %s
            """
            df = pd.read_sql(sql, self.conn, params=(symbol, interval, lookback))
            df['start_time'] = pd.to_datetime(df['start_time'])
            df.set_index('start_time', inplace=True)

            interval_minutes = int(interval)
            expected_times = pd.date_range(start=df.index.min(), end=df.index.max(), freq=f'{interval_minutes}min')
            missing_times = expected_times.difference(df.index)

            if missing_times.empty:
                self.logger.info(f"No gaps for {symbol}-{interval}")
                return

            gap_groups = pd.Series(missing_times).groupby(
                (pd.Series(missing_times).diff() != pd.Timedelta(minutes=interval_minutes)).cumsum()
            )

            for _, gap in gap_groups:
                start_gap = gap.min()
                end_gap = gap.max()
                self.logger.info(f"Gap detected: {start_gap} to {end_gap} ({symbol}-{interval})")
                klines = self._fetch_bybit_klines(symbol, interval, start_gap, end_gap + pd.Timedelta(minutes=interval_minutes))
                if klines:
                    self._insert_missing_klines(symbol, interval, klines)
                    self.logger.info(f"Inserted {len(klines)} missing klines for gap {start_gap} - {end_gap}")
                else:
                    self.logger.warning(f"No klines returned for gap {start_gap} - {end_gap}")

        except Exception as e:
            self.logger.error(f"Gap-fix error: {e}")
        finally:
            cursor.close()


    def _fetch_bybit_klines(self, symbol, interval, start_time, end_time):
        import requests
        klines = []
        interval_minutes = int(interval)
        start_ts = int(start_time.timestamp() * 1000)
        end_ts = int(end_time.timestamp() * 1000)

        while start_ts <= end_ts:
            params = {
                'symbol': symbol,
                'interval': interval,
                'start': start_ts,
                'limit': 1000,
                'category': 'linear'
            }
            resp = requests.get("https://api.bybit.com/v5/market/kline", params=params).json()

            if resp['retCode'] != 0 or not resp['result']['list']:
                self.logger.warning(f"No API data returned: {resp}")
                break

            batch = resp['result']['list']
            klines.extend(batch)

            last_ts = int(batch[-1][0])

            if last_ts <= start_ts:
                break

            start_ts = last_ts + interval_minutes * 60000  # Advance correctly to next candle

            time.sleep(0.2)  # Respect API rate limits

        return klines


    def _insert_missing_klines(self, symbol, interval, klines):
        cursor = self.conn.cursor()
        insert_sql = f"""
            INSERT INTO {db_config.DB_TRADING_SCHEMA}.kline_data 
            (symbol, interval, start_time, open, high, low, close, volume, turnover, confirmed)
            VALUES (%s, %s, to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (symbol, interval, start_time) DO NOTHING
        """
        try:
            for k in klines:
                cursor.execute(insert_sql, (
                    symbol, interval, int(k[0]), float(k[1]), float(k[2]),
                    float(k[3]), float(k[4]), float(k[5]), float(k[6])
                ))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Inserting klines failed: {e}")
        finally:
            cursor.close()


if __name__ == "__main__":
    # Example usage
    open('logs/DB_BOT.log', 'w').close()
    db_bot = PostgresDBBot("DB_BOT.log")
    try:
        db_bot.run()
    except KeyboardInterrupt:
        db_bot.logger.info("Keyboard interrupt - stopping DB Bot.")
    finally:
        db_bot.stop()
    db_bot.close()
