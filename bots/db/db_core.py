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

"""
This file contains the core PostgresDBBot class and all DB/Redis handlers.
Gap-fix and Bybit klines logic is now in gap_utils.py for modularity.
Startup is handled by a runner script, not by __main__ here.
"""

import json
import time
import threading
import datetime
import redis
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import pytz
import logging
import pandas as pd
import config.config_db as db_config
import config.config_redis as config_redis
from BOTStatusHandler import BOTStatusHandler
from utils.db_postgres import PostgresHandler
from utils.redis_handler import RedisHandler
from config.config_common import  HEARTBEAT_TIMEOUT_SECONDS, HEARTBEAT_CHANNEL
from sqlalchemy import create_engine
from utils.logger import setup_logger
from utils.HeartBeatService import HeartBeat
from psycopg2.extras import execute_values
from gap_utils import fix_all_data_gaps
from kline_handler import KlineHandler
from trade_handler import TradeHandler
from macro_handler import MacroHandler
from heartbeat_listener import HeartbeatListener
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')


# --- In PostgresDBBot ---

class PostgresDBBot:
    def __init__(self, log_filename=db_config.LOG_FILENAME):
        
        self.logger = setup_logger(log_filename, getattr(logging, db_config.LOG_LEVEL.upper(), logging.WARNING))
        self.db = PostgresHandler(self.logger)
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
        self.bot_status = self._load_initial_bot_status()
        self.conn = None
        self.db_engine = create_engine(
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
        )
        self.redis_handler = RedisHandler(config_redis, self.logger)
        self.redis_handler.connect()
        self.redis_handler.subscribe([config_redis.COIN_CHANNEL,
            config_redis.PRE_PROC_KLINE_UPDATES,
            config_redis.PRE_PROC_TRADE_CHANNEL,
            config_redis.PRE_PROC_ORDER_BOOK_UPDATES,
            config_redis.RESYNC_CHANNEL,
            config_redis.SERVICE_STATUS_CHANNEL,
            config_redis.MACRO_METRICS_CHANNEL,
            config_redis.REQUEST_COINS,
            config_redis.RAW_TRADE_CHANNEL,
            config_redis.DB_SAVE_SUBSCRIPTIONS,
            config_redis.DB_REQUEST_SUBSCRIPTIONS])
        self.logger.info("Redis connected and subscribed to channels.")
        self.redis_client = self.redis_handler.client
        self.pubsub = self.redis_handler.pubsub
        self.logger.info("Redis setup complete.")
        self.running = True
        self.subscribed_channels = set()
        self.status_handler = BOTStatusHandler(self)
        self.heartbeat_timeout = HEARTBEAT_TIMEOUT_SECONDS
        self.heartbeat = HeartBeat(
            bot_name=db_config.BOT_NAME,
            auth_token=db_config.BOT_AUTH_TOKEN,
            logger=self.logger,
            redis_handler=self.redis_handler,
            metadata=self.status
        )
        self.logger.info("Initializing PostgresDBBot...")
        self.conn = self.db.conn
        self.Klinehandler = KlineHandler(self.conn, self.logger)
        self.trade_handler = TradeHandler(self.conn, self.logger, db_config)
        self.macro_handler = MacroHandler(self.conn, self.logger, db_config)
        self.heartbeat_listener = HeartbeatListener(self, config_redis, HEARTBEAT_CHANNEL, self.logger)
        self.heartbeat_listener.start()
        if self.conn is None:
            self.logger.error("❌ Database connection failed! Exiting.")
            raise RuntimeError("Database connection failed")
                # --- Fix data gaps at startup ---
        try:
            fix_all_data_gaps(self)
        except Exception as e:
            self.logger.error(f"Error during gap fixing: {e}")
        self._start_archive_scheduler()
        threading.Thread(target=self.trade_handler.flush_trade_buffer, args=(lambda: self.running,), daemon=True).start()
    def _start_archive_scheduler(self):
        def scheduler():
            while self.running:
                now = datetime.datetime.now(pytz.timezone("Australia/Sydney"))
                target = now.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
                sleep_duration = (target - now).total_seconds()
                self.logger.info(f"Next archive scheduled in {sleep_duration / 3600:.2f} hours.")
                time.sleep(sleep_duration)
                self.KlineHandler._archive_kline_data()
        threading.Thread(target=scheduler, daemon=True).start()

    def _listen_to_redis(self):
        self.logger.info("🧠 Redis listener thread started.")
        while self.running:
            try:
                message = self.redis_handler.get_message(timeout=1)
                if message:
                    if self.conn is None:
                        self.logger.error("❌ DB connection is None before DB operation!")
                    channel = message['channel']
                    data_str = message['data']
                    #self.logger.info(f"[DEBUG] Received Redis message on channel: {channel} | data: {data_str}")
                    try:
                        data_obj = json.loads(data_str)
                    except json.JSONDecodeError:
                        self.logger.error(f"[DEBUG] Invalid JSON from channel={channel}: {data_str}")
                        continue
                    try:
                        self._route_redis_message(channel, data_obj, message)
                    except Exception as route_exc:
                        self.logger.error(f"[DEBUG] Exception in _route_redis_message: {route_exc}", exc_info=True)
                        # Check connection state before any rollback
                        if hasattr(self, 'conn'):
                            self.logger.error(f"[DEBUG] self.conn is: {self.conn}")
                            if self.conn is None:
                                self.logger.error("[DEBUG] self.conn is None inside _route_redis_message exception handler!")
                            else:
                                try:
                                    self.conn.rollback()
                                    self.logger.error("[DEBUG] Performed rollback after exception in _route_redis_message.")
                                except Exception as rb_exc:
                                    self.logger.error(f"[DEBUG] Rollback itself failed: {rb_exc}", exc_info=True)
                        else:
                            self.logger.error("[DEBUG] self.conn attribute does not exist!")
            except Exception as e:
                self.logger.error(f"❌ Redis listener error (outer): {e}", exc_info=True)
                time.sleep(2)

    def _route_redis_message(self, channel, data_obj, raw_message):
        if channel == config_redis.PRE_PROC_KLINE_UPDATES:
            self.Klinehandler.handle_kline_update(data_obj)
        elif channel == config_redis.DB_REQUEST_SUBSCRIPTIONS:
            market = data_obj.get("market") or data_obj.get("owner")
            if market:
                self.logger.info(f"Publishing websocket subscriptions for market '{market}' to Redis set.")
                self.publish_websocket_subscriptions(market)
        elif channel == config_redis.DB_SAVE_SUBSCRIPTIONS:
            data = json.loads(raw_message['data'])
            self.save_subscription(data['market'], data['symbols'], data['topics'], data['owner'])
        elif channel == config_redis.MACRO_METRICS_CHANNEL:
            self.macro_handler.handle_macro_metrics(data_obj)
        elif channel == config_redis.RAW_TRADE_CHANNEL:
            self.trade_handler.handle_raw_trade(data_obj)
        elif channel == config_redis.PRE_PROC_TRADE_CHANNEL:
            self.trade_handler.handle_trade_update(data_obj)
        elif channel == config_redis.PRE_PROC_ORDER_BOOK_UPDATES:
            self.handle_orderbook_update(data_obj)
        elif channel == config_redis.COIN_CHANNEL:
            self.handle_coin_list_update(data_obj)
        elif channel == config_redis.SERVICE_STATUS_CHANNEL:
            self.status_handler.handle_bot_status_update(data_obj)
        elif channel == config_redis.REQUEST_COINS:
            self.logger.info("Received request for coins list.")
            self._publish_current_coin_list()
        elif channel == config_redis.RESYNC_CHANNEL:
            self.logger.info("Received resync signal, publishing coin list.")
            self._publish_current_coin_list()
        else:
            self.logger.warning(f"Unrecognized channel: {channel}, data={data_obj}")
    def save_subscription(self,market: str, symbols: list, topics: list, owner: str):
        if not owner:
            return
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

    def run(self):
        self.logger.info("DB Bot running, starting Redis listener thread...")
        threading.Thread(target=self._listen_to_redis, daemon=True).start()
        while self.running:
            time.sleep(1)
  
    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed.")

    def stop(self):
        self.running = False
        if hasattr(self, "heartbeat"):
            self.heartbeat_listener.stop()
        try:
            self.logger.info("Sending stopped status update to DB...")
            self.status_handler.handle_bot_status_update(stopped_status)
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
        self.redis_handler.publish(config_redis.COIN_CHANNEL, payload)
        self.logger.info(f"Published {len(current_coin_list)} coins to COIN_CHANNEL.")

    def store_order_book(self, symbol, price, volume, side):
        insert_sql = f"""
        INSERT INTO {db_config.DB_TRADING_SCHEMA}.orderbook_data (symbol, price, volume, side)
        VALUES (%s, %s, %s, %s)
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(insert_sql, (symbol, price, volume, side))
            self.conn.commit()
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            self.logger.error(f"Error inserting order book data: {e}")
        finally:
            cursor.close()

    def store_updated_coins_list(self, symbols):
        now = datetime.datetime.utcnow()
        try:
            cursor = self.conn.cursor()
            sql = f"DELETE FROM {db_config.DB_TRADING_SCHEMA}.current_coins;"
            cursor.execute(sql)
            sql = f"INSERT INTO {db_config.DB_TRADING_SCHEMA}.current_coins (symbol, timestamp) VALUES (%s, %s);"
            with self.conn.cursor() as cur:
                for symbol in symbols:
                    cur.execute(sql, (symbol, now))
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"Error storing symbols: {e}")
            if self.conn:
                self.conn.rollback()

    def handle_coin_list_update(self,cdata):
        conn = self.conn
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE {db_config.DB_TRADING_SCHEMA}.previous_coins;")
                    cur.execute(f"INSERT INTO {db_config.DB_TRADING_SCHEMA}.previous_coins SELECT * FROM {db_config.DB_TRADING_SCHEMA}.current_coins;")
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
            if self.conn:
                self.conn.rollback()
            self.logger.error(f"❌ Failed to update last_seen for {bot_name}: {e}")
        finally:
            cursor.close()

 

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
            if self.conn:
                self.conn.rollback()
            self.logger.error(f"❌ Failed to mark {bot_name} as stopped: {e}")
        finally:
            cursor.close()
    def handle_orderbook_update(self, odata):
        try:
            symbol = odata["symbol"]
            bids = odata["bids"]
            asks = odata["asks"]
            for p, v in bids[:5]:
                self.store_order_book(symbol, float(p), float(v), "bid")
            for p, v in asks[:5]:
                self.store_order_book(symbol, float(p), float(v), "ask")
        except KeyError as e:
            self.logger.error(f"Missing key in orderbook update: {e}") 
    def _load_initial_bot_status(self):
        status = {}
        cursor = self.db.conn.cursor()
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

    def get_websocket_subscriptions(self, market: str) -> list:
        """
        Fetch websocket subscriptions for a given market type.
        Args:
            market (str): The market type (e.g., 'spot', 'linear').
        Returns:
            list: List of subscription dicts/rows.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT * FROM {db_config.DB_TRADING_SCHEMA}.websocket_subscriptions WHERE market = %s",
            (market,)
        )
        rows = cursor.fetchall()
        cursor.close()
        # Convert rows to dicts if needed
        return [dict(zip([desc[0] for desc in cursor.description], row)) for row in rows]

    def publish_websocket_subscriptions(self, market: str):
        """
        Fetch websocket subscriptions for a given market and publish them as a single JSON object
        to the correct Redis list, matching the manual lpush format.
        """
        cur = self.conn.cursor()
        try:
            cur.execute(
                f"SELECT symbol, topic, owner FROM {db_config.DB_TRADING_SCHEMA}.websocket_subscriptions WHERE market = %s",
                (market,)
            )
            rows = cur.fetchall()
            symbols = set()
            topics = set()
            owner = None
            for symbol, topic, row_owner in rows:
                symbols.add(symbol)
                if isinstance(topic, str):
                    try:
                        topic_list = json.loads(topic)
                        topics.update(topic_list if isinstance(topic_list, list) else [topic_list])
                    except Exception:
                        topics.add(topic)
                else:
                    topics.add(topic)
                if not owner and row_owner:
                    owner = row_owner
            if symbols and topics:
                payload = json.dumps({
                    "action": "set",
                    "owner": owner or "db_bot",
                    "market": market,
                    "symbols": sorted(list(symbols)),
                    "topics": sorted(list(topics))
                })
                # Push to the correct Redis list
                redis_list = f"{market}_coin_subscriptions"
                self.redis_handler.client.lpush(redis_list, payload)
            else:
                self.logger.info(f"No websocket subscriptions found for market '{market}'. Redis list not updated.")
        except Exception as e:
            self.logger.error(f"Error publishing websocket subscriptions: {e}")
        finally:
            cur.close()

