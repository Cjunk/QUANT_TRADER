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
import hashlib
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
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')
class PostgresDBBot:
    def __init__(self, log_filename=db_config.LOG_FILENAME):
        
        self.logger = setup_logger(log_filename, getattr(logging, db_config.LOG_LEVEL.upper(), logging.WARNING))
        self.db = PostgresHandler(self.logger)
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
            config_redis.DB_SAVE_SUBSCRIPTIONS,
            config_redis.DB_REQUEST_SUBSCRIPTIONS])
        self.logger.info("Redis connected and subscribed to channels.")
        self.redis_client = self.redis_handler.client
        self.pubsub = self.redis_handler.pubsub
        self.logger.info("Redis setup complete.")
        
        self.Klinehandler = KlineHandler(self.conn, self.logger)
        
        self.running = True
        self.subscribed_channels = set()
        #self.bot_status = {}
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
                # --- Fix data gaps at startup ---
        try:
            fix_all_data_gaps(self)
        except Exception as e:
            self.logger.error(f"Error during gap fixing: {e}")
        self._start_heartbeat_listener()
        self._start_archive_scheduler()
        threading.Thread(target=self._flush_trade_buffer, daemon=True).start()
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
                    channel = message['channel']
                    data_str = message['data']
                    try:
                        data_obj = json.loads(data_str)
                    except json.JSONDecodeError:
                        self.logger.error(f"Invalid JSON from channel={channel}: {data_str}")
                        continue
                    self._route_redis_message(channel, data_obj, message)
            except Exception as e:
                self.logger.error(f"❌ Redis listener error: {e}")
                time.sleep(2)

    def _route_redis_message(self, channel, data_obj, raw_message):
        if channel == config_redis.PRE_PROC_KLINE_UPDATES:
            self.Klinehandler.handle_kline_update(data_obj)
        elif channel == config_redis.DB_REQUEST_SUBSCRIPTIONS:
            self.logger.info("Requesting subscriptions from the db")
            self.handle_request_subscriptions(data_obj)
        elif channel == config_redis.DB_SAVE_SUBSCRIPTIONS:
            data = json.loads(raw_message['data'])
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
            self.status_handler.handle_bot_status_update(data_obj)
        elif channel == config_redis.REQUEST_COINS:
            self.logger.info("Received request for coins list.")
            self._publish_current_coin_list()
        elif channel == config_redis.RESYNC_CHANNEL:
            self.logger.info("Received resync signal, publishing coin list.")
            self._publish_current_coin_list()
        else:
            self.logger.warning(f"Unrecognized channel: {channel}, data={data_obj}")

    def handle_request_subscriptions(self, data):
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
            self.redis_handler.publish(config_redis.DB_REQUEST_SUBSCRIPTIONS, payload)
            self.logger.info(f"Published {len(subscriptions)} subscriptions to {config_redis.DB_REQUEST_SUBSCRIPTIONS} channel.")
        else:
            self.logger.info(f"No saved subscriptions found for market '{market_filter}' in DB.")

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
            self.heartbeat.stop()
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
                        if bot_name and timestamp:
                            if bot_name not in self.status or self.status[bot_name]['status'] == 'stopped':
                                status_obj = {
                                    "bot_name": bot_name,
                                    "status": "started",
                                    "time": timestamp,
                                    "auth_token": payload.get("auth_token", ""),
                                    "metadata": payload.get("metadata", {})
                                }
                                self.status_handler.handle_bot_status_update(status_obj)
                            self._update_bot_last_seen(bot_name, timestamp)
                            self.bot_status[bot_name] = {'last_seen': now, 'status': 'started'}
                    except Exception as e:
                        self.logger.error(f"Failed to handle heartbeat message: {e}")
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
                    self.trade_buffer.clear()
                except Exception as e:
                    self.conn.rollback()
                    self.logger.error(f" Error during bulk insert of trades: {e}")
                finally:
                    cursor.close()
            time.sleep(5)

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

    def handle_macro_metrics(self, data):
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
            f"SELECT * FROM {self.trading_schema}.websocket_subscriptions WHERE market = %s",
            (market,)
        )
        rows = cursor.fetchall()
        cursor.close()
        # Convert rows to dicts if needed
        return [dict(zip([desc[0] for desc in cursor.description], row)) for row in rows]

