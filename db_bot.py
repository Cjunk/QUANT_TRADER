"""
Institutional-Style Database Bot for Quant Trading
Now with Redis Pub/Sub to receive data from your WebSocket bot.
This bot is the Postgresql bot. Most functions to do with updating postgresql will be handled by this bot

✅  TODO LIST
    1.  Subscribe to redis kline channels. And Store the kline data in the kline_data table in Postgresql ✅
    2.  Subs to redis Trade channel. Store filtered Trade data in postgresql. ✅
    3.  Periodically aggregate Order book data and store.
    4.  Sub to new coin channel. Refresh the current master coin list (Perhaps keep a trailing pre list too) ✅
    5.  Retrieve Large Trade Thresholds from Postgresql
    6.  Categorise functions
    7.  Externalise variables into config files
    8.  clean INIT
    9.  Optimise
"""

import json
import time
import threading
import datetime
import redis
import os
import psycopg2
import hashlib
import pandas as pd
import psycopg2.extras
import config.config_db as db_config
import config.config_redis as config_redis
import config.config_ws as ws_config  # If you store Redis host/port here or wherever
from sqlalchemy import create_engine
from utils.logger import setup_logger
from utils.global_indicators import GlobalIndicators # central indicators script so all bots and services using the same ones

class PostgresDBBot:
    def __init__(self, log_filename="DB_BOT.log"):
        # Logger
        self.logger = setup_logger(log_filename)
        self.logger.info("Initializing PostgresDBBot...")                
        self.GlobalIndicators = GlobalIndicators()
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
        self._setup_redis()
        # Control flag for run loop
        self.running = True
        self.large_trade_thresholds = { #TODO Transfer to database
            "BTCUSDT": 5,    # ✅ BTC large trades are usually 5+ BTC
            "ETHUSDT": 50,   # ✅ ETH large trades start at ~50 ETH
            "SOLUSDT": 500,  # ✅ SOL is volatile, large trades are ~500 SOL
            "ADAUSDT": 5000, # ✅ ADA needs ~5000 ADA to be significant
            "DOTUSDT": 2000, # ✅ DOT large trades are 2000 DOT+
            "DOGEUSDT": 100000, # ✅ DOGE is high supply, needs 100k+ DOGE
            "XRPUSDT": 50000, # ✅ XRP whale trades start at 50k+
            "BNBUSDT": 100,  # ✅ BNB large trades ~100 BNB
            "LTCUSDT": 500,  # ✅ LTC large trades ~500 LTC
            "LINKUSDT": 2000, # ✅ LINK large trades ~2000 LINK
            "TRXUSDT": 100000, # ✅ TRX needs 100k+ to be significant
            "XLMUSDT": 50000,  # ✅ XLM whale trades ~50k XLM
            "ATOMUSDT": 1000,  # ✅ ATOM large trades ~1000 ATOM
            "ALGOUSDT": 100000, # ✅ ALGO needs 100k+
            "PEPEUSDT": 1000000000, # ✅ Meme coins are extreme
            "AVAXUSDT": 1000,  # ✅ AVAX large trades ~1000 AVAX
            "UNIUSDT": 1000,   # ✅ UNI whale trades ~1000 UNI
            "SUSDT": 5000,     # ✅ Assuming this is a lower cap coin
            "NEARUSDT": 5000,  # ✅ NEAR large trades ~5000 NEAR
            "ICPUSDT": 3000,   # ✅ ICP large trades ~3000 ICP
            "ONDOUSDT": 250,
        }
      
    """
    =-=-=-=-=-=-=- Internal Bot operational functions
    """  
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
            config_redis.KLINE_UPDATES,
            config_redis.TRADE_CHANNEL,
            config_redis.ORDER_BOOK_UPDATES,
            config_redis.RESYNC_CHANNEL,
            config_redis.SERVICE_STATUS_CHANNEL
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

                    if channel == config_redis.KLINE_UPDATES:
                        self.handle_kline_update(data_obj)
                    elif channel == config_redis.TRADE_CHANNEL:
                        self.handle_trade_update(data_obj)
                    elif channel == config_redis.ORDER_BOOK_UPDATES:
                        self.handle_orderbook_update(data_obj)
                    elif channel == config_redis.COIN_CHANNEL:
                        self.handle_coin_list_update(data_obj)
                    elif channel == config_redis.SERVICE_STATUS_CHANNEL:
                        self.handle_bot_status_update(data_obj)
                    else:
                        self.logger.warning(f"Unrecognized channel: {channel}, data={data_obj}")
            except redis.ConnectionError as e:
                self.logger.error(f"Redis connection lost: {e}. Reconnecting...")
                time.sleep(2)  # Wait before retrying
                self.pubsub = self.redis_client.pubsub()  # Reinitialize Redis Pub/Sub
                self.pubsub.subscribe(config_redis.KLINE_UPDATES, config_redis.TRADE_CHANNEL, config_redis.ORDER_BOOK_UPDATES)

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
        self.pubsub.close()
        if self.conn:
            self.conn.close()
        self.logger.info("DB Bot Stopped.")   
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
            self.logger.debug(f"Order book row inserted {symbol}: {price} / {volume} / {side}")
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
    """
    =-=-=-=-=-=-=- Data handlers
    """    
    def handle_kline_update(self, kdata):
        """
        Expects a JSON object with keys like:
        {
          "symbol": "BTCUSDT",
          "interval": "1",
          "start_time": 1677600000,  # epoch seconds
          "open": 24000.12,
          "close": 24001.21,
          "high": 24100.99,
          "low": 23950.00,
          "volume": 123.4567,
          "turnover": 456789.12,
          "confirmed": true
        }
        """
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
            confirmed = bool(kdata["confirmed"])

            # Convert epoch to python datetime
            
            #start_dt = datetime.datetime.fromisoformat(kdata["start_time"])

            # Store in DB
            self.store_kline_data(symbol, interval, start_dt, open_price, close_price,
                                  high_price, low_price, volume, turnover, confirmed)
            # Compute and update technical indicators for this symbol and interval.
            self.update_indicators_for_symbol(symbol, interval)                                  
        except KeyError as e:
            self.logger.error(f"Missing key in kline update: {e}")    
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
    def update_indicators_for_symbol(self, symbol, interval):
        """
        Query all kline_data rows for a given symbol and interval, compute technical
        indicators (RSI, MACD, MA, Bollinger Bands), and update the table rows.
        """
        query = """
        SELECT id, start_time, open, close, high, low, volume, turnover
        FROM trading.kline_data
        WHERE symbol = %s AND interval = %s
        ORDER BY start_time ASC;
        """
        try:
            df = pd.read_sql(query, self.db_engine, params=(symbol, interval))
        except Exception as e:
            self.logger.error(f"Error reading kline data for {symbol} {interval}: {e}")
            return

        if df.empty:
            self.logger.info(f"No kline data to compute indicators for {symbol} {interval}")
            return

        # Compute technical indicators on the DataFrame (assuming compute_indicators is defined)
        df = self.GlobalIndicators.compute_indicators(df)

        # Now update each row with the computed values
        update_sql = f"""
        UPDATE {db_config.DB_TRADING_SCHEMA}.kline_data
        SET rsi = %s,
            macd = %s,
            macd_signal = %s,
            macd_hist = %s,
            ma = %s,
            upper_band = %s,
            lower_band = %s
        WHERE id = %s;
        """
        cursor = self.conn.cursor()
        try:
            for index, row in df.iterrows():
                cursor.execute(update_sql, (
                    row.get("RSI"),
                    row.get("MACD"),
                    row.get("MACD_Signal"),
                    row.get("MACD_Hist"),
                    row.get("MA"),
                    row.get("UpperBand"),
                    row.get("LowerBand"),
                    row["id"]
                ))
            self.conn.commit()
            #self.logger.info(f"Indicators updated for {symbol} {interval}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error updating indicators for {symbol} {interval}: {e}")
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
