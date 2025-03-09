"""
Institutional-Style Database Bot for Quant Trading
Now with Redis Pub/Sub to receive data from your WebSocket bot.
"""

import json
import time
import threading
import datetime
import redis
import psycopg2
import pandas as pd

import psycopg2.extras
from utils.logger import setup_logger
import config.config_db as db_config
import config.config_ws as ws_config  # If you store Redis host/port here or wherever

class PostgresDBBot:
    """Maintains all DB operations for Kline, Order Book, and Trades."""

    def __init__(self, log_filename="DB_BOT.log"):
        """Initialize DB connection, create tables if needed, set up logger."""
        # Logger
        self.logger = setup_logger(log_filename)
        self.logger.info("Initializing PostgresDBBot...")
        self.host = db_config.DB_HOST
        self.port = db_config.DB_PORT
        self.database = db_config.DB_DATABASE
        self.user = db_config.DB_USER
        # Connect to Postgres
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=db_config.DB_PASSWORD
        )
        print("Database Bot (Postgresql) is running .......")
        # Use DictCursor or NamedTupleCursor if you prefer
        self.conn.autocommit = False  # Or True, if you want auto-commit
        self.logger.info(f"Connected to Postgres @ {self.host}:{self.port}/{self.database}")

        # Create tables if they do not exist
        self._create_tables()
        # Now alter the kline_data table to include indicator columns
        self._alter_table_for_indicators()
        # Redis Pub/Sub
        self.redis_client = redis.Redis(
            host=ws_config.REDIS_HOST,
            port=ws_config.REDIS_PORT,
            db=ws_config.REDIS_DB,
            decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        # Subscribe to channels (adjust names as needed)
        self.pubsub.subscribe("kline_updates", "trade_updates", "orderbook_updates")

        # Control flag for run loop
        self.running = True
    def _alter_table_for_indicators(self):
        """Alter kline_data table to add columns for technical indicators if they do not exist."""
        alter_sqls = [
            "ALTER TABLE public.kline_data ADD COLUMN IF NOT EXISTS rsi NUMERIC(8,4);",
            "ALTER TABLE public.kline_data ADD COLUMN IF NOT EXISTS macd NUMERIC(18,8);",
            "ALTER TABLE public.kline_data ADD COLUMN IF NOT EXISTS macd_signal NUMERIC(18,8);",
            "ALTER TABLE public.kline_data ADD COLUMN IF NOT EXISTS macd_hist NUMERIC(18,8);",
            "ALTER TABLE public.kline_data ADD COLUMN IF NOT EXISTS ma NUMERIC(18,8);",
            "ALTER TABLE public.kline_data ADD COLUMN IF NOT EXISTS upper_band NUMERIC(18,8);",
            "ALTER TABLE public.kline_data ADD COLUMN IF NOT EXISTS lower_band NUMERIC(18,8);"
        ]
        cursor = self.conn.cursor()
        try:
            for sql in alter_sqls:
                cursor.execute(sql)
            self.conn.commit()
            self.logger.info("Table 'kline_data' altered successfully for technical indicators.")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error altering table for indicators: {e}")
        finally:
            cursor.close()

    def _create_tables(self):
        """
        Create tables for kline, order book, trades, etc.
        If you want partitions or advanced schemas, adjust accordingly.
        """
        create_kline_table = """
        CREATE TABLE IF NOT EXISTS kline_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            interval VARCHAR(10) NOT NULL,
            start_time TIMESTAMP NOT NULL,
            open NUMERIC(18,8),
            close NUMERIC(18,8),
            high NUMERIC(18,8),
            low NUMERIC(18,8),
            volume NUMERIC(28,10),
            turnover NUMERIC(28,10),
            confirmed BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (symbol, interval, start_time)
        );
        """

        create_orderbook_table = """
        CREATE TABLE IF NOT EXISTS orderbook_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            price NUMERIC(18,8) NOT NULL,
            volume NUMERIC(28,10) NOT NULL,
            side VARCHAR(4) NOT NULL, -- "bid" or "ask"
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        create_trades_table = """
        CREATE TABLE IF NOT EXISTS trade_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            trade_time TIMESTAMP NOT NULL,
            price NUMERIC(18,8),
            volume NUMERIC(28,10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        cursor = self.conn.cursor()
        try:
            cursor.execute(create_kline_table)
            cursor.execute(create_orderbook_table)
            cursor.execute(create_trades_table)
            self.conn.commit()
            self.logger.info("Tables verified/created successfully.")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error creating tables: {e}")
        finally:
            cursor.close()

    def store_kline_data(self, symbol, interval, start_time, open_price, close_price,
                         high_price, low_price, volume, turnover, confirmed):
        """
        Insert or update a kline row. 
        Typically you insert a new row each time a candle closes or to track partial updates.
        """
        insert_sql = """
        INSERT INTO kline_data 
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
            self.logger.info(f"Kline data inserted for {symbol}, interval={interval}, time={start_time}")
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
        insert_sql = """
        INSERT INTO orderbook_data (symbol, price, volume, side)
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
        insert_sql = """
        INSERT INTO trade_data (symbol, trade_time, price, volume)
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
    def run(self):
        """Run loop listening to Redis Pub/Sub channels."""
        self.logger.info("DB Bot running, listening to Redis Pub/Sub...")
        while self.running:
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

                if channel == "kline_updates":
                    self.handle_kline_update(data_obj)
                elif channel == "trade_updates":
                    self.handle_trade_update(data_obj)
                elif channel == "orderbook_updates":
                    self.handle_orderbook_update(data_obj)
                else:
                    self.logger.warning(f"Unrecognized channel: {channel}, data={data_obj}")

            time.sleep(0.1)
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
        Expects a JSON object like:
        {
          "symbol": "BTCUSDT",
          "time": 1677600123,    # epoch seconds
          "price": 23456.78,
          "volume": 0.1234
        }
        """
        try:
            symbol = tdata["symbol"]
            trade_time = tdata["time"]
            price = float(tdata["price"])
            volume = float(tdata["volume"])

            import datetime
            trade_dt = datetime.datetime.utcfromtimestamp(trade_time)

            self.store_trade_data(symbol, trade_dt, price, volume)
        except KeyError as e:
            self.logger.error(f"Missing key in trade update: {e}")
    def update_indicators_for_symbol(self, symbol, interval):
        """
        Query all kline_data rows for a given symbol and interval, compute technical
        indicators (RSI, MACD, MA, Bollinger Bands), and update the table rows.
        """
        query = """
        SELECT id, start_time, open, close, high, low, volume, turnover
        FROM kline_data
        WHERE symbol = %s AND interval = %s
        ORDER BY start_time ASC;
        """
        try:
            df = pd.read_sql(query, self.conn, params=(symbol, interval))
        except Exception as e:
            self.logger.error(f"Error reading kline data for {symbol} {interval}: {e}")
            return

        if df.empty:
            self.logger.info(f"No kline data to compute indicators for {symbol} {interval}")
            return

        # Compute technical indicators on the DataFrame (assuming compute_indicators is defined)
        df = self.compute_indicators(df)

        # Now update each row with the computed values
        update_sql = """
        UPDATE kline_data
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
            self.logger.info(f"Indicators updated for {symbol} {interval}")
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Error updating indicators for {symbol} {interval}: {e}")
        finally:
            cursor.close()
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
    def compute_wilder_rsi_loop(self,prices, period=14):
        """
        Compute the Relative Strength Index (RSI) using Wilder's smoothing method.
        Returns a list of RSI values.
        """
        if len(prices) < period + 1:
            return [None] * len(prices)
        
        rsi = [None] * len(prices)
        gains = []
        losses = []
        
        # Initial averages
        for i in range(1, period + 1):
            change = prices[i] - prices[i - 1]
            gains.append(max(change, 0))
            losses.append(abs(min(change, 0)))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
        rsi[period] = 100 - (100 / (1 + rs))
        
        # Apply Wilder's smoothing
        for i in range(period + 1, len(prices)):
            change = prices[i] - prices[i - 1]
            gain = max(change, 0)
            loss = abs(min(change, 0))
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
            rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
            rsi[i] = 100 - (100 / (1 + rs))
        
        return rsi

    def compute_indicators(self,df):
        """
        Given a DataFrame with a 'close' column, compute technical indicators:
        - RSI (using a 14-period Wilder smoothing)
        - MACD, MACD Signal, and MACD Histogram (using spans of 12, 26, and 9)
        - A 20-period Moving Average (MA)
        - Bollinger Bands (Upper and Lower) calculated as MA ± 2 * STD
        Returns the DataFrame with new columns added.
        """
        # Compute RSI. Convert the close column to a list.
        df['RSI'] = self.compute_wilder_rsi_loop(df['close'].tolist(), period=14)
        
        # Compute MACD and its signal line
        df['EMA12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['EMA26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = df['EMA12'] - df['EMA26']
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
        
        # Compute Moving Average and Bollinger Bands (20-period window)
        df['MA'] = df['close'].rolling(window=20).mean()
        df['STD'] = df['close'].rolling(window=20).std()
        df['UpperBand'] = df['MA'] + 2 * df['STD']
        df['LowerBand'] = df['MA'] - 2 * df['STD']
        
        # Optionally, drop the intermediate columns if you don't need them:
        df.drop(columns=['EMA12', 'EMA26', 'STD'], inplace=True)
        
        return df

if __name__ == "__main__":
    # Example usage
    db_bot = PostgresDBBot("DB_BOT.log")
    try:
        db_bot.run()
    except KeyboardInterrupt:
        db_bot.logger.info("Keyboard interrupt - stopping DB Bot.")
    finally:
        db_bot.stop()
    db_bot.close()
