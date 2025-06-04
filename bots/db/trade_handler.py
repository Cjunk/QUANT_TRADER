
import time
import pandas as pd
class TradeHandler:
    """
    Handles trade data buffering and database operations for trades.
    """
    def __init__(self, conn, logger, db_config):
        self.conn = conn
        self.logger = logger
        self.db_config = db_config
        self.trade_buffer = []

    def handle_raw_trade(self, tdata):
        """Handles and inserts raw trade data into the database."""
        try:
            cursor = self.conn.cursor()
            insert_sql = f"""
            INSERT INTO {self.db_config.DB_TRADING_SCHEMA}.raw_trade_data 
            (symbol, market, trade_time, price, volume, side, is_buyer_maker, trade_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_sql, (
                tdata["symbol"],
                tdata["market"],
                pd.to_datetime(tdata["trade_time"]),
                float(tdata["price"]),
                float(tdata["volume"]),
                tdata.get("side"),
                tdata.get("is_buyer_maker"),
                tdata.get("trade_id")
            ))
            self.conn.commit()
            cursor.close()
            #self.logger.debug(f"Inserted trade: {tdata['symbol']} {tdata['price']} {tdata['volume']}")
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            self.logger.error(f"🚨 Failed to insert raw_trade_data: {e}")

    def handle_trade_update(self, tdata):
        """Buffers summarized trade data for later bulk insert."""
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

    def flush_trade_buffer(self, running_flag):
        """Periodically flushes the trade buffer to the database in bulk."""
        while running_flag():
            if self.trade_buffer:
                try:
                    cursor = self.conn.cursor()
                    insert_sql = f"""
                    INSERT INTO {self.db_config.DB_TRADING_SCHEMA}.trade_summary_data
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
                    count = len(self.trade_buffer)
                    self.logger.info(f"Flushed {count} summarized trades to DB.")
                    self.trade_buffer.clear()
                except Exception as e:
                    if self.conn:
                        self.conn.rollback()
                    self.logger.error(f"Error during bulk insert of trades: {e}")
                finally:
                    cursor.close()
            time.sleep(5)