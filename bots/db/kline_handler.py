
import datetime
import pytz
import config.config_db as db_config
import pandas as pd

class KlineHandler:
    def __init__(self, conn, logger):
        self.conn = conn
        self.logger = logger

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
            market = kdata.get("market") or "unknown2"
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