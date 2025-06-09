"""
kline_fetcher.py

📈 KlineFetcher – A professional utility for fetching historical OHLCV (kline) data
from the Bybit REST API and storing it into a structured PostgreSQL database.

Author: QuantEdge Labs | Built for high-frequency and institutional-grade backtesting engines.
Date: 2025-06-08
Version: 1.1

Usage:
    from utils.kline_fetcher import KlineFetcher

    fetcher = KlineFetcher(db_handler, logger)
    fetcher.fetch_and_store(
        symbol="BTCUSDT",
        interval_minutes=1,
        start_time=datetime(2025, 6, 5, 6, 19, tzinfo=timezone.utc),
        total_candles=1440
    )

Requirements:
    - PostgreSQL with a `trading.kline_data` table
    - Bybit API access
    - Proper logging and database handler instances

"""

import requests
from datetime import datetime, timezone, timedelta


class KlineFetcher:
    """
    A class for fetching historical kline (candlestick) data from Bybit
    and storing it into a PostgreSQL database.

    Attributes:
        db (PostgresHandler): Active database handler with .conn for psycopg2.
        logger (logging.Logger): Logger instance for structured output.
        market (str): Market type to pull klines from (default: 'spot').
    """

    def __init__(self, db_handler, logger, market="spot"):
        self.db = db_handler
        self.logger = logger
        self.market = market
        self.base_url = "https://api.bybit.com/v5/market/kline"
        self.max_limit = 1000

    def fetch_and_store(self, symbol, interval_minutes, start_time, total_candles):
        """
        Fetches kline data in paginated chunks and stores into PostgreSQL.

        Args:
            symbol (str): Symbol pair, e.g. 'BTCUSDT'.
            interval_minutes (int): Interval in minutes (1, 5, or 60).
            start_time (datetime): UTC datetime to begin fetching from.
            total_candles (int): Total number of candles to fetch.
        """
        assert interval_minutes in [1, 5, 60], "Interval must be 1, 5, or 60"
        interval_map = {1: "1", 5: "5", 60: "60"}
        interval = interval_map[interval_minutes]

        insert_sql = """
        INSERT INTO trading.kline_data (
            symbol, interval, start_time, open, high, low, close,
            volume, turnover, confirmed, market
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
        ON CONFLICT (symbol, interval, start_time) DO NOTHING;
        """

        fetched_total = 0
        current_start = start_time
        cursor = self.db.conn.cursor()

        try:
            while fetched_total < total_candles:
                candles_to_fetch = min(self.max_limit, total_candles - fetched_total)
                start_ms = int(current_start.timestamp() * 1000)
                end_ms = int((current_start + timedelta(minutes=candles_to_fetch * interval_minutes)).timestamp() * 1000)

                params = {
                    "category": self.market,
                    "symbol": symbol,
                    "interval": interval,
                    "start": start_ms,
                    "end": end_ms,
                    "limit": candles_to_fetch
                }

                self.logger.info(f"📥 Fetching {candles_to_fetch} {interval}-minute klines from {current_start}...")

                try:
                    response = requests.get(self.base_url, params=params, timeout=10)
                    data = response.json()

                    if data.get("retCode") != 0 or "list" not in data.get("result", {}):
                        self.logger.error(f"❌ API Error: {data}")
                        break

                    klines = data["result"]["list"]
                    if not klines:
                        self.logger.warning("⚠️ No klines returned. Ending fetch loop.")
                        break

                    if len(klines) < candles_to_fetch:
                        self.logger.warning(f"⚠️ Fewer klines than requested ({len(klines)} < {candles_to_fetch})")

                    for candle in klines:
                        try:
                            candle_time = datetime.fromtimestamp(int(candle[0]) / 1000, tz=timezone.utc)
                            o, h, l, c, volume, turnover = map(float, candle[1:7])
                            cursor.execute(insert_sql, (
                                symbol, interval, candle_time, o, h, l, c, volume, turnover, self.market
                            ))
                        except Exception as candle_err:
                            self.logger.warning(f"⚠️ Skipping malformed candle: {candle_err}")

                    self.db.conn.commit()
                    fetched_total += len(klines)
                    current_start = datetime.fromtimestamp(int(klines[-1][0]) / 1000, tz=timezone.utc) + timedelta(minutes=interval_minutes)

                    self.logger.info(f"✅ Inserted {len(klines)} klines (cumulative: {fetched_total})")

                    if len(klines) < candles_to_fetch:
                        break  # No more data

                except requests.exceptions.Timeout:
                    self.logger.warning("⚠️ Request timed out. Retrying next chunk...")
                    continue
                except requests.RequestException as req_err:
                    self.logger.error(f"❌ Network error: {req_err}")
                    break

        except KeyboardInterrupt:
            self.logger.warning("🛑 Interrupted by user. Safely stopping kline fetch.")
        except Exception as e:
            self.logger.error(f"❌ Unexpected error during fetch loop: {e}")
        finally:
            cursor.close()
            self.logger.info(f"🏁 Fetch complete: {fetched_total} klines stored for {symbol} ({interval}-minute)")
