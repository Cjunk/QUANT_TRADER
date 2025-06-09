from datetime import datetime, timedelta
from pytz import UTC

# Control constants
DEFAULT_WINDOW_MINUTES = 5  # Default time window for event loading (in minutes)

class MarketDataLoader:
    """
    MarketDataLoader provides methods to load and merge historical market data 
    (trades and order book deltas) from a PostgreSQL database within a specified time window.

    This class supports precise time-based slicing of historical data, making it ideal for:
    - Market replay and backtesting
    - Strategy evaluation and optimization
    - Quantitative research and analysis

    Attributes:
        db: An instance of a database handler class (e.g., PostgresHandler).
        logger: A logging instance for structured logging.
    """

    def __init__(self, db, logger):
        """
        Initialize the data loader.

        Args:
            db: Database connection wrapper (must support .conn.cursor()).
            logger: Configured logger for runtime logging.
        """
        self.db = db
        self.logger = logger

    def report_data_ranges(self):
        """
        Logs the available data ranges for both trade and order book deltas.
        This is useful for verifying historical coverage before running simulations or backtests.
        """
        try:
            cursor = self.db.conn.cursor()

            # Fetch the available timestamp range for trade data
            trade_range_sql = """
                SELECT MIN(trade_time) AS start, MAX(trade_time) AS end
                FROM trading.synced_trade_data;
            """
            cursor.execute(trade_range_sql)
            trade_start, trade_end = cursor.fetchone()

            # Fetch the available timestamp range for order book deltas
            book_range_sql = """
                SELECT MIN(received_at) AS start, MAX(received_at) AS end
                FROM trading.orderbook_deltas;
            """
            cursor.execute(book_range_sql)
            book_start, book_end = cursor.fetchone()

            cursor.close()

            self.logger.info("🕒 Synced Trade Data Range: %s to %s", trade_start, trade_end)
            self.logger.info("🕒 Order Book Data Range: %s to %s", book_start, book_end)

        except Exception as e:
            self.logger.error(f"❌ Error loading data ranges: {e}")

    def get_merged_events(self, start_time=None, duration_minutes=DEFAULT_WINDOW_MINUTES,
                          enable_trades=True, enable_orderbook=True, enable_klines=True):
        """
        Retrieves and merges all market events (trades, order book updates, klines)
        within a defined time window.

        Args:
            start_time (datetime): UTC start timestamp of the data window.
            duration_minutes (int): Length of the data window in minutes.
            enable_trades (bool): Whether to load trade events.
            enable_orderbook (bool): Whether to load orderbook events.
            enable_klines (bool): Whether to load kline candle events.

        Returns:
            list: A sorted list of event dictionaries.
        """
        try:
            if not start_time:
                self.logger.error("❌ Start time must be provided.")
                return []

            end_time = start_time + timedelta(minutes=duration_minutes)
            cursor = self.db.conn.cursor()

            trade_events, book_events, candle_events = [], [], []

            if enable_trades:
                self.logger.info("📥 Loading trade data...")
                trade_sql = """
                    SELECT trade_time, symbol, price, volume, side, is_buyer_maker
                    FROM trading.synced_trade_data
                    WHERE trade_time >= %s AND trade_time <= %s
                    ORDER BY trade_time ASC;
                """
                cursor.execute(trade_sql, (start_time, end_time))
                trade_rows = cursor.fetchall()
                self.logger.info(f"✅ Loaded {len(trade_rows)} trade events")
                trade_events = [
                    {
                        "type": "trade",
                        "trade_time": row[0].replace(tzinfo=UTC).isoformat(),
                        "symbol": row[1],
                        "price": float(row[2]),
                        "volume": float(row[3]),
                        "side": row[4],
                        "is_buyer_maker": row[5]
                    }
                    for row in trade_rows
                ]

            if enable_orderbook:
                self.logger.info("📥 Loading order book deltas...")
                book_sql = """
                    SELECT received_at, symbol, side, price, volume, update_type
                    FROM trading.orderbook_deltas
                    WHERE received_at >= %s AND received_at <= %s
                    ORDER BY received_at ASC;
                """
                cursor.execute(book_sql, (start_time, end_time))
                book_rows = cursor.fetchall()
                self.logger.info(f"✅ Loaded {len(book_rows)} order book events")
                book_events = [
                    {
                        "type": "orderbook",
                        "timestamp": row[0].replace(tzinfo=UTC).isoformat(),
                        "symbol": row[1],
                        "side": row[2],
                        "price": float(row[3]),
                        "volume": float(row[4]),
                        "update_type": row[5]
                    }
                    for row in book_rows
                ]

            if enable_klines:
                candle_events = self._load_kline_candles(start_time, end_time)

            cursor.close()

            # Combine and sort
            self.logger.info("🔄 Merging and sorting all events...")
            all_events = trade_events + book_events + candle_events
            all_events.sort(key=lambda e: e.get("trade_time") or e.get("timestamp"))
            self.logger.info(f"✅ Total merged events: {len(all_events)}")

            return all_events

        except Exception as e:
            self.logger.error(f"❌ Error merging events: {e}")
            return []

    def _load_kline_candles(self, start_time, end_time):
        """
        Loads candle data for 1, 5, and 60 minute intervals from the database.
        """
        self.logger.info("📥 Loading kline (candle) data...")
        cursor = self.db.conn.cursor()
        candle_sql = """
            SELECT start_time, symbol, interval, open, high, low, close, volume, market, turnover
            FROM trading.synced_kline_data
            WHERE start_time >= %s AND start_time <= %s
            AND interval IN ('1', '5', '60')
            ORDER BY start_time ASC;
        """

        cursor.execute(candle_sql, (start_time, end_time))
        rows = cursor.fetchall()
        self.logger.info(f"✅ Loaded {len(rows)} candle records")
        cursor.close()

        candle_events = [
            {
                "type": "kline",
                "timestamp": row[0].replace(tzinfo=UTC).isoformat(),
                "symbol": row[1],
                "interval": f"{row[2]}",
                "open": float(row[3]),
                "high": float(row[4]),
                "low": float(row[5]),
                "close": float(row[6]),
                "volume": float(row[7]),
                "market": row[8],
                "turnover": float(row[9])
            }
            for row in rows
        ]

        return candle_events




