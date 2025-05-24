import unittest
import sys, os, time, redis, psycopg2, json, datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from bots.db.db_core import PostgresDBBot
import config.config_db as db_config
import config.config_common as config_common
import config.config_redis as config_redis

class TestPostgresDBBot(unittest.TestCase):

    def setUp(self):
        self.bot = PostgresDBBot()
        self.redis_client = redis.StrictRedis(
            host=config_redis.REDIS_HOST,
            port=config_redis.REDIS_PORT,
            db=config_redis.REDIS_DB,
            decode_responses=True
        )

    def tearDown(self):
        self.bot.stop()
        self.bot.close()

    # Your existing tests:
    def test_postgres_connection(self):
        """Test PostgreSQL connection."""
        self.assertIsNotNone(self.bot.conn)
        self.assertFalse(self.bot.conn.closed)
        self.bot.close()
        self.assertTrue(self.bot.conn.closed)

    def test_heartbeat_sent_to_redis(self):
        """Test heartbeat is sent to Redis."""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(config_common.HEARTBEAT_CHANNEL)
        time.sleep(config_common.HEARTBEAT_INTERVAL_SECONDS + 2)

        message = None
        for _ in range(10):
            msg = pubsub.get_message(timeout=1)
            if msg and msg['type'] == 'message':
                data = json.loads(msg['data'])
                if data.get('bot_name') == db_config.BOT_NAME:
                    message = data
                    break
            time.sleep(0.5)

        pubsub.close()
        self.assertIsNotNone(message)
        self.assertEqual(message.get('bot_name'), db_config.BOT_NAME)

    def test_status_update_in_db(self):
        """Test status update in PostgreSQL."""
        time.sleep(config_common.HEARTBEAT_INTERVAL_SECONDS + 2)

        conn = psycopg2.connect(
            host=db_config.DB_HOST,
            port=db_config.DB_PORT,
            dbname=db_config.DB_DATABASE,
            user=db_config.DB_USER,
            password=db_config.DB_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(f"SELECT status, last_updated FROM {db_config.DB_TRADING_SCHEMA}.bots WHERE bot_name = %s", (db_config.BOT_NAME,))
        row = cur.fetchone()
        cur.close()
        conn.close()

        self.assertIsNotNone(row)
        status, last_updated = row
        self.assertEqual(status, 'started')
        self.assertIsNotNone(last_updated)

    # ✅ New Test: Redis Subscription Check
    def test_redis_channel_subscriptions(self):
        """Test correct Redis channel subscriptions."""
        expected_channels = {
            config_redis.COIN_CHANNEL,
            config_redis.PRE_PROC_KLINE_UPDATES,
            config_redis.PRE_PROC_TRADE_CHANNEL,
            config_redis.PRE_PROC_ORDER_BOOK_UPDATES,
            config_redis.RESYNC_CHANNEL,
            config_redis.SERVICE_STATUS_CHANNEL,
            config_redis.MACRO_METRICS_CHANNEL,
            config_redis.REQUEST_COINS
        }
        actual_channels = set(self.bot.pubsub.channels.keys())
        self.assertTrue(expected_channels.issubset(actual_channels))

    # ✅ New Test: Insert and Verify Kline Data
    def test_handle_kline_update(self):
        """Test inserting a kline into PostgreSQL."""
        test_kline = {
            "symbol": "BTCUSDT",
            "interval": "1",
            "start_time": "2025-05-22T10:00:00Z",
            "open": "50000", "close": "51000", "high": "51500", "low": "49500",
            "volume": "10", "turnover": "500000", "confirmed": True, "market": "linear"
        }
        # Clean up any existing test row first
        cursor = self.bot.conn.cursor()
        cursor.execute(f"DELETE FROM {db_config.DB_TRADING_SCHEMA}.kline_data WHERE symbol=%s AND interval=%s AND start_time=%s", ("BTCUSDT", "1", "2025-05-22T10:00:00Z"))
        self.bot.conn.commit()
        cursor.close()
        self.bot.handle_kline_update(test_kline)
        cursor = self.bot.conn.cursor()
        cursor.execute(f"""
            SELECT open, close FROM {db_config.DB_TRADING_SCHEMA}.kline_data 
            WHERE symbol=%s AND interval=%s AND start_time=%s
        """, ("BTCUSDT", "1", "2025-05-22T10:00:00Z"))
        row = cursor.fetchone()
        print(f"Fetched kline row: {row}")
        cursor.close()

        self.assertIsNotNone(row)
        self.assertEqual(float(row[0]), 50000)
        self.assertEqual(float(row[1]), 51000)

    # ✅ New Test: Handle Invalid JSON from Redis
    def test_handle_invalid_json_from_redis(self):
        """Ensure invalid JSON from Redis is handled gracefully."""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(config_redis.PRE_PROC_KLINE_UPDATES)
        time.sleep(1)
        self.redis_client.publish(config_redis.PRE_PROC_KLINE_UPDATES, "not-json")

        # Allow processing
        time.sleep(2)

        # Check bot still running after invalid data
        self.assertTrue(self.bot.running)
        pubsub.close()

    # ✅ New Test: Test coin list update functionality
    def test_handle_coin_list_update(self):
        """Test updating coin list in PostgreSQL."""
        test_coin_data = {'symbols': ['TESTCOIN1', 'TESTCOIN2']}
        self.bot.handle_coin_list_update(test_coin_data)

        cursor = self.bot.conn.cursor()
        cursor.execute(f"SELECT symbol FROM {db_config.DB_TRADING_SCHEMA}.current_coins")
        rows = cursor.fetchall()
        cursor.close()

        symbols_in_db = {row[0] for row in rows}
        self.assertSetEqual(symbols_in_db, {'TESTCOIN1', 'TESTCOIN2'})

    # ✅ New Test: Ensure trade summaries flush to DB
    def test_trade_summary_flush(self):
        """Test that summarized trades flush to DB."""
        trade_update = {
            "symbol": "BTCUSDT",
            "minute_start": datetime.datetime.utcnow().replace(second=0, microsecond=0).isoformat(),
            "total_volume": 100, "vwap": 50100,
            "trade_count": 5, "largest_trade_volume": 25, "largest_trade_price": 50200
        }
        self.bot.handle_trade_update(trade_update)
        time.sleep(6)  # Allow flush (flushes every 5 seconds)

        cursor = self.bot.conn.cursor()
        cursor.execute(f"""
            SELECT total_volume, vwap FROM {db_config.DB_TRADING_SCHEMA}.trade_summary_data 
            WHERE symbol=%s ORDER BY minute_start DESC LIMIT 1
        """, ("BTCUSDT",))
        row = cursor.fetchone()
        cursor.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], 100)
        self.assertEqual(row[1], 50100)

if __name__ == "__main__":
    unittest.main()
