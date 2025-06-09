from dotenv import load_dotenv
load_dotenv(override=True)
import sys, os
from datetime import datetime, timezone

# ==== CONFIGURATION ====
REPLAY_SPEED = 1000000.0  # Speed multiplier for playback
START_TIME = datetime(2025, 5, 24, 13, 19, tzinfo=timezone.utc)
WINDOW_MINUTES = 100    # Duration of replay window

# ==== PATH SETUP ====
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# ==== IMPORTS ====
from utils.logger import setup_logger
from utils.db_postgres import PostgresHandler
from utils.redis_handler import RedisHandler
import config.config_redis as config_redis
from market_data_loader import MarketDataLoader
from market_replay_engine import MarketReplayEngine
from config import config_redis

# ==== INITIALIZATION ====
RESET_KLINE_WINDOWS_ON_START = True
logger = setup_logger("market_replay.log")
db = PostgresHandler(logger)
redis = RedisHandler(config_redis, logger)
redis.connect()
#kline_fetcher = KlineFetcher(db, logger)
# ==== LOAD AND REPLAY DATA ====
loader = MarketDataLoader(db, logger)
loader.report_data_ranges()
#
if RESET_KLINE_WINDOWS_ON_START:
    pattern = "kline_window:*"
    keys = redis.client.keys(pattern)
    if keys:
        logger.warning(f"🚨 Clearing {len(keys)} old kline window keys...")
        redis.client.delete(*keys)
events = loader.get_merged_events(
    start_time=START_TIME,
    duration_minutes=WINDOW_MINUTES,
    enable_trades=False,
    enable_orderbook=False,
    enable_klines=True
)
logger.info(f"✅ Retrieved {len(events)} events for replay")

if events:
    replay = MarketReplayEngine(
        events,
        redis_handler=redis,
        logger=logger,
        speed=REPLAY_SPEED,
        config=config_redis
    )
    replay.replay()
else:
    logger.warning("⚠️ No events found for the specified time window.")








