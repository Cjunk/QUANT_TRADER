# radar_runner.py

"""
Launches TradeRadar as a standalone service.
Feeds it live trade data from Redis pub/sub and publishes stats to Redis.
"""

# ========== CONFIGURATION ========== 
DEBUG = False  # Set to False to disable debug logging

# ========== ENV SETUP ========== 
from dotenv import load_dotenv
load_dotenv(override=True)

import sys, os, time, json
from datetime import datetime
import threading

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from traderadar import TradeRadar
from utils.redis_handler import RedisHandler
from utils.logger import setup_logger
from utils.HeartBeatService import HeartBeat
import config.config_redis as config_redis

# Configuration
TRADE_WINDOW_DURATION = 900
IDLE_TIMEOUT = 300
INTERVALS = [60, 300, 600]
PUBLISH_INTERVAL = 2  # seconds
TARGET_SYMBOL = "BTCUSDT"
TARGET_MARKET = "spot"

# Setup Radar
logger = setup_logger("TradeRadarService.log")
if DEBUG:
    logger.setLevel("DEBUG")
else:
    logger.setLevel("INFO")

redis = RedisHandler(config_redis, logger, service_name="TradeRadar", debug=DEBUG)
redis.connect()
if DEBUG:
    logger.debug("DEBUG mode is ON")
logger.info("✅ Redis connection established")

radar = TradeRadar(
    max_duration_secs=TRADE_WINDOW_DURATION,
    max_idle_secs=IDLE_TIMEOUT,
    redis_handler=redis,
    intervals_secs=INTERVALS
)

# Setup HeartBeat
heartbeat = HeartBeat(
    bot_name="TradeRadar",
    auth_token="your_auth_token",  # If required, else remove
    logger=logger,
    redis_handler=redis,
    metadata={"service": "TradeRadar"},
)
logger.info("✅ HeartBeat service started")


# Subscribe to trade feed
channels_to_sub = [
    config_redis.REDIS_CHANNEL["spot.trade_out"],
    config_redis.REDIS_CHANNEL["linear.trade_out"]
]
pubsub = redis.client.pubsub()
pubsub.subscribe(*channels_to_sub)
logger.info(f"✅ Subscribed to channels: {channels_to_sub}")

if __name__ == "__main__":
    logger.info("🚀 TradeRadar Service Running...")
    last_publish_time = time.time()
    try:
        while True:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            if message and message['type'] == 'message':
                try:
                    channel = message['channel']
                    market = channel.split("_")[0]

                    data = json.loads(message['data'])
                    symbol = data.get("symbol")
                    price = data.get("price")
                    volume = data.get("volume")
                    side = data.get("side")
                    timestamp_str = data.get("timestamp")
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")) if timestamp_str else None

                    if DEBUG:
                        logger.debug(f"[radar_runner] Received trade: {symbol} {market} {side} {volume}@{price} at {timestamp}")

                    radar.add_trade(symbol, market, price, volume, side, timestamp)

                    if symbol == TARGET_SYMBOL and market == TARGET_MARKET:
                        now = time.time()
                        if now - last_publish_time >= PUBLISH_INTERVAL:
                            if DEBUG:
                                logger.debug(f"[radar_runner] Publishing stats for {symbol}-{market}")
                            radar.publish_stats(symbol, market, include_trend_bias=True)
                            last_publish_time = now

                except Exception as e:
                    logger.warning(f"⚠️ Failed to process trade message: {e}")

    except KeyboardInterrupt:
        logger.info("[radar_runner] 🛑 TradeRadar Service stopped manually.")