"""
TradarRadarBot (Main Entrypoint)

This script acts as the execution engine for feeding live or replayed trade data into the TradeRadar model. 
It listens to Redis pub/sub channels for trade messages (spot/futures), processes those trades into rolling 
windows using the TradeRadar engine, and outputs real-time trend bias assessments (e.g., Bullish, Bearish, Divergence).

Usage:
- Run this script as your trading bot's market data "decision module".
- Configure which Redis trade feeds it listens to (spot, linear).
- Customize the statistical time windows via TradeRadar initialization.
- Use the trend bias it computes to guide trading logic in your strategy.

Requirements:
- Environment `.env` file with Redis connection details.
- Redis server running and receiving trade data on specified channels.
- Compatible with both real-time and replayed historical trade streams.
"""

# ========== CONFIGURATION ==========
DEBUG = True
WINDOW_SECONDS = 300             
PUBLISH_INTERVAL = 5        # Frequency in seconds to publish stats
TRADE_WINDOW_DURATION = 900        # Duration of each trade window in seconds
IDLE_TIMEOUT = 300                          # Timeout for idle trade windows

# ========== ENV SETUP ==========
from dotenv import load_dotenv
load_dotenv(override=True)

import sys, os, json
from datetime import datetime, timezone

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# ========== IMPORTS ==========
from utils.logger import setup_logger
from utils.redis_handler import RedisHandler
import config.config_redis as config_redis
from traderadar import TradeRadar
from trade_descision_engine import TradeDecisionEngine
from trade_descision_engine import TradeSignal

# ========== LOGGER & REDIS ==========
logger = setup_logger("TradarRadarBot.log")
redis = RedisHandler(config_redis, logger)
redis.connect()
logger.info("✅ Redis connection established")

# ========== INIT RADAR ENGINE ==========
radar = TradeRadar(
    max_duration_secs=TRADE_WINDOW_DURATION,
    max_idle_secs=IDLE_TIMEOUT,
    redis_handler=redis,
    intervals_secs=[WINDOW_SECONDS]
)
decision_engine = TradeDecisionEngine(3,1.3)
# ========== REDIS CHANNELS ==========
channels_to_sub = [
    config_redis.REDIS_CHANNEL["spot.trade_out"],
    config_redis.REDIS_CHANNEL["linear.trade_out"]
]
pubsub = redis.client.pubsub()
pubsub.subscribe(*channels_to_sub)
logger.info(f"✅ Subscribed to channels: {channels_to_sub}")

# ========== BOT MAIN LOOP ==========
try:
    logger.info("🔊 Listening to Redis channels. Press Ctrl+C to exit.")
    last_print_time = datetime.now()
    while True:
        message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
        if message and message['type'] == 'message':
            try:
                channel = message['channel'].decode() if isinstance(message['channel'], bytes) else str(message['channel'])
                market = channel.split("_")[0]

                data = json.loads(message['data'])
                symbol = data.get("symbol")
                price = data.get("price")
                volume = data.get("volume")
                side = data.get("side")
                timestamp_str = data.get("timestamp")
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")) if timestamp_str else None
                radar.add_trade(symbol, market, price, volume, side, timestamp)
                # Periodically fetch and print radar stats
                now = datetime.now()
                if (now - last_print_time).total_seconds() >= PUBLISH_INTERVAL:
                    radar_data = radar.get_stats(symbol, market, include_trend_bias=True)

                    stats = radar_data.get(f"{WINDOW_SECONDS}s", {}) if radar_data else {}
                    bias = stats.get("Bias", "❓ Bias not available yet")

                    if DEBUG:
                        print(f"""
                [{now.isoformat()}] 📊 [{symbol}] [{market.upper()}] {WINDOW_SECONDS}s Snapshot
                ├─ VWAP:        {stats.get("VWAP", 0):,.2f}
                ├─ CVD:         {stats.get("CVD", 0):,.2f}
                ├─ Buy Volume:  {stats.get("Buy Volume", 0):.2f}
                ├─ Sell Volume: {stats.get("Sell Volume", 0):.2f}
                └─ Trend Bias:  {bias}
                """)
                    last_print_time = now
                    # --- New Decision Logic ---
                    vwap = stats.get("VWAP", 0)
                    cvd = stats.get("CVD", 0)
                    buy = stats.get("Buy Volume", 0)
                    sell = stats.get("Sell Volume", 0)
                    signal = decision_engine.process_signal(stats)

                    if signal != TradeSignal.HOLD:
                        print(f"🚀 TRADE SIGNAL for {symbol} [{market.upper()}]: {signal.name}")

            except Exception as e:
                logger.warning(f"⚠️ Failed to process message: {e}")

except KeyboardInterrupt:
    logger.info("🛑 Stopped listening to Redis channels.")
