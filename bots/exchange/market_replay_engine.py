import time
import json
from dateutil.parser import isoparse

class MarketReplayEngine:
    """
    Replays historical market data into Redis with real-time timing simulation.
    """

    def __init__(self, events, redis_handler, logger, speed=1.0, config=None):
        """
        Args:
            events (list): Event dicts sorted chronologically.
            redis_handler (RedisHandler): Redis client for publishing.
            logger (Logger): Logger instance.
            speed (float): Replay speed multiplier.
            config (module): Redis config module (e.g., config.config_redis).
        """
        self.events = events
        self.redis = redis_handler
        self.logger = logger
        self.speed = speed
        self.config = config

    def replay(self):
        """
        Replays all market events in time-synchronized order to Redis.
        """
        if not self.events:
            self.logger.warning("⚠️ No events to replay.")
            return

        # --- Startup Info ---
        channel_keys = [
            f"{market}.{etype}_out"
            for market in ["spot", "linear", "derivatives"]
            for etype in ["kline", "trade", "orderbook"]
        ]
        channels = [
            self.config.REDIS_CHANNEL.get(key)
            for key in channel_keys
            if self.config.REDIS_CHANNEL.get(key) is not None
        ]
        self.logger.info("========== MarketReplayEngine Startup ==========")
        self.logger.info("This is the MarketReplayEngine.")
        self.logger.info(f"Will publish to Redis channels: {channels}")
        self.logger.info("===============================================")

        self.logger.info(f"🚀 Starting market replay at {self.speed}x speed...")

        prev_ts = isoparse(self.events[0].get("trade_time") or self.events[0].get("timestamp"))

        for event in self.events:

            current_ts = isoparse(event.get("trade_time") or event.get("timestamp"))
            time_diff = (current_ts - prev_ts).total_seconds() / self.speed
            if time_diff > 0:
                time.sleep(time_diff)

            market = event.get("market", "spot").lower()
            event_type = event["type"]

            # Determine channel using centralized config
            try:
                if event_type == "trade":
                    channel = self.config.REDIS_CHANNEL[f"{market}.trade_out"]
                elif event_type == "orderbook":
                    channel = self.config.REDIS_CHANNEL[f"{market}.orderbook_out"]
                elif event_type == "kline":
                    
                    event["start_time"] = event.pop("timestamp", None)
                    channel = self.config.REDIS_CHANNEL.get(f"{market}.kline_out", self.config.KLINE_UPDATES)
                else:
                    self.logger.warning(f"⚠️ Unknown event type: {event_type}")
                    continue

                message = json.dumps(event)
                self.logger.debug(f"📤 Publishing → {channel} | {message}")
                self.redis.publish(channel, message)

            except KeyError as err:
                self.logger.warning(f"⚠️ No channel found for event type '{event_type}' in market '{market}': {err}")
            except Exception as e:
                self.logger.error(f"❌ Redis publish error: {e}")

            prev_ts = current_ts

        self.logger.info("✅ Market replay complete.")

