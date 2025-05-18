import json, threading, logging
from bots.utils.redis_client import get_redis
from bots.config import config_redis as cfg
from bots.utils.logger import setup_logger

# MAX_SYMBOLS is now the single source of truth for symbol limits, used by both SubscriptionHandler and WebSocketBot.
MAX_SYMBOLS = 50  # Maximum allowed symbols per subscription

def get_log_level():
    # Use LOG_LEVEL from config if available, default to INFO
    return logging.DEBUG if getattr(cfg, "LOG_LEVEL", "INFO").upper() == "DEBUG" else logging.INFO

class SubscriptionHandler(threading.Thread):
    def __init__(self, redis_conn, out_q, subscription_channel, log_level=None):
        super().__init__(daemon=True)
        self.redis = redis_conn
        self.out_q = out_q
        self.subscription_channel = subscription_channel
        # Use config log level unless explicitly overridden
        self.logger = setup_logger("subscription_handler.log", log_level or get_log_level())
        self.running = True

    def run(self):
        self.logger.info(f"Listening on Redis list '{self.subscription_channel}' …")
        while self.running:
            _key, raw = self.redis.blpop(self.subscription_channel)
            try:
                cmd = json.loads(raw)
                cmd = self._normalize(cmd)
                # ==== Jericho: Enforce symbol cap ====
                if len(cmd.get("symbols", [])) > MAX_SYMBOLS:
                    self.logger.warning(f"❌ Subscription rejected: too many symbols ({len(cmd['symbols'])} > {MAX_SYMBOLS})")
                    continue
                # ==== Jericho: Require OWNER field ====
                if not cmd.get("owner"):
                    self.logger.warning(f"❌ Subscription rejected: missing OWNER field. RAW: {cmd}")
                    continue
                self.out_q.put(cmd)
                self.logger.debug(f"✅ Sent command to out_q: {cmd}")
            except Exception as exc:
                self.logger.error(f"Invalid command: {exc} RAW:{raw}")

    def _normalize(self, cmd: dict) -> dict:
        cmd.setdefault("action", "add")
        cmd.setdefault("market", "linear")
        if isinstance(cmd.get("symbols"), str):
            cmd["symbols"] = [cmd["symbols"]]
        if "topics" not in cmd:
            cmd["topics"] = ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"]
        # ==== Jericho: Normalize OWNER field ====
        if "owner" not in cmd:
            cmd["owner"] = None
        return cmd

    def stop(self):
        self.running = False

    def remove_orderbook_subscriptions(self):
        """
        Remove all orderbook subscriptions from the queue or memory.
        This function can be expanded to interact with the actual subscription state as needed.
        """
        # Example: If you store active subscriptions in memory, filter them here.
        # This is a placeholder for future logic.
        self.logger.info("Removing all orderbook subscriptions (functionality to be implemented as needed).")

"""
subscription_message = {
    "action": "set",
    "market": "linear",
    "symbols": ["BTCUSDT"],
    "topics": ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"]
    }
r.lpush("coin_subscription", json.dumps(subscription_message))
"""