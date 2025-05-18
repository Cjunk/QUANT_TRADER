import json, threading, logging
from bots.utils.redis_client import get_redis
from bots.config import config_redis as cfg
from bots.utils.logger import setup_logger

class SubscriptionHandler(threading.Thread):

    def __init__(self, redis_conn, out_q, subscription_channel, log_level=logging.INFO):
        super().__init__(daemon=True)
        self.redis = redis_conn
        self.out_q = out_q
        self.subscription_channel = subscription_channel
        self.logger = setup_logger("subscription_handler.log", log_level)
        self.running = True

    def run(self):
        self.logger.info(f"Listening on Redis list '{self.subscription_channel}' …")
        while self.running:
            _key, raw = self.redis.blpop(self.subscription_channel)
            try:
                cmd = json.loads(raw)
                cmd = self._normalize(cmd)
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
        return cmd

    def stop(self):
        self.running = False

"""
subscription_message = {
    "action": "set",
    "market": "linear",
    "symbols": ["BTCUSDT"],
    "topics": ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"]
    }
r.lpush("coin_subscription", json.dumps(subscription_message))
"""