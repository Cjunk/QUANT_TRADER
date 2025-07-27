import json, threading, logging, requests
from utils.redis_client import get_redis
from config import config_redis as r_cfg
import config_websocket_bot as cfg
from utils.logger import setup_logger

# === MASTER DEBUG SWITCH ===
DEBUG_MODE = True  # Set to False for INFO level and less verbose logs

MAX_SYMBOLS = 50  # Maximum allowed symbols per subscription

def get_log_level():
    return logging.DEBUG if DEBUG_MODE else logging.INFO

class SubscriptionHandler(threading.Thread):
    def __init__(self, market, logger):
        super().__init__(daemon=True)
        self.market = market
        self.logger = logger
        self.subscription_channel = {
            "spot": r_cfg.SPOT_SUBSCRIPTION_CHANNEL,
            "linear": r_cfg.LINEAR_SUBSCRIPTION_CHANNEL,
            "derivatives": r_cfg.DERIVATIVES_SUBSCRIPTION_CHANNEL,
        }.get(self.market, r_cfg.SPOT_SUBSCRIPTION_CHANNEL)
        self.redis = get_redis()
        self.out_q = None  # Initialize out_q, set it properly in the actual implementation
        self.logger = setup_logger("subscription_handler.log", get_log_level())
        self.running = True

    def run(self):
        self.logger.info(f"Listening on Redis list '{self.subscription_channel}' …")
        while self.running:
            if DEBUG_MODE:
                self.logger.debug("Waiting for subscription command from Redis...")
            _key, raw = self.redis.blpop(self.subscription_channel)
            if DEBUG_MODE:
                self.logger.debug(f"Received raw from Redis: {_key=} {raw=}")
            try:
                cmd = json.loads(raw)
                if DEBUG_MODE:
                    self.logger.debug(f"Decoded JSON command: {cmd}")
                cmd = self._normalize(cmd)
                if DEBUG_MODE:
                    self.logger.debug(f"Normalized command: {cmd}")
                if len(cmd.get("symbols", [])) > MAX_SYMBOLS:
                    self.logger.warning(f"❌ Subscription rejected: too many symbols ({len(cmd['symbols'])} > {MAX_SYMBOLS})")
                    continue
                if not cmd.get("owner"):
                    self.logger.warning(f"❌ Subscription rejected: missing OWNER field. RAW: {cmd}")
                    continue
                if self.out_q is not None:
                    self.out_q.put(cmd)
                    if DEBUG_MODE:
                        self.logger.debug(f"Put command on out_q: {cmd}")
                else:
                    self.logger.warning("out_q is None, cannot forward command to bot.")
                self.redis.publish(r_cfg.DB_SAVE_SUBSCRIPTIONS, json.dumps(cmd))
                if DEBUG_MODE:
                    self.logger.debug(f"Published command to Redis channel {r_cfg.DB_SAVE_SUBSCRIPTIONS}: {cmd}")
                self.logger.info(f"✅ Sent command to out_q: {cmd}")
            except Exception as exc:
                self.logger.error(f"Invalid command: {exc} RAW:{raw}")

    def _normalize(self, cmd: dict) -> dict:
        if DEBUG_MODE:
            self.logger.debug(f"Normalizing command: {cmd}")
        cmd.setdefault("action", "add")
        cmd.setdefault("market", "linear")
        if isinstance(cmd.get("symbols"), str):
            cmd["symbols"] = [cmd["symbols"]]
        if "topics" not in cmd:
            cmd["topics"] = ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"]
        if "owner" not in cmd:
            cmd["owner"] = None
        return cmd

    def stop(self):
        self.running = False

    def _sync_subscriptions_to_db(self, subscriptions, owner=None):
        """
        Sync only the subscriptions that belong to the specified owner.
        Prevents overwriting protected or unrelated subscriptions in the DB.
        """
        if not owner:
            self.logger.warning("❌ Cannot sync to DB: missing owner.")
            return

        # Filter only the subscriptions that match the given owner
        filtered = {
            sub for sub, meta in subscriptions.items()
            if meta.get("owner") == owner
        }

        if not filtered:
            self.logger.info(f"🟡 No subscriptions to sync for owner={owner}")
            return

        symbols = sorted({sub.split(".")[-1] for sub in filtered})
        topics = sorted({".".join(sub.split(".")[:-1]) for sub in filtered})

        payload = {
            "action": "set_websocket_subscriptions",
            "owner": owner,
            "market": self.market,
            "symbols": symbols,
            "topics": topics,
        }
        self.logger.info(f"Syncing subscriptions to DB bot: {payload}")
        self.redis.publish(r_cfg.DB_SAVE_SUBSCRIPTIONS, json.dumps(payload))


    def remove_orderbook_subscriptions(self):
        self.logger.info("Removing all orderbook subscriptions (functionality to be implemented as needed).")

    def build_subscriptions(self, symbols, channels, owner=None):
        subs = {}
        for sym in symbols:
            for channel in channels:
                if channel.startswith("kline."):
                    interval = channel.split(".")[1]
                    key = f"kline.{interval}.{sym}"
                    subs[key] = {"owner": owner or self.market}
                elif channel.startswith("orderbook"):
                    depth = channel.split(".")[1] if "." in channel else 50
                    key = f"orderbook.{depth}.{sym}"
                    subs[key] = {"owner": owner or self.market}
                elif channel == "trade":
                    key = f"publicTrade.{sym}"
                    subs[key] = {"owner": owner or self.market}
        if DEBUG_MODE:
            self.logger.debug(f"Built subscriptions: {subs}")
        return subs

    def fetch_subscriptions_from_api(self):
        try:
            url = f"http://db_bot:8001/subscriptions/{self.market}"
            self.logger.info(f"📱 Fetching subscriptions from API: {url}")
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            subs = {
                f"{item['topic']}.{item['symbol']}": {"owner": item.get("owner", self.market)}
                for item in data
                if item.get("market") == self.market
            }
            self.logger.info(f"✅ API subscriptions fetched: {subs}")
            if DEBUG_MODE:
                self.logger.debug(f"API raw response: {data}")
            return subs
        except Exception as e:
            self.logger.warning(f"❌ Failed to fetch subscriptions from API: {e}")
            return set()

    def fetch_protected_symbols(self):
        try:
            url = "http://db_bot:8001/subscriptions/protected"
            self.logger.info(f"📱 Fetching protected symbols from API: {url}")
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            items = response.json()
            protected = set()
            for item in items:
                try:
                    protected.add((item["symbol"], item["market"]))
                except Exception as e:
                    self.logger.warning(f"Failed to process protected symbol: {item} | {e}")
            self.logger.info(f"🛡️ Protected symbols loaded: {protected}")
            if DEBUG_MODE:
                self.logger.debug(f"API protected symbols raw: {items}")
            return protected
        except Exception as e:
            self.logger.warning(f"❌ Failed to fetch protected symbols: {e}")
            return set()
"""

subscription_message = {
    "action": "set",
    "market": "linear",
    "symbols": ["BTCUSDT"],
    "owner":"bot_name",
    "topics": ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"]
    }
r.lpush("coin_subscription", json.dumps(subscription_message))
"""