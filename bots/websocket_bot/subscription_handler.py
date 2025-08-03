import json, threading, logging, requests
from utils.redis_client import get_redis
from config import config_redis as r_cfg
from utils.logger import setup_logger
import config_websocket_bot as cfg

DEBUG_MODE = False
BATCH_SIZE = getattr(cfg, "BATCH_SIZE", 10)
PING_SEC, PONG_TIMEOUT, REOPEN_SEC = 20, 10, 2
MAX_SYMBOLS = 50

def get_log_level():
    return logging.DEBUG if DEBUG_MODE else logging.INFO

class SubscriptionHandler(threading.Thread):
    def __init__(self, market, logger, reset_seq_callback=None, update_ws_subscriptions=None):
        super().__init__(daemon=True)
        self.market = market
        self.logger = setup_logger("subscription_handler.log", get_log_level())
        self.reset_seq_callback = reset_seq_callback
        self.ws_update_callback = update_ws_subscriptions
        self.subscription_channel = {
            "spot": r_cfg.SPOT_SUBSCRIPTION_CHANNEL,
            "linear": r_cfg.LINEAR_SUBSCRIPTION_CHANNEL,
            "derivatives": r_cfg.DERIVATIVES_SUBSCRIPTION_CHANNEL,
        }.get(self.market, r_cfg.SPOT_SUBSCRIPTION_CHANNEL)
        self.redis = get_redis()
        self.out_q = None
        self.running = True
        self.ws = None
        self.channels = set()
        self.subscriptions = self.fetch_subscriptions_from_api()
        self.protected_symbols = self.fetch_protected_symbols()

    def run(self):
        self.logger.info(f"Listening on Redis list '{self.subscription_channel}' …")
        while self.running:
            if DEBUG_MODE:
                self.logger.debug("Waiting for subscription command from Redis...")
            _key, raw = self.redis.blpop(self.subscription_channel)
            try:
                cmd = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                if DEBUG_MODE:
                    self.logger.debug(f"Decoded JSON command: {cmd}")
                cmd = self._normalize(cmd)
                if len(cmd.get("symbols", [])) > MAX_SYMBOLS:
                    self.logger.warning(f"❌ Subscription rejected: too many symbols ({len(cmd['symbols'])} > {MAX_SYMBOLS})")
                    continue
                if not cmd.get("owner"):
                    self.logger.warning(f"❌ Subscription rejected: missing OWNER field. RAW: {cmd}")
                    continue
                for symbol in cmd.get("symbols", []):
                    single_cmd = {
                        "action": cmd["action"],
                        "market": cmd["market"],
                        "symbol": symbol,
                        "owner": cmd["owner"],
                        "topics": cmd["topics"]
                    }
                    self.redis.publish(r_cfg.DB_SAVE_SUBSCRIPTIONS, json.dumps(single_cmd))
                self.logger.info(f"✅ Sent command to out_q: {cmd}")
                self.handle_command(cmd)
            except Exception as exc:
                self.logger.error(f"Invalid command: {exc} RAW:{raw}")

    def _normalize(self, cmd: dict) -> dict:
        cmd.setdefault("action", "add")
        cmd.setdefault("market", self.market)
        if isinstance(cmd.get("symbols"), str):
            cmd["symbols"] = [cmd["symbols"]]
        if "topics" not in cmd:
            cmd["topics"] = ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"]
        if "owner" not in cmd:
            cmd["owner"] = None
        return cmd

    def stop(self):
        self.running = False

    def build_subscriptions(self, symbols, topics, owner=None):
        subs = {}
        for sym in symbols:
            for topic in topics:
                if topic.startswith("kline."):
                    interval = topic.split(".")[1]
                    key = f"kline.{interval}.{sym}"
                    subs[key] = {"owner": owner or self.market}
                elif topic.startswith("orderbook"):
                    depth = topic.split(".")[1] if "." in topic else 50
                    key = f"orderbook.{depth}.{sym}"
                    subs[key] = {"owner": owner or self.market}
                elif topic == "publicTrade":
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
            return {}

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

    def handle_command(self, cmd):
        self.logger.debug(f"[DEBUG][handle_command] called with: {cmd}")
        owner = cmd.get("owner", "unknown owner")
        action = cmd.get("action", "add")
        market = cmd.get("market", self.market)
        symbols = cmd.get("symbols", [])
        topics = cmd.get("topics", ["kline.1"])

        self.logger.debug(f"[DEBUG][handle_command] action={action}, market={market}, symbols={symbols}, topics={topics}")

        if len(symbols) > MAX_SYMBOLS:
            self.logger.warning(f"[DEBUG][handle_command] ⚠️ Symbol limit ({MAX_SYMBOLS}) exceeded. Trimming extra symbols.")
            symbols = symbols[:MAX_SYMBOLS]

        if market not in cfg.WS_URL:
            self.logger.error(f"[DEBUG][handle_command] ⚠️ Invalid market type: {market}")
            return

        new_subs_dict = self.build_subscriptions(symbols, topics, owner=owner)
        new_sub_keys = set(new_subs_dict.keys())
        self.logger.debug(f"[DEBUG][handle_command] new_subs_dict={new_subs_dict}")
        self.logger.debug(f"[DEBUG][handle_command] new_sub_keys={new_sub_keys}")

        if action == "set":
            self.logger.debug(f"[DEBUG][handle_command] SET action")
            self.subscriptions = new_subs_dict
        elif action == "add":
            self.logger.debug(f"[DEBUG][handle_command] ADD action")
            new_to_add = {k: v for k, v in new_subs_dict.items() if k not in self.subscriptions}
            self.logger.debug(f"[DEBUG][handle_command] new_to_add={new_to_add}")
            self.subscriptions.update(new_to_add)
            self.channels -= set(new_to_add.keys())
            if new_to_add:
                self.logger.info(f"[DEBUG][handle_command] 🔄 Detected new topics to subscribe: {set(new_to_add.keys())}")
        elif action == "remove":
            self.logger.debug(f"[DEBUG][handle_command] REMOVE action")
            self.logger.debug(f"[DEBUG][handle_command] Subscriptions before removal: {self.subscriptions}")
            self.logger.debug(f"[DEBUG][handle_command] Attempting to remove: {list(new_sub_keys)}")
            for sub in new_sub_keys:
                symbol = sub.split(".")[-1]
                if (symbol, self.market) in self.protected_symbols:
                    self.logger.info(f"🛡️ Skipping removal of protected subscription: {sub}")
                    continue
                self.subscriptions.pop(sub, None)
            self.logger.debug(f"[DEBUG][handle_command] Subscriptions after removal: {self.subscriptions}")

        self.logger.debug(f"[DEBUG][handle_command] Updated subscriptions: {self.subscriptions}")

        if self.ws_update_callback:
            self.logger.debug(f"[DEBUG][handle_command] Calling ws_update_callback with: {set(self.subscriptions.keys())}")
            try:
                self.ws_update_callback(set(self.subscriptions.keys()))
                self.logger.debug(f"[DEBUG][handle_command] ws_update_callback call succeeded")
            except Exception as e:
                self.logger.error(f"[DEBUG][handle_command] ws_update_callback call FAILED: {e}")
        else:
            self.logger.warning(f"[DEBUG][handle_command] ws_update_callback is None!")

    def _update_subscriptions(self):
        self.logger.debug(f"[DEBUG][_update_subscriptions] channels={self.channels}, subs={self.subscriptions}")
        if not self.ws or not self.ws.sock or not self.ws.sock.connected:
            self.logger.warning("[DEBUG][_update_subscriptions] WebSocket disconnected; subscriptions delayed.")
            return

        protected = self.protected_symbols
        new_subs = set(self.subscriptions)
        curr_channels = set(self.channels)
        to_sub = new_subs - curr_channels
        to_unsub = curr_channels - new_subs

        filtered_unsub = {s for s in to_unsub if (s.split(".")[-1], self.market) not in protected}
        skipped_unsub = to_unsub - filtered_unsub

        if skipped_unsub:
            self.logger.info(f"🛡️ Skipped unsubscribe for protected: {skipped_unsub}")

        self.logger.info(f"[DEBUG][_update_subscriptions] to_sub={to_sub}, to_unsub={filtered_unsub}")

        for sub in to_sub:
            parts = sub.split(".")
            if len(parts) >= 3:
                symbol = parts[2]
                if self.reset_seq_callback:
                    self.reset_seq_callback(symbol)

        if filtered_unsub:
            for i in range(0, len(filtered_unsub), BATCH_SIZE):
                batch = list(filtered_unsub)[i:i+BATCH_SIZE]
                try:
                    self.ws.send(json.dumps({"op": "unsubscribe", "args": batch}))
                    self.logger.info(f"[DEBUG][_update_subscriptions] Unsubscribe batch sent: {batch}")
                except Exception as e:
                    self.logger.error(f"[DEBUG][_update_subscriptions] Failed to unsubscribe batch {batch}: {e}")
            self.channels -= filtered_unsub

        if to_sub:
            for i in range(0, len(to_sub), BATCH_SIZE):
                batch = list(to_sub)[i:i+BATCH_SIZE]
                try:
                    self.ws.send(json.dumps({"op": "subscribe", "args": batch}))
                    self.logger.info(f"[DEBUG][_update_subscriptions] Subscribe batch sent: {batch}")
                except Exception as e:
                    self.logger.error(f"[DEBUG][_update_subscriptions] Failed to subscribe batch {batch}: {e}")
            self.channels |= to_sub

        if not to_sub and not filtered_unsub:
            self.logger.info("[DEBUG][_update_subscriptions] 🟢 No subscription changes needed.")

        self._log_current_subscriptions()


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