import json, threading, logging, requests
from utils.redis_client import get_redis
from config import config_redis as r_cfg
from utils.logger import setup_logger
import config_websocket_bot as cfg

# === MASTER DEBUG SWITCH ===
DEBUG_MODE = True  # Set to False for INFO level and less verbose logs
BATCH_SIZE = getattr(cfg, "BATCH_SIZE", 10)
PING_SEC, PONG_TIMEOUT, REOPEN_SEC = 20, 10, 2
MAX_SYMBOLS = 50  # Maximum allowed symbols per subscription

def get_log_level():
    return logging.DEBUG if DEBUG_MODE else logging.INFO

class SubscriptionHandler(threading.Thread):
    def __init__(self, market, logger, reset_seq_callback=None, update_ws_subscriptions=None):
        super().__init__(daemon=True)
        self.market = market
        self.logger = logger
        self.reset_seq_callback = reset_seq_callback
        self.ws_update_callback = update_ws_subscriptions
        self.subscription_channel = {
            "spot": r_cfg.SPOT_SUBSCRIPTION_CHANNEL,
            "linear": r_cfg.LINEAR_SUBSCRIPTION_CHANNEL,
            "derivatives": r_cfg.DERIVATIVES_SUBSCRIPTION_CHANNEL,
        }.get(self.market, r_cfg.SPOT_SUBSCRIPTION_CHANNEL)
        self.redis = get_redis()
        self.out_q = None
        self.logger = setup_logger("subscription_handler.log", get_log_level())
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
                self.redis.publish(r_cfg.DB_SAVE_SUBSCRIPTIONS, json.dumps(cmd))
                if DEBUG_MODE:
                    self.logger.debug(f"Published command to Redis channel {r_cfg.DB_SAVE_SUBSCRIPTIONS}: {cmd}")
                self.logger.info(f"✅ Sent command to out_q: {cmd}")

                # PATCH: Actually update in-memory state and trigger live WS update!
                self.handle_command(cmd)

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
        self.logger.debug(f"[DEBUG][_sync_subscriptions_to_db] Called with subscriptions: {subscriptions}, owner={owner}")
        try:
            # Your DB sync logic here
            # For example:
            # result = db.save_subscriptions(subscriptions, owner)
            # self.logger.debug(f"[DEBUG][_sync_subscriptions_to_db] DB result: {result}")
            pass
        except Exception as e:
            self.logger.error(f"[DEBUG][_sync_subscriptions_to_db] Exception: {e}")

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

    def _save_subscriptions_to_redis(self):
        key = self._redis_key()
        self.redis.delete(key)
        if self.subscriptions:
            self.redis.sadd(key, *self.subscriptions)
            self.logger.info(f"💾 Saved current subscriptions to Redis: {self.market} {self.subscriptions}")
        else:
            self.logger.info("⚠️ No subscriptions to save.")

    def _redis_key(self):
        return f"{r_cfg.REDIS_SUBSCRIPTION_KEY}:{self.market}"

    def _log_current_subscriptions(self):
        if self.subscriptions:
            self.logger.info(f"📡 [{self.market.upper()}] Current subscriptions ({len(self.subscriptions)}): {', '.join(sorted(self.subscriptions))}")
        else:
            self.logger.info(f"📡 [{self.market.upper()}] No active subscriptions.")

    def handle_command(self, cmd):
        self.logger.debug(f"[DEBUG][handle_command] called with: {cmd}")
        owner = cmd.get("owner", self.market)
        action = cmd.get("action", "add")
        market = cmd.get("market", "linear")
        symbols = cmd.get("symbols", [])
        channels = cmd.get("topics", ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"])

        self.logger.debug(f"[DEBUG][handle_command] action={action}, market={market}, symbols={symbols}, channels={channels}")

        if len(symbols) > MAX_SYMBOLS:
            self.logger.warning(f"[DEBUG][handle_command] ⚠️ Symbol limit ({MAX_SYMBOLS}) exceeded. Trimming extra symbols.")
            symbols = symbols[:MAX_SYMBOLS]

        if market not in cfg.WS_URL:
            self.logger.error(f"[DEBUG][handle_command] ⚠️ Invalid market type: {market}")
            return

        new_subs_dict = self.build_subscriptions(symbols, channels, owner=owner)
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
                if sub in self.subscriptions:
                    self.logger.debug(f"[DEBUG][handle_command] Removing subscription: {sub}")
                    self.subscriptions.pop(sub, None)
            self.logger.debug(f"[DEBUG][handle_command] Subscriptions after removal: {self.subscriptions}")

        self.logger.debug(f"[DEBUG][handle_command] Updated subscriptions: {self.subscriptions}")

        # Add debug for DB sync
        self.logger.debug(f"[DEBUG][handle_command] Syncing subscriptions to DB for owner={owner}")
        self._sync_subscriptions_to_db(self.subscriptions, owner=owner)
        self.logger.debug(f"[DEBUG][handle_command] DB sync complete for owner={owner}")

        # PATCH: Always call the callback to update live WS subscriptions
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
        if not self.ws:
            self.logger.warning("[DEBUG][_update_subscriptions] self.ws is None!")
        elif not self.ws.sock:
            self.logger.warning("[DEBUG][_update_subscriptions] self.ws.sock is None!")
        elif not self.ws.sock.connected:
            self.logger.warning("[DEBUG][_update_subscriptions] WebSocket disconnected; subscriptions delayed.")
            return

        new_subs, curr_channels = set(self.subscriptions), set(self.channels)
        self.logger.debug(f"[DEBUG][_update_subscriptions] new_subs={new_subs}, curr_channels={curr_channels}")
        to_sub, to_unsub = new_subs - curr_channels, curr_channels - new_subs

        self.logger.info(f"[DEBUG][_update_subscriptions] to_sub={to_sub}, to_unsub={to_unsub}, curr_channels={curr_channels}, new_subs={new_subs}")

        for sub in list(to_sub):
            parts = sub.split(".")
            if len(parts) >= 3:
                symbol = parts[2]
                self.logger.debug(f"[DEBUG][_update_subscriptions] Resetting sequence for symbol: {symbol}")
                if self.reset_seq_callback:
                    self.logger.debug(f"[DEBUG][_update_subscriptions] Calling reset_seq_callback for symbol: {symbol}")
                    self.reset_seq_callback(symbol)

        to_unsub = {s for s in to_unsub if (s.split(".")[-1], self.market) not in self.protected_symbols}

        if to_unsub:
            self.logger.info(f"[DEBUG][_update_subscriptions] 🚫 Unsubscribing from {len(to_unsub)} topics")
            for i in range(0, len(to_unsub), BATCH_SIZE):
                batch = list(to_unsub)[i:i+BATCH_SIZE]
                self.logger.info(f"[DEBUG][_update_subscriptions] Sending unsubscribe batch: {batch}")
                try:
                    self.ws.send(json.dumps({"op": "unsubscribe", "args": batch}))
                    self.logger.info(f"[DEBUG][_update_subscriptions] Unsubscribe batch sent: {batch}")
                except Exception as e:
                    self.logger.error(f"[DEBUG][_update_subscriptions] Failed to unsubscribe batch {batch}: {e}")
            self.channels -= to_unsub

        if to_sub:
            self.logger.info(f"[DEBUG][_update_subscriptions] ✅ Subscribing to {len(to_sub)} new topics")
            for i in range(0, len(to_sub), BATCH_SIZE):
                batch = list(to_sub)[i:i+BATCH_SIZE]
                self.logger.info(f"[DEBUG][_update_subscriptions] Sending subscribe batch: {batch}")
                try:
                    self.ws.send(json.dumps({"op": "subscribe", "args": batch}))
                    self.logger.info(f"[DEBUG][_update_subscriptions] Subscribe batch sent: {batch}")
                except Exception as e:
                    self.logger.error(f"[DEBUG][_update_subscriptions] Failed to subscribe batch {batch}: {e}")
            self.channels |= to_sub

        if not to_sub and not to_unsub:
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