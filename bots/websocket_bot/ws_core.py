import json, threading, queue, time, signal, datetime, logging
import websocket
import bots.websocket_bot.config_websocket_bot as cfg
from bots.config import config_redis as r_cfg
from bots.websocket_bot.subscription_handler import SubscriptionHandler
from bots.websocket_bot.message_router import MessageRouter
from bots.websocket_bot.websocket_utils import send_webhook
from bots.utils.logger import setup_logger
from bots.utils.redis_client import get_redis

BATCH_SIZE = getattr(cfg, "BATCH_SIZE", 10)
PING_SEC, PONG_TIMEOUT, REOPEN_SEC = 20, 10, 2

class WebSocketBot(threading.Thread):
    def __init__(self, market):
        super().__init__(daemon=True)
        self.market = market
        self.logger = setup_logger(f"{market}_ws_core.py", logging.INFO)
        self.redis = get_redis()
        self.cmd_q = queue.Queue()
        self.ws = None
        self.subscriptions = set()
        self.pending_subscriptions = []
        self.channels = set()
        self.exit_evt = threading.Event()
        self.router = MessageRouter(self.redis, market=market)

        # Choose the correct subscription channel based on market
        if self.market == "spot":
            subscription_channel = r_cfg.SPOT_SUBSCRIPTION_CHANNEL
        elif self.market == "linear":
            subscription_channel = r_cfg.LINEAR_SUBSCRIPTION_CHANNEL
        else:
            subscription_channel = r_cfg.SPOT_SUBSCRIPTION_CHANNEL

        self.sub_handler = SubscriptionHandler(self.redis, self.cmd_q, subscription_channel=subscription_channel)
        self.sub_handler.start()

        threading.Thread(target=self._heartbeat, daemon=True).start()
        threading.Thread(target=self._ws_watchdog, daemon=True).start()

        # Load subscriptions from Redis at startup
        self._load_subscriptions_from_redis()
        self._connect_ws()
    def run(self):
        send_webhook(cfg.DISCORD_WEBHOOK, "WebSocket Bot started.")
        self.logger.info(f"🚀 WebSocketBot running. {self.market}")
        while not self.exit_evt.is_set():
            try:
                cmd = self.cmd_q.get(timeout=1)
                self._handle_command(cmd)
            except queue.Empty:
                continue

    def stop(self):
        if self.exit_evt.is_set():
            return
        self.logger.info("🛑 Shutting down...")
        self.exit_evt.set()

        if self.ws and self.ws.sock:
            try:
                self.ws.close()
                self.logger.info("🟢 WebSocket closed successfully.")
            except Exception as e:
                self.logger.error(f"⚠️ WebSocket close failed: {e}")

        if self.sub_handler:
            self.sub_handler.stop()

        self._save_subscriptions_to_redis()
        send_webhook(cfg.DISCORD_WEBHOOK, "WebSocket Bot stopped.")
        self.logger.info("✅ Shutdown complete.")

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
    def log_current_subscriptions(self):
        if self.subscriptions:
            self.logger.info("📡 Current subscriptions (%d): %s",
                        len(self.subscriptions), ', '.join(sorted(self.subscriptions)))
        else:
            self.logger.info("📡 No active subscriptions.")
    def _load_subscriptions_from_redis(self):
        key = self._redis_key()
        saved_subscriptions = self.redis.smembers(key)
        if saved_subscriptions:
            self.subscriptions = {sub for sub in saved_subscriptions}
            self.logger.info(f"🔄 Loaded subscriptions from Redis: {self.market} {self.subscriptions}")
            self.log_current_subscriptions()
        else:
            self.logger.info("⚠️ No subscriptions found in Redis at startup.")
    def _handle_command(self, cmd):
        action = cmd.get("action", "add")
        market = cmd.get("market", "linear")
        symbols = cmd.get("symbols", [])
        channels = cmd.get("topics", ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"])

        if market != self.market:
            self.logger.info(f"🔄 Market change detected: {self.market} → {market}")
            self._change_market(market)

        new_subs = self._build_subscriptions(symbols, channels)

        if action == "set":
            self.subscriptions = new_subs
        elif action == "add":
            self.subscriptions |= new_subs
        elif action == "remove":
            self.subscriptions -= new_subs

        self._update_subscriptions()

    def _build_subscriptions(self, symbols, channels):
        subs = set()
        for sym in symbols:
            for channel in channels:
                if channel.startswith("kline."):
                    interval = channel.split(".")[1]
                    subs.add(f"kline.{interval}.{sym}")
                elif channel.startswith("orderbook"):
                    depth = channel.split(".")[1] if "." in channel else cfg.ORDER_BOOK_DEPTH
                    subs.add(f"orderbook.{depth}.{sym}")
                elif channel == "trade":
                    subs.add(f"publicTrade.{sym}")
        return subs

    def _change_market(self, new_market):
        if new_market == self.market:
            self.logger.info("🔵 Market unchanged (%s), no action taken.", new_market)
            return

        self.logger.info(f"🔄 Market change detected: {self.market} → {new_market}")

        if self.ws:
            try:
                self.ws.close()
                self.logger.info("🟢 WebSocket closed for market change.")
            except Exception as e:
                self.logger.warning("⚠️ Error closing WebSocket: %s", e)
            finally:
                self.ws = None

        self.channels.clear()
        self.subscriptions.clear()
        self.market = new_market

        self._connect_ws()


    def _update_subscriptions(self):
        if not self.ws or not self.ws.sock or not self.ws.sock.connected:
            self.logger.warning("⚠️ WebSocket disconnected; subscriptions delayed.")
            return

        new_subscriptions = self.subscriptions
        current_channels = set(self.channels)

        to_subscribe = new_subscriptions - current_channels
        to_unsubscribe = current_channels - new_subscriptions

        if to_unsubscribe:
            self.logger.info("🚫 Unsubscribing from %d topics", len(to_unsubscribe))
            for i in range(0, len(to_unsubscribe), BATCH_SIZE):
                batch = list(to_unsubscribe)[i:i+BATCH_SIZE]
                self.ws.send(json.dumps({"op": "unsubscribe", "args": batch}))
                self.logger.debug(f"Unsubscribed batch: {batch}")
            self.channels -= to_unsubscribe

        if to_subscribe:
            self.logger.info("✅ Subscribing to %d new topics", len(to_subscribe))
            for i in range(0, len(to_subscribe), BATCH_SIZE):
                batch = list(to_subscribe)[i:i+BATCH_SIZE]
                self.ws.send(json.dumps({"op": "subscribe", "args": batch}))
                self.logger.debug(f"Subscribed batch: {batch}")
            self.channels |= to_subscribe

        if not to_subscribe and not to_unsubscribe:
            self.logger.info("🟢 No subscription changes needed.")

        self.log_current_subscriptions()




    def _connect_ws(self):
        url = cfg.WS_URL[self.market]

        def _runner():
            while not self.exit_evt.is_set():
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws: (
                        self.logger.info("WS connected"),
                        self._update_subscriptions()  # <-- ONLY UPDATE HERE
                    ),
                    on_message=self._on_message,
                    on_error=lambda ws, err: self.logger.error(f"WS error: {err}"),
                    on_close=lambda *_: self.logger.warning("WS closed"),
                    on_pong=lambda *_: self.logger.debug("pong"),
                )
                self.ws.run_forever(ping_interval=PING_SEC, ping_timeout=PONG_TIMEOUT)
                if not self.exit_evt.is_set():
                    self.logger.warning(f"Reconnecting WS in {REOPEN_SEC}s...")
                    time.sleep(REOPEN_SEC)

        threading.Thread(target=_runner, daemon=True).start()


    def _flush_pending(self):
        if not self.pending_subscriptions:
            return
        if not self.ws or not self.ws.sock or not self.ws.sock.connected:
            self.logger.warning("⚠️ WebSocket not connected yet, delaying subscription.")
            return

        self.logger.info(f"✅ Subscribing to {len(self.pending_subscriptions)} new topics")
        for i in range(0, len(self.pending_subscriptions), BATCH_SIZE):
            batch = self.pending_subscriptions[i:i+BATCH_SIZE]
            try:
                self.ws.send(json.dumps({"op": "subscribe", "args": batch}))
            except websocket.WebSocketConnectionClosedException:
                self.logger.warning("⚠️ WebSocket unexpectedly closed during subscription.")
                self.ws = None  # Reset connection to trigger reconnection
                return

        self.pending_subscriptions.clear()  # Clear subscriptions after successful send


    def _ws_watchdog(self):
        while not self.exit_evt.is_set():
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self._flush_pending()
            self.exit_evt.wait(5)

    def _heartbeat(self):
        while not self.exit_evt.is_set():
            self.redis.publish(r_cfg.HEARTBEAT_CHANNEL, json.dumps({
                "bot_name": cfg.BOT_NAME,
                "heartbeat": True,
                "time": datetime.datetime.utcnow().isoformat(),
                "auth_token": cfg.BOT_AUTH_TOKEN
            }))
            self.exit_evt.wait(cfg.HEARTBEAT_INTERVAL)

    def _on_message(self, _ws, raw: str):
        try:
            data = json.loads(raw)
            topic = data.get("topic", "")
            if "kline" in topic:
                _, interval, symbol = topic.split(".")
                self.logger.debug("KLINE → %s %s", symbol, interval)
            elif "orderbook" in topic:
                _, depth, symbol = topic.split(".")
                self.logger.debug("ORDERBOOK → %s depth %s", symbol, depth)
            elif "publicTrade" in topic:
                _, symbol = topic.split(".")
                self.logger.debug("TRADE → %s", symbol)

            # Add debugging for SEQ GAP warning
            if "orderbook" in topic and "seq_gap" in data.get("type", "").lower():
                symbol = data.get("symbol", "?")
                last_seq = data.get("last_seq", "?")
                new_seq = data.get("new_seq", "?")
                self.logger.debug(f"[DEBUG][SEQ GAP] symbol={symbol} last_seq={last_seq} new_seq={new_seq} raw={raw[:200]}")

            # Routing (unchanged)
            if "publicTrade" in topic:
                self.router.trade(data)
            elif "kline" in topic:
                self.router.kline(data)
            elif "orderbook" in topic:
                self.router.orderbook(data)

        except Exception as exc:
            self.logger.error("Parse fail: %s – first 120 chars: %s…",
                              exc, raw[:120])




