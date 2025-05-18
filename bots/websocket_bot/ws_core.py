"""
WebSocketBot Core Logic
Author: Jericho
Clean, professional, and beautifully structured. All variables at the top, concise logic, and clear comments.
"""
import json, threading, queue, time, signal, datetime, logging
import websocket
import bots.websocket_bot.config_websocket_bot as cfg
from bots.config import config_redis as r_cfg
from bots.websocket_bot.subscription_handler import SubscriptionHandler
from bots.websocket_bot.message_router import MessageRouter
from bots.websocket_bot.websocket_utils import send_webhook
from bots.utils.logger import setup_logger
from bots.utils.redis_client import get_redis

# ==== Jericho: Configurable Constants ====
BATCH_SIZE = getattr(cfg, "BATCH_SIZE", 10)
PING_SEC, PONG_TIMEOUT, REOPEN_SEC = 20, 10, 2

class WebSocketBot(threading.Thread):
    """
    Jericho: Professional, minimal, and robust WebSocket trading bot core.
    Handles subscriptions, Redis sync, and message routing for spot/linear markets.
    """
    def __init__(self, market):
        super().__init__(daemon=True)
        # ==== Jericho: Core State ====
        self.market = market
        self.logger = setup_logger(f"{market}_ws_core.py", logging.INFO)
        self.redis = get_redis()
        self.cmd_q = queue.Queue()
        self.ws = None
        self.subscriptions = set()
        self.channels = set()
        self.exit_evt = threading.Event()
        self.router = MessageRouter(self.redis, market=market)
        self.pending_subscriptions = []

        # ==== Jericho: Market-specific Redis channel ====
        subscription_channel = {
            "spot": r_cfg.SPOT_SUBSCRIPTION_CHANNEL,
            "linear": r_cfg.LINEAR_SUBSCRIPTION_CHANNEL
        }.get(self.market, r_cfg.SPOT_SUBSCRIPTION_CHANNEL)
        self.sub_handler = SubscriptionHandler(self.redis, self.cmd_q, subscription_channel=subscription_channel)
        self.sub_handler.start()

        # ==== Jericho: Background Threads ====
        threading.Thread(target=self._heartbeat, daemon=True).start()
        threading.Thread(target=self._ws_watchdog, daemon=True).start()

        # ==== Jericho: Startup State ====
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
        if self.exit_evt.is_set(): return
        self.logger.info("🛑 Shutting down...")
        self.exit_evt.set()
        if self.ws and self.ws.sock:
            try:
                self.ws.close()
                self.logger.info("🟢 WebSocket closed successfully.")
            except Exception as e:
                self.logger.error(f"⚠️ WebSocket close failed: {e}")
        if self.sub_handler: self.sub_handler.stop()
        self._save_subscriptions_to_redis()
        send_webhook(cfg.DISCORD_WEBHOOK, "WebSocket Bot stopped.")
        self.logger.info("✅ Shutdown complete.")

    # ==== Jericho: Redis Subscription State ====
    def _redis_key(self):
        return f"{r_cfg.REDIS_SUBSCRIPTION_KEY}:{self.market}"

    def _save_subscriptions_to_redis(self):
        key = self._redis_key()
        self.redis.delete(key)
        if self.subscriptions:
            self.redis.sadd(key, *self.subscriptions)
            self.logger.info(f"💾 Saved current subscriptions to Redis: {self.market} {self.subscriptions}")
        else:
            self.logger.info("⚠️ No subscriptions to save.")

    def _load_subscriptions_from_redis(self):
        key = self._redis_key()
        saved = self.redis.smembers(key)
        if saved:
            self.subscriptions = set(saved)
            self.logger.info(f"🔄 Loaded subscriptions from Redis: {self.market} {self.subscriptions}")
            self.log_current_subscriptions()
        else:
            self.logger.info("⚠️ No subscriptions found in Redis at startup.")

    def log_current_subscriptions(self):
        if self.subscriptions:
            self.logger.info(f"📡 Current subscriptions ({len(self.subscriptions)}): {', '.join(sorted(self.subscriptions))}")
        else:
            self.logger.info("📡 No active subscriptions.")

    # ==== Jericho: Command Handling ====
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
            self.logger.info(f"🔵 Market unchanged ({new_market}), no action taken.")
            return
        self.logger.info(f"🔄 Market change detected: {self.market} → {new_market}")
        if self.ws:
            try:
                self.ws.close()
                self.logger.info("🟢 WebSocket closed for market change.")
            except Exception as e:
                self.logger.warning(f"⚠️ Error closing WebSocket: {e}")
            finally:
                self.ws = None
        self.channels.clear()
        self.subscriptions.clear()
        self.market = new_market
        self._connect_ws()

    # ==== Jericho: Subscription Management ====
    def _update_subscriptions(self):
        if not self.ws or not self.ws.sock or not self.ws.sock.connected:
            self.logger.warning("⚠️ WebSocket disconnected; subscriptions delayed.")
            return
        new_subs, curr_channels = self.subscriptions, set(self.channels)
        to_sub, to_unsub = new_subs - curr_channels, curr_channels - new_subs
        # Jericho: Reset sequence for new subscriptions
        for sub in to_sub:
            parts = sub.split(".")
            if len(parts) >= 3:
                symbol = parts[2]
                self.router.reset_seq(symbol)
        # Jericho: Unsubscribe
        if to_unsub:
            self.logger.info(f"🚫 Unsubscribing from {len(to_unsub)} topics")
            for i in range(0, len(to_unsub), BATCH_SIZE):
                batch = list(to_unsub)[i:i+BATCH_SIZE]
                self.ws.send(json.dumps({"op": "unsubscribe", "args": batch}))
                self.logger.debug(f"Unsubscribed batch: {batch}")
            self.channels -= to_unsub
        # Jericho: Subscribe
        if to_sub:
            self.logger.info(f"✅ Subscribing to {len(to_sub)} new topics")
            for i in range(0, len(to_sub), BATCH_SIZE):
                batch = list(to_sub)[i:i+BATCH_SIZE]
                self.ws.send(json.dumps({"op": "subscribe", "args": batch}))
                self.logger.debug(f"Subscribed batch: {batch}")
            self.channels |= to_sub
        if not to_sub and not to_unsub:
            self.logger.info("🟢 No subscription changes needed.")
        self.log_current_subscriptions()

    # ==== Jericho: WebSocket Connection ====
    def _connect_ws(self):
        url = cfg.WS_URL[self.market]
        def _runner():
            while not self.exit_evt.is_set():
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws: (self.logger.info("WS connected"), self._update_subscriptions()),
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

    # ==== Jericho: Pending Subscription Flush ====
    def _flush_pending(self):
        if not self.pending_subscriptions: return
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
                self.ws = None
                return
        self.pending_subscriptions.clear()

    # ==== Jericho: Watchdog & Heartbeat ====
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

    # ==== Jericho: WebSocket Message Handler ====
    def _on_message(self, _ws, raw: str):
        try:
            data = json.loads(raw)
            topic = data.get("topic", "")
            if "kline" in topic:
                _, interval, symbol = topic.split(".")
                self.logger.debug(f"KLINE → {symbol} {interval}")
            elif "orderbook" in topic:
                _, depth, symbol = topic.split(".")
                self.logger.debug(f"ORDERBOOK → {symbol} depth {depth}")
            elif "publicTrade" in topic:
                _, symbol = topic.split(".")
                self.logger.debug(f"TRADE → {symbol}")
            # Jericho: SEQ GAP Debugging (remove when resolved)
            if "orderbook" in topic and "seq_gap" in data.get("type", "").lower():
                symbol = data.get("symbol", "?")
                last_seq = data.get("last_seq", "?")
                new_seq = data.get("new_seq", "?")
                self.logger.debug(f"[DEBUG][SEQ GAP] symbol={symbol} last_seq={last_seq} new_seq={new_seq} raw={raw[:200]}")
            # Jericho: Route message
            if "publicTrade" in topic:
                self.router.trade(data)
            elif "kline" in topic:
                self.router.kline(data)
            elif "orderbook" in topic:
                self.router.orderbook(data)
        except Exception as exc:
            self.logger.error(f"Parse fail: {exc} – first 120 chars: {raw[:120]}…")




