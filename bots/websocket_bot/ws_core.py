"""
WebSocketBot Core Logic
Author: Jericho
Clean, professional, and beautifully structured. All variables at the top, concise logic, and clear comments.

Redis Channels Used:
-------------------
- r_cfg.SERVICE_STATUS_CHANNEL
    Purpose: Publishes bot status updates (started, stopped) and heartbeats.
    Used in: run(), stop(), _heartbeat()

- r_cfg.REDIS_SUBSCRIPTION_KEY (as f"{r_cfg.REDIS_SUBSCRIPTION_KEY}:{self.market}")
    Purpose: Stores the set of active subscriptions for each market (spot, linear, etc.)
    Used in: _save_subscriptions_to_redis(), _load_subscriptions_from_redis()

- r_cfg.SPOT_SUBSCRIPTION_CHANNEL, r_cfg.LINEAR_SUBSCRIPTION_CHANNEL, r_cfg.DERIVATIVES_SUBSCRIPTION_CHANNEL
    Purpose: Used by SubscriptionHandler to listen for subscription commands for each market type.
    Used in: __init__ (passed to SubscriptionHandler)
"""

# =====================================================
# Jericho: Imports and Config
# =====================================================
import json, threading, queue, time, signal, datetime, logging, os
import websocket

import config_websocket_bot as cfg
from utils import setup_logger, get_redis
from utils.redis_handler import RedisHandler
from utils.HeartBeatService import HeartBeat
from config import config_redis as r_cfg
from config import config_common as common_cfg
from subscription_handler import SubscriptionHandler, MAX_SYMBOLS
from message_router import MessageRouter
from websocket_utils import send_webhook

# =====================================================
# Jericho: Configurable Constants
# =====================================================
BATCH_SIZE = getattr(cfg, "BATCH_SIZE", 10)
PING_SEC, PONG_TIMEOUT, REOPEN_SEC = 20, 10, 2

# =====================================================
# Jericho: WebSocketBot Class
# =====================================================
class WebSocketBot(threading.Thread):
    """
    Jericho: Professional, minimal, and robust WebSocket trading bot core.
    Handles subscriptions, Redis sync, and message routing for spot/linear markets.
    """
    def __init__(self, market):
        super().__init__(daemon=True)
        self.market = market
        # Set up logger with correct log file and level
        log_level = logging.DEBUG if getattr(cfg, "LOG_LEVEL", "INFO").upper() == "DEBUG" else logging.INFO
        self.logger = setup_logger(f"{market}_ws_core.log", log_level)
        self.redis_handler = RedisHandler(r_cfg, self.logger)
        self.redis_handler.connect()
        self.redis = self.redis_handler.client
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
            "linear": r_cfg.LINEAR_SUBSCRIPTION_CHANNEL,
            "derivatives": getattr(r_cfg, 'DERIVATIVES_SUBSCRIPTION_CHANNEL', None)
        }.get(self.market, r_cfg.SPOT_SUBSCRIPTION_CHANNEL)
        self.sub_handler = SubscriptionHandler(self.redis, self.cmd_q, subscription_channel=subscription_channel)
        self.sub_handler.start()

        # Heartbeat setup
        self.status = {
            "bot_name": f"{cfg.BOT_NAME}:{self.market}",
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": cfg.BOT_AUTH_TOKEN,
            "metadata": {
                "version": getattr(cfg, "VERSION", "1.0.0"),
                "pid": os.getpid(),
                "strategy": getattr(cfg, "STRATEGY_NAME", "-"),
                "vitals": {
                    "market": self.market,
                    "subscriptions": sorted(list(self.subscriptions)),
                    "kline_count": getattr(self, "kline_count", 0),
                    "timestamp": datetime.datetime.utcnow().isoformat(),
                }
            }
        }
        self.heartbeat = HeartBeat(
            bot_name=f"{cfg.BOT_NAME}:{self.market}",
            auth_token=cfg.BOT_AUTH_TOKEN,
            logger=self.logger,
            redis_handler=self.redis_handler,
            metadata=self.status
        )

        # ==== Jericho: Startup State ====
        self._load_subscriptions_from_redis()
        self._connect_ws()
        threading.Thread(target=self._ws_watchdog, daemon=True).start()

    # =====================================================
    # Jericho: Main Run Loop
    # =====================================================
    def run(self):
        """
        Main thread loop for the WebSocketBot.
        Processes commands from the queue and exits cleanly when the exit event is set.
        """
        send_webhook(cfg.DISCORD_WEBHOOK, "WebSocket Bot started.")
        self.logger.info(f"🚀 WebSocketBot running. {self.market}")
        while not self.exit_evt.is_set():
            try:
                cmd = self.cmd_q.get(timeout=1)
                self.logger.debug(f"Received command: {cmd}")
                self._handle_command(cmd)
            except queue.Empty:
                continue

    # =====================================================
    # Jericho: Shutdown Logic
    # =====================================================
    def stop(self):
        """
        Cleanly shuts down the WebSocketBot.
        Closes the WebSocket, stops the subscription handler, and saves subscriptions to Redis.
        """
        if self.exit_evt.is_set(): return
        self.logger.info("🛑 Shutting down...")
        self.exit_evt.set()
        if self.ws and self.ws.sock:
            try:
                self.ws.close()
                self.logger.info("🟢 WebSocket closed successfully.")
            except Exception as e:
                self.logger.warning(f"⚠️ WebSocket close failed: {e}")
        if self.sub_handler: self.sub_handler.stop()
        self._save_subscriptions_to_redis()
        send_webhook(cfg.DISCORD_WEBHOOK, "WebSocket Bot stopped.")
        self.logger.info("✅ Shutdown complete.")

    # =====================================================
    # Jericho: Redis Subscription State
    # =====================================================
    def _redis_key(self):
        """
        Returns the Redis key for storing subscriptions for the current market.
        Returns:
            str: Redis key string.
        """
        return f"{r_cfg.REDIS_SUBSCRIPTION_KEY}:{self.market}"

    def _save_subscriptions_to_redis(self):
        """
        Saves the current set of subscriptions to Redis for persistence across restarts.
        If there are no subscriptions, deletes the key.
        """
        key = self._redis_key()
        self.redis.delete(key)
        if self.subscriptions:
            self.redis.sadd(key, *self.subscriptions)
            self.logger.info(f"💾 Saved current subscriptions to Redis: {self.market} {self.subscriptions}")
        else:
            self.logger.info("⚠️ No subscriptions to save.")

    def _load_subscriptions_from_redis(self):
        """
        Loads the set of subscriptions from Redis, if any exist, and updates the bot's state.
        """
        key = self._redis_key()
        saved = self.redis.smembers(key)
        if saved:
            self.subscriptions = set(saved)
            self.logger.info(f"🔄 Loaded subscriptions from Redis: {self.market} {self.subscriptions}")
            self.log_current_subscriptions()
        else:
            self.logger.info("⚠️ No subscriptions found in Redis at startup.")

    def log_current_subscriptions(self):
        """
        Logs the current active subscriptions for the market.
        """
        if self.subscriptions:
            self.logger.info(f"📡 [{self.market.upper()}] Current subscriptions ({len(self.subscriptions)}): {', '.join(sorted(self.subscriptions))}")
        else:
            self.logger.info(f"📡 [{self.market.upper()}] No active subscriptions.")

    # =====================================================
    # Jericho: Command Handling
    # =====================================================
    def _handle_command(self, cmd):
        """
        Handles incoming subscription commands from the queue.
        Supports 'add', 'remove', and 'set' actions for symbols and channels.
        Args:
            cmd (dict): Command dictionary with 'action', 'market', 'symbols', and 'topics'.
        """
        action = cmd.get("action", "add")
        market = cmd.get("market", "linear")
        symbols = cmd.get("symbols", [])
        channels = cmd.get("topics", ["trade", "orderbook", "kline.1", "kline.5", "kline.60", "kline.D"])
        # Enforce symbol cap
        if len(symbols) > MAX_SYMBOLS:
            self.logger.warning(f"⚠️ Symbol limit ({MAX_SYMBOLS}) exceeded. Trimming extra symbols.")
            symbols = symbols[:MAX_SYMBOLS]
        # If invalid market, do not change subscriptions
        if market not in cfg.WS_URL:
            self.logger.error(f"⚠️ Invalid market type: {market}")
            return
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
        self.logger.debug(f"Updated subscriptions: {self.subscriptions}")
        self._update_subscriptions()

    def _build_subscriptions(self, symbols, channels):
        """
        Constructs a set of subscription topics based on provided symbols and channels.
        Args:
            symbols (list): List of symbol strings.
            channels (list): List of channel/topic strings.
        Returns:
            set: Set of subscription topic strings.
        """
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
        """
        Handles switching the bot to a new market type.
        Closes the current WebSocket, clears state, and reconnects to the new market.
        Args:
            new_market (str): The new market type to switch to.
        """
        if new_market == self.market:
            self.logger.info(f"🔵 Market unchanged ({new_market}), no action taken.")
            return
        # Invalid market check
        if new_market not in cfg.WS_URL:
            self.logger.error(f"⚠️ Invalid market type: {new_market}")
            return  # Exit gracefully
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

    # =====================================================
    # Jericho: Subscription Management
    # =====================================================
    def _update_subscriptions(self):
        """
        Updates the WebSocket with the current set of subscriptions.
        Handles subscribing and unsubscribing in batches, and logs all changes.
        """
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
                self.logger.debug(f"Resetting sequence for symbol: {symbol}")
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

    # =====================================================
    # Jericho: WebSocket Connection
    # =====================================================
    def _connect_ws(self):
        """
        Establishes and maintains the WebSocket connection for the current market.
        Handles reconnection logic and triggers subscription updates on connect.
        """
        url = cfg.WS_URL[self.market] if self.market in cfg.WS_URL else cfg.WS_URL["spot"]
        def _runner():
            while not self.exit_evt.is_set():
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws: (self.logger.info("WS connected"), self._update_subscriptions()),
                    on_message=self._on_message,
                    on_error=lambda ws, err: self.logger.error(f"WS error: {err}"),
                    on_close=lambda *_: (self.logger.warning("WS closed"), self.channels.clear()),
                    on_pong=lambda *_: self.logger.debug("pong"),
                )
                self.ws.run_forever(ping_interval=PING_SEC, ping_timeout=PONG_TIMEOUT)
                if not self.exit_evt.is_set():
                    self.logger.warning(f"Reconnecting WS in {REOPEN_SEC}s...")
                    time.sleep(REOPEN_SEC)
        threading.Thread(target=_runner, daemon=True).start()

    # =====================================================
    # Jericho: Pending Subscription Flush
    # =====================================================
    def _flush_pending(self):
        """
        Sends any pending subscriptions to the WebSocket in batches.
        Handles connection errors gracefully and clears the pending list on success.
        """
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

    # =====================================================
    # Jericho: Watchdog & Heartbeat
    # =====================================================
    def _ws_watchdog(self):
        """
        Background thread that ensures the WebSocket is connected and flushes pending subscriptions.
        Runs every 5 seconds until the bot is stopped.
        """
        while not self.exit_evt.is_set():
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self._flush_pending()
            self.exit_evt.wait(5)

    # =====================================================
    # Jericho: WebSocket Message Handler
    # =====================================================
    def _on_message(self, _ws, raw: str):
        """
        Handles incoming WebSocket messages, parses topic, logs key events, and routes data to the MessageRouter.
        Increments kline counters and logs sequence gaps for debugging.
        Args:
            _ws: The WebSocketApp instance (unused).
            raw (str): Raw JSON message string from the WebSocket.
        """
        try:
            data = json.loads(raw)
            topic = data.get("topic", "")
            if "kline" in topic:
                _, interval, symbol = topic.split(".")
                # Only log at debug here; info-level log will be in MessageRouter.kline for confirmed klines
                self.logger.debug(f"KLINE  ¹ {symbol} {interval}")
                # --- Kline counter ---
                if not hasattr(self, "kline_count"):
                    self.kline_count = 0
                self.kline_count += 1
            elif "orderbook" in topic:
                _, depth, symbol = topic.split(".")
                self.logger.debug(f"ORDERBOOK  ¹ {symbol} depth {depth}")
            elif "publicTrade" in topic:
                _, symbol = topic.split(".")
                self.logger.debug(f"TRADE  ¹ {symbol}")
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
            self.logger.error(f"Parse fail: {exc}  ¹ first 120 chars: {raw[:120]}")

    def request_subscriptions_from_db(self):
        """
        Requests subscriptions from the database bot via Redis.
        """
        self.logger.info("Requesting subscriptions from the database bot...")
        payload = json.dumps({"action": "request_subscriptions", "owner": self.market})
        self.redis.publish(r_cfg.DB_REQUEST_SUBSCRIPTIONS, payload)

    def subscribe_to_db_subscriptions(self):
        """
        Subscribes to the subscriptions retrieved from the database bot.
        """
        self.logger.info("Subscribing to database subscriptions...")
        key = self._redis_key()
        saved_subscriptions = self.redis.smembers(key)
        if saved_subscriptions:
            self.subscriptions = set(saved_subscriptions)
            self._update_subscriptions()
            self.logger.info(f"Subscribed to {len(saved_subscriptions)} subscriptions from the database bot.")
        else:
            self.logger.warning("No subscriptions found in Redis.")




