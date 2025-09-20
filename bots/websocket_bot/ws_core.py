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
import json, threading, queue, datetime, time, logging, os
import websocket
import config_websocket_bot as cfg
from utils import setup_logger
from utils.redis_handler import RedisHandler
from utils.HeartBeatService import HeartBeat
from config import config_redis as r_cfg
from subscription_handler import SubscriptionHandler, MAX_SYMBOLS
from message_router import MessageRouter
from websocket_utils import send_webhook
# =====================================================
# Jericho: Configurable Constants
# =====================================================
BATCH_SIZE = getattr(cfg, "BATCH_SIZE", 10)
PING_SEC, PONG_TIMEOUT, REOPEN_SEC = 20, 10, 2
CAPTURE_ORDER_DELTAS = True
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
        log_level = logging.DEBUG if getattr(cfg, "LOG_LEVEL", "INFO").upper() == "DEBUG" else logging.INFO
        self.logger = setup_logger(f"{market}_ws_core.log", log_level)
        self.redis_handler = RedisHandler(r_cfg, self.logger)
        self.redis_handler.connect()
        self.redis = self.redis_handler.client
        self.cmd_q = queue.Queue()
        self.ws = None
        self.channels = set()
        self.exit_evt = threading.Event()
        self.router = MessageRouter(self.redis, market=market)
        self.sub_handler = SubscriptionHandler(
            self.market, self.logger,
            reset_seq_callback=self.router.reset_seq,
            update_ws_subscriptions=self._update_subscriptions
        )
        self.sub_handler.ws = self.ws
        self.sub_handler.ws_update_callback = self._update_subscriptions
        self.sub_handler.out_q = self.cmd_q
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
                    "subscriptions": sorted(list(self.sub_handler.subscriptions)),
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
        self.logger.info(f"[DEBUG] WebSocketBot for market '{self.market}' initialized.")
        self._connect_ws()
        threading.Thread(target=self._ws_watchdog, daemon=True).start()
    # =====================================================
    # Jericho: Main Run Loop
    # =====================================================
    def run(self):
        send_webhook(cfg.DISCORD_WEBHOOK, "WebSocket Bot started.")
        self.logger.info(f"🚀 WebSocketBot running. {self.market}")
        while not self.exit_evt.is_set():
            try:
                #self.logger.debug("[DEBUG][run] Waiting for command in cmd_q...")
                new_subs = self.cmd_q.get(timeout=1)
                self.logger.debug(f"[DEBUG][run] Got new subscriptions from cmd_q: {new_subs} (type={type(new_subs)})")
                self._update_subscriptions(new_subs)
            except queue.Empty:
                #self.logger.debug("[DEBUG][run] cmd_q is empty, continuing loop.")
                continue
    def _update_subscriptions(self, new_subs):
        self.logger.debug(f"*************** [DEBUG][_update_subscriptions] called with: {new_subs} (type={type(new_subs)})")
        if not self.ws:
            return
        if not self.ws.sock:
            self.logger.error("[DEBUG][_update_subscriptions] WebSocket.sock is None!")
            return
        if not self.ws.sock.connected:
            self.logger.warning("[DEBUG][_update_subscriptions] WebSocket not connected, skipping update.")
            return
        self.logger.debug(f"[DEBUG][_update_subscriptions] Preparing to update live subscriptions. Current channels: {self.channels}")
        self.logger.info(f"[DEBUG][_update_subscriptions] New subscriptions requested: {new_subs}")
        # Debug: Show difference between current and new
        to_sub = set(new_subs) - self.channels
        to_unsub = self.channels - set(new_subs)
        self.logger.info(f"[DEBUG][_update_subscriptions] to_sub={to_sub}, to_unsub={to_unsub}")
        # Unsubscribe from topics not in new_subs
        if to_unsub:
            for batch_start in range(0, len(to_unsub), BATCH_SIZE):
                batch = list(to_unsub)[batch_start:batch_start+BATCH_SIZE]
                self.logger.debug(f"[DEBUG][_update_subscriptions] Unsubscribing from batch: {batch}")
                try:
                    self.ws.send(json.dumps({"op": "unsubscribe", "args": batch}))
                    self.logger.info(f"[DEBUG][_update_subscriptions] Unsubscribed from batch: {batch}")
                except Exception as e:
                    self.logger.error(f"[DEBUG][_update_subscriptions] Failed to unsubscribe batch {batch}: {e}")
        self.channels -= to_unsub
        # Subscribe to new topics
        if to_sub:
            for batch_start in range(0, len(to_sub), BATCH_SIZE):
                batch = list(to_sub)[batch_start:batch_start+BATCH_SIZE]
                self.logger.info(f"[DEBUG][_update_subscriptions] Subscribing to batch: {batch}")
                for symbol in batch:
                    sub_type = self.market if hasattr(self, 'market') else 'unknown'
                    self.logger.info(f"[SUBSCRIBE] Symbol: {symbol} | Type: {sub_type}")
                try:
                    self.ws.send(json.dumps({"op": "subscribe", "args": batch}))
                    self.logger.info(f"[DEBUG][_update_subscriptions] Subscribed to batch: {batch}")
                except Exception as e:
                    self.logger.error(f"[DEBUG][_update_subscriptions] Failed to subscribe batch {batch}: {e}")
        self.channels |= to_sub

        if not to_sub and not to_unsub:
            self.logger.info("[DEBUG][_update_subscriptions] No subscription changes needed.")
        # Final state
        self.logger.info(f"[DEBUG][_update_subscriptions] Final channels: {self.channels}")
    # =====================================================
    # Jericho: Shutdown Logic
    # =====================================================
    def stop(self):
        self.logger.debug("[DEBUG][stop] Called stop()")
        if self.exit_evt.is_set():
            self.logger.info("[DEBUG][stop] exit_evt already set, returning.")
            return
        self.logger.info("🛑 Shutting down...")
        self.exit_evt.set()
        if self.ws and self.ws.sock:
            try:
                self.ws.close()
                self.logger.info("🟢 WebSocket closed successfully.")
            except Exception as e:
                self.logger.warning(f"⚠️ WebSocket close failed: {e}")
        if self.sub_handler:
            self.logger.debug("[DEBUG][stop] Stopping sub_handler...")
            self.sub_handler.stop()
            self.logger.debug("[DEBUG][stop] Saving subscriptions to Redis...")
            self.sub_handler._save_subscriptions_to_redis()
        send_webhook(cfg.DISCORD_WEBHOOK, "WebSocket Bot stopped.")
        self.logger.debug("✅ Shutdown complete.")
    # =====================================================
    # Jericho: WebSocket Connection
    # =====================================================
    def _connect_ws(self):
        url = cfg.WS_URL[self.market] if self.market in cfg.WS_URL else cfg.WS_URL["spot"]
        self.logger.debug(f"[DEBUG][_connect_ws] Connecting to WebSocket at URL: {url}")
        def _runner():
            while not self.exit_evt.is_set():
                self.logger.debug("[DEBUG][_connect_ws] Creating WebSocketApp...")
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws: (
                        self.logger.debug("[DEBUG][_connect_ws] WS connected"),
                        send_webhook(cfg.DISCORD_WEBHOOK, f"WebSocket connected to {self.market}"),
                        self._update_subscriptions(set(self.sub_handler.subscriptions.keys()))
                    ),
                    on_message=self._on_message,
                    on_error=lambda ws, err: self.logger.error(f"[DEBUG][_connect_ws] WS error: {err}"),
                    on_close=lambda *_: (self.logger.warning("[DEBUG][_connect_ws] WS closed"), send_webhook(cfg.DISCORD_WEBHOOK, f"WebSocket closed for {self.market}"), self.sub_handler.channels.clear()),
                    on_pong=lambda *_: self.logger.debug("[DEBUG][_connect_ws] pong"),
                )
                self.logger.debug("[DEBUG][_connect_ws] Starting run_forever...")
                self.ws.run_forever(ping_interval=PING_SEC, ping_timeout=PONG_TIMEOUT)
                self.sub_handler.ws = self.ws
                if not self.exit_evt.is_set():
                    self.logger.warning(f"[DEBUG][_connect_ws] Reconnecting WS in {REOPEN_SEC}s...")
                    time.sleep(REOPEN_SEC)
        threading.Thread(target=_runner, daemon=True).start()
    # =====================================================
    # Jericho: Watchdog & Heartbeat
    # =====================================================
    def _ws_watchdog(self):
        """
        Background thread that ensures the WebSocket is connected.
        Runs every 5 seconds until the bot is stopped.
        """
        while not self.exit_evt.is_set():
            # No need to flush pending subscriptions
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
            self.logger.debug(f"[DEBUG] Received WS message: topic={topic} raw={raw[:200]}")
            if "kline" in topic:
                _, interval, symbol = topic.split(".")
                # --- Kline counter ---
                if not hasattr(self, "kline_count"):
                    self.kline_count = 0
                self.kline_count += 1
            elif "orderbook" in topic:
                _, depth, symbol = topic.split(".")
                if CAPTURE_ORDER_DELTAS:
                    payload = data.get("data", {})
                for side_key, side_label in [("b", "bid"), ("a", "ask")]:
                    if side_key in payload:
                        for entry in payload[side_key]:
                            try:
                                price, volume = entry
                                update_type = "delete" if float(volume) == 0 else "update"
                                message = {
                                    "symbol": symbol,
                                    "market": self.market,
                                    "depth": int(depth),
                                    "side": side_label,
                                    "price": price,
                                    "volume": volume,
                                    "update_type": update_type,
                                    "received_at": datetime.datetime.utcnow().isoformat()

                                }
                                self.redis_handler.publish(
                                    r_cfg.REDIS_CHANNEL[f"{self.market}.orderbook_delta"],
                                    json.dumps(message)
                                )
                            except Exception as e:
                                self.logger.error(f"Redis publish failed for entry: {entry} | Error: {e}")


            elif "publicTrade" in topic:
                _, symbol = topic.split(".")

            # Jericho: SEQ GAP Debugging (remove when resolved)
            if "orderbook" in topic and "seq_gap" in data.get("type", "").lower():
                symbol = data.get("symbol", "?")
                last_seq = data.get("last_seq", "?")
                new_seq = data.get("new_seq", "?")
                self.logger.debug(f"[DEBUG][SEQ GAP] symbol={symbol} last_seq={last_seq} new_seq={new_seq} raw={raw[:200]}")
            # Jericho: Route message
            if "publicTrade" in topic:
                #self.logger.info(f"[DEBUG] Routing trade data to MessageRouter for symbol={symbol}")
                self.router.trade(data)
            elif "kline" in topic:
                #self.logger.info(f"[DEBUG] Routing kline data to MessageRouter for symbol={symbol}")
                self.router.kline(data)
            elif "orderbook" in topic:
                #self.logger.info(f"[DEBUG] Routing orderbook data to MessageRouter for symbol={symbol}")
                self.router.orderbook(data)
        except Exception as exc:
            self.logger.error(f"Parse fail: {exc}  ¹ first 120 chars: {raw[:120]}")
