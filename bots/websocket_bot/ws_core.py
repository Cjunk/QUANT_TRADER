# ws_core.py
# --------------------------------------------------------------------------- #
#  WebSocket Bot – queue-driven, single-socket version                         #
# --------------------------------------------------------------------------- #
import json, threading, queue, time, signal, datetime, logging
import websocket
from websocket_bot import config_websocket_bot as cfg
from config        import config_redis        as r_cfg
from websocket_bot.subscription_handler       import SubscriptionHandler
from websocket_bot.message_router             import MessageRouter
from websocket_utils                          import send_webhook
from utils.logger                             import setup_logger
from utils.redis_client                       import get_redis
# --------------------------------------------------------------------------- #

BATCH_SIZE   = getattr(cfg, "BATCH_SIZE", 10)
ORDER_DEPTH  = getattr(cfg, "ORDER_BOOK_DEPTH", 200)
PING_SEC     = 20
PONG_TIMEOUT = 10
REOPEN_SEC   = 2

# --------------------------------------------------------------------------- #
class WebSocketBot:
    """
    * SubscriptionHandler pushes commands into `cmd_q` (set/add/remove symbols)
    * This class (re)subscribes on demand and streams raw Bybit messages
      to MessageRouter → Redis.
    """

    # ------------------------------------------------------------------ init
    def __init__(self):
        self.logger  = setup_logger("ws_core.py",
                                    getattr(logging, cfg.LOG_LEVEL.upper()))
        self.redis   = get_redis()

        # ─ runtime state
        self.cmd_q        = queue.Queue()
        self.channels     = set()         # active topics on remote stream
        self.pending_args = []            # topics queued until WS ready
        self.symbols      = set()
        self.market       = "spot"        # spot / linear / inverse
        self.ws           = None          # websocket.WebSocketApp
        self.exit_evt     = threading.Event()

        self.router       = MessageRouter(self.redis)

        # ─ threads
        threading.Thread(target=self._heartbeat,   daemon=True).start()
        threading.Thread(target=self._ws_watchdog, daemon=True).start()

        self.sub_handler = SubscriptionHandler(self.redis, self.cmd_q)
        self.sub_handler.start()

        signal.signal(signal.SIGINT, lambda *_: self.stop())  # Ctrl-C clean exit

    # ===================================================== public ===========
    def run(self):
        send_webhook(cfg.DISCORD_WEBHOOK,
                     f"WebSocket Bot <@{cfg.DISCORD_USER_ID}> started …")
        self.logger.info("Booted – waiting for subscription commands.")

        while not self.exit_evt.is_set():
            try:
                cmd = self.cmd_q.get(timeout=0.5)
                self._handle_command(cmd)
            except queue.Empty:
                continue

    def stop(self):
        if self.exit_evt.is_set():
            return
        self.logger.info("🛑 Shutting down WebSocketBot …")
        self.exit_evt.set()

        # close WS
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

        # stop listener
        try:
            self.sub_handler.stop()
        except Exception:
            pass

        # final status
        self.redis.publish(
            r_cfg.SERVICE_STATUS_CHANNEL,
            json.dumps({
                "bot_name" : cfg.BOT_NAME,
                "status"   : "stopped",
                "time"     : datetime.datetime.utcnow().isoformat(),
                "auth_token": cfg.BOT_AUTH_TOKEN,
            })
        )
        send_webhook(cfg.DISCORD_WEBHOOK,
                     f"WebSocket Bot **{cfg.BOT_NAME}** stopped.")
        self.logger.info("✅ Shutdown complete.")

    # =================================================== internal ===========
    # ---------------------------------------------------------------- handle
    def _handle_command(self, cmd: dict):
        action  = cmd["action"]           # set / add / remove
        market  = cmd.get("market", "spot")
        symbols = set(cmd["symbols"])

        # reconnect if market switched
        if market != self.market:
            self._disconnect_ws()
            self.market = market
            self._spawn_ws_thread()

        # compute new symbol set
        if action == "set":
            new_syms = symbols
        elif action == "add":
            new_syms = self.symbols | symbols
        elif action == "remove":
            new_syms = self.symbols - symbols
        else:
            self.logger.warning("Unknown action %s", action)
            return

        # apply if changed
        if new_syms != self.symbols:
            self._resubscribe(new_syms, action)
        else:
            self.logger.info("No symbol change.")

    # ---------------------------------------------------------------- resub
    # --- inside class WebSocketBot ---------------------------------------------
    def _resubscribe(self, new_syms: set, action: str):
        # 🔹 ensure we have a live socket first
        if not self._ws_ready():
            self._spawn_ws_thread()          # <-- start WS thread immediately

        # ---------- build topic list -------------------------------------------
        args = []
        for sym in new_syms:
            args.extend([
                f"publicTrade.{sym}",
                f"orderbook.{ORDER_DEPTH}.{sym}",
                f"kline.1.{sym}", f"kline.5.{sym}",
                f"kline.60.{sym}", f"kline.D.{sym}",
            ])
        self.pending_args = args            # queue until _flush_pending()

        # flush right away if socket is already connected
        if self._ws_ready():
            self._flush_pending()

        # bookkeeping / logging
        self.symbols = new_syms
        self.logger.info(
            "🛰️  action=%s market=%s sym=%s  sub=%d",
            action, self.market, sorted(new_syms), len(args)
        )

    # ---------------------------------------------------------------- socket
    def _spawn_ws_thread(self):
        if self._ws_ready():
            return

        def _runner():
            url = cfg.WS_URL[self.market]
            while not self.exit_evt.is_set():
                self.logger.info("Connecting → %s", url)

                self.ws = websocket.WebSocketApp(
                    url,
                    on_open    = lambda ws: (self.logger.info("Connected"),
                                             self._flush_pending()),
                    on_close   = lambda *_: self.logger.warning("WS closed"),
                    on_error   = lambda ws, err: self.logger.error("WS error: %s", err),
                    on_message = self._on_message,
                    on_pong    = lambda ws, msg=None: self.logger.debug("⏱ pong"),
                )

                self.ws.run_forever(ping_interval=PING_SEC,
                                    ping_timeout=PONG_TIMEOUT)

                if self.exit_evt.is_set():
                    break
                self.logger.warning("WS lost – reconnecting in %s s …", REOPEN_SEC)
                time.sleep(REOPEN_SEC)

        threading.Thread(target=_runner, daemon=True).start()

    def _disconnect_ws(self):
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        self.ws = None
        self.channels.clear()

    def _ws_ready(self) -> bool:
        return self.ws and self.ws.sock and self.ws.sock.connected

    # ---------------------------------------------------------------- flush
    def _flush_pending(self):
        if not self.pending_args or not self._ws_ready():
            return
        self.logger.info("Sending %d subscribe topics …", len(self.pending_args))
        for i in range(0, len(self.pending_args), BATCH_SIZE):
            batch = self.pending_args[i:i+BATCH_SIZE]
            self.ws.send(json.dumps({"op": "subscribe", "args": batch}))
            self.logger.debug("SUB-batch %s", batch)
        self.channels.update(self.pending_args)
        self.pending_args = []

    # ---------------------------------------------------------------- watch
    def _ws_watchdog(self):
        while not self.exit_evt.is_set():
            if self._ws_ready():
                self._flush_pending()
            self.exit_evt.wait(5)

    # ---------------------------------------------------------------- heart
    def _heartbeat(self):
        while not self.exit_evt.is_set():
            try:
                self.redis.publish(
                    r_cfg.HEARTBEAT_CHANNEL,
                    json.dumps({
                        "bot_name": cfg.BOT_NAME,
                        "heartbeat": True,
                        "time": datetime.datetime.utcnow().isoformat(),
                        "auth_token": cfg.BOT_AUTH_TOKEN,
                    })
                )
            except Exception as e:
                self.logger.warning("heartbeat failed: %s", e)
            self.exit_evt.wait(cfg.HEARTBEAT_INTERVAL)

    # ---------------------------------------------------------------- route
    def _on_message(self, _ws, raw: str):
        try:
            data  = json.loads(raw)
            topic = data.get("topic", "")
            if   "publicTrade" in topic: self.router.trade(data)
            elif "kline"       in topic: self.router.kline(data)
            elif "orderbook"   in topic: self.router.orderbook(data)
        except Exception as exc:
            self.logger.error("Parse fail: %s – first 120 chars: %s…",
                              exc, raw[:120])

