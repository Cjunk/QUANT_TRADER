import sys, time, os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)
import websocket
import threading
import json, time, redis, datetime, os, requests
import logging
from websocket_bot.utils import (
    setup_logger, send_webhook, extract_symbol,
    extract_update_seq, is_snapshot
)
import config.config_ws as config
import config.config_redis as config_redis
from config.config_ws import HEARTBEAT_INTERVAL,LOG_FILENAME,LOG_LEVEL
from config.config_redis import KLINE_UPDATES,TRADE_CHANNEL,ORDER_BOOK_UPDATES


class WebSocketBot:
    def __init__(self):
        self.logger = setup_logger(config.LOG_FILENAME, getattr(logging, config.LOG_LEVEL.upper(), logging.WARNING))
        self.running = True
        self.redis_client = redis.Redis(
            host=config_redis.REDIS_HOST,
            port=config_redis.REDIS_PORT,
            db=config_redis.REDIS_DB,
            decode_responses=True
        )
        self.symbols = set()
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(config_redis.COIN_CHANNEL, config_redis.COIN_FEED_AUTO, config.BOT_NAME)
        self.last_trade_log_time = {}
        self.last_snapshot_time = {}
        self.last_sequences = {}
        self.ws_running = False

        send_webhook(config.WEBHOOK, f"WebSocket Bot <@{config.WEBSOCKET_USERID}> started...")
        self._start_self_heartbeat()

    def _start_self_heartbeat(self):
        def heartbeat():
            while self.running:
                payload = {
                    "bot_name": config.BOT_NAME,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat(),
                    "auth_token": config.BOT_AUTH_TOKEN
                }
                try:
                    self.redis_client.publish(config_redis.HEARTBEAT_CHANNEL, json.dumps(payload))
                except Exception as e:
                    self.logger.warning(f"Heartbeat failed: {e}")
                time.sleep(config.HEARTBEAT_INTERVAL)

        threading.Thread(target=heartbeat, daemon=True).start()

    def request_coin_list(self):
        payload = {
            "bot_name": "websocket_bot",
            "request": "resend_coin_list"
        }
        print(f"Requesting coin list: {payload}")
        self.redis_client.publish(config_redis.RESYNC_CHANNEL, json.dumps(payload))



    def start(self):
        self.logger.info("WebSocket Bot Starting...")
        print("WebSocket Bot Starting...")

        if not self.ws_running:
            threading.Thread(
                target=self._run_websocket,
                args=(config.SPOT_WEBSOCKET_URL, config.SUBSCRIPTIONS["spot"], "Spot"),
                daemon=True
            ).start()
            self.ws_running = True

        # Start listener before requesting coins
        time.sleep(1)  # give the listener thread a chance to subscribe
        self.request_coin_list()
        self.logger.info("Coin list request sent after WebSocket and listener ready")

        # Wait up to 15 seconds for coin list to arrive via pubsub
        for i in range(15):
            if self.symbols:
                self.logger.info(f"Coin list received: {self.symbols}")
                break
            time.sleep(1)
        else:
            self.logger.warning("⚠️ No coins received within timeout. Will listen for dynamic updates.")


    def _run_websocket(self, url, subs, name):
        ws = websocket.WebSocketApp(
            url,
            on_open=lambda ws: self._on_open(ws, subs, name),
            on_message=lambda ws, msg: self._on_message(ws, msg, name),
            on_error=lambda ws, err: self.logger.error(f"{name} WebSocket error: {err}"),
            on_close=lambda ws, *_: self.logger.warning("WebSocket disconnected.")
        )
        threading.Thread(target=self._listen_for_coin_updates, args=(ws,), daemon=True).start()
        ws.run_forever(ping_interval=30)

    def _on_open(self, ws, subs, name):
        self.subscribe_in_batches(ws)

    def _on_message(self, ws, message, name):
        try:
            data = json.loads(message)
            topic = data.get("topic", "")
            if "publicTrade" in topic:
                 self._process_trade(data)
            elif "orderbook" in topic:
                self._process_order_book(data)
            elif "kline" in topic:
                candle = data["data"][0]
                confirm_flag = candle["confirm"]
                if confirm_flag:
                    interval = candle["interval"]
                    volume = candle["volume"]
                    turnover = candle["turnover"]
                    start_dt = datetime.datetime.utcfromtimestamp(float(candle["start"]) / 1000.0)
                    symbol = topic.split(".")[-1]
                    
                    kline_data = {
                        "symbol": symbol,
                        "interval": interval,
                        "start_time": start_dt.isoformat(),
                        "open": candle["open"],
                        "close": candle["close"],
                        "high": candle["high"],
                        "low": candle["low"],
                        "volume": volume,
                        "turnover": turnover,
                        "confirmed": True
                    }
                    self.redis_client.publish(KLINE_UPDATES, json.dumps(kline_data))
                ...  # Process candles
        except Exception as e:
            self.logger.error(f"Failed to handle message: {e}")

    def _listen_for_coin_updates(self, ws):
        self.logger.info(" Listening for coin updates on COIN_CHANNEL...")
        for msg in self.pubsub.listen():
            if msg["type"] == "message":
                try:
                    self.logger.debug(f"Received coin update: {msg['data']}")
                    data = json.loads(msg["data"])
                    new_symbols = data.get("symbols", [])
                    if new_symbols:
                        self.symbols = set(new_symbols)
                        self.subscribe_in_batches(ws)
                        self.logger.info(f"Subscribed to symbols: {self.symbols}")
                except Exception as e:
                    self.logger.warning(f"Invalid symbol update: {e}")


    def subscribe_in_batches(self, ws, batch_size=10):
        channels = []
        for cat in config.SUBSCRIPTIONS["spot"].values():
            channels.extend(cat)
        for i in range(0, len(channels), batch_size):
            ws.send(json.dumps({"op": "subscribe", "args": channels[i:i + batch_size]}))

    def stop(self):
        self.running = False
        send_webhook(config.WEBHOOK, "WebSocket Bot has stopped.")