"""
    High Performance Websocket Bot for Byebit Exchange
    This bot is designed to handle high-frequency trading data and publish it to Redis.
    It subscribes to multiple channels and processes messages in real-time.     

    It also handles reconnections and resubscriptions automatically.
    The bot is designed to be run as a standalone script and can be easily integrated into other systems.   

    WORK FLOW 27/04/2025:
    1. subscribe to redis channels COIN_CHANNEL, COIN_FEED_AUTO, BOT_NAME

    Written by: Jericho Sharman

"""


import sys, time, os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))   # Synch folder path
sys.path.insert(0, project_root)
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')  # CLI encoding for Windows

# Import necessary libraries
import websocket,threading

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
        self.subscribed_channels = set()  # ✅ Track which channels we are subscribed to

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
        retries = 15
        while retries > 0 and not self.symbols:
            self.logger.info(f" Waiting for coin list... ({retries}s left)")
            time.sleep(1)
            retries -= 1
        if not self.symbols:
            self.logger.error(" No coin list received. Exiting.")
            sys.exit(1)
        self.logger.info(f" Coin list received: {self.symbols}")
    def _run_websocket(self, url, subs, name):
        while self.running:
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws: self._on_open(ws, subs, name),
                    on_message=lambda ws, msg: self._on_message(ws, msg, name),
                    on_error=lambda ws, err: self.logger.error(f"{name} WebSocket error: {err}"),
                    on_close=lambda ws, *_: self.logger.warning("WebSocket disconnected.")
                )
                threading.Thread(target=self._listen_for_coin_updates, args=(ws,), daemon=True).start()
                ws.run_forever(ping_interval=30)
            except Exception as e:
                self.logger.error(f"🚨 WebSocket critical error: {e}. Retrying in 5 seconds...")
                time.sleep(5)

    def _on_open(self, ws, subs, name):
        self.subscribe_in_batches(ws)
    def _process_order_book(self, data):
        return 1
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
                    data = json.loads(msg["data"])
                    new_symbols = data.get("symbols", [])

                    if new_symbols:
                        new_symbols_set = set(new_symbols)

                        if self.subscribed_channels:
                            payload = {"op": "unsubscribe", "args": list(self.subscribed_channels)}
                            ws.send(json.dumps(payload))
                            self.logger.info(f" Unsubscribed {len(self.subscribed_channels)} channels.")
                            self.subscribed_channels.clear()

                        subscription_args = []
                        for symbol in new_symbols_set:
                            subscription_args.append(f"publicTrade.{symbol}")
                            subscription_args.append(f"orderbook.200.{symbol}")
                            subscription_args.append(f"kline.1.{symbol}")
                            subscription_args.append(f"kline.5.{symbol}")
                            subscription_args.append(f"kline.60.{symbol}")
                            subscription_args.append(f"kline.D.{symbol}")

                        for i in range(0, len(subscription_args), 10):
                            ws.send(json.dumps({"op": "subscribe", "args": subscription_args[i:i+10]}))

                        self.subscribed_channels.update(subscription_args)
                        self.symbols = new_symbols_set
                        self.logger.info(f" Resubscribed to {len(subscription_args)} channels for: {self.symbols}")

                except Exception as e:
                    self.logger.warning(f"Invalid symbol update: {e}")
    def _process_trade(self, data):
        try:
            trade = data["data"][-1]  # Latest trade
            symbol = trade["s"]
            price = float(trade["p"])
            volume = float(trade["v"])
            side = trade["S"]  # Buy or Sell
            trade_time = int(trade["T"]) / 1000  # ms to seconds

            trade_data = {
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "side": side,
                "trade_time": datetime.datetime.utcfromtimestamp(trade_time).isoformat()
            }
            self.redis_client.publish(TRADE_CHANNEL, json.dumps(trade_data))
            #self.logger.debug(f" Published trade for {symbol}: {price} x {volume} ({side})")

        except Exception as e:
            self.logger.error(f"Failed to process trade message: {e}")

    def subscribe_in_batches(self, ws, batch_size=10):
        if not self.symbols:
            self.logger.warning(" No symbols to subscribe to.")
            return

        subscription_args = []
        for symbol in self.symbols:
            subscription_args.append(f"publicTrade.{symbol}")
            subscription_args.append(f"orderbook.200.{symbol}")
            subscription_args.append(f"kline.1.{symbol}")
            subscription_args.append(f"kline.5.{symbol}")
            subscription_args.append(f"kline.60.{symbol}")
            subscription_args.append(f"kline.D.{symbol}")

        for i in range(0, len(subscription_args), batch_size):
            payload = {"op": "subscribe", "args": subscription_args[i:i+batch_size]}
            ws.send(json.dumps(payload))
            self.logger.debug(f"🛰️ Sent subscription batch: {payload}")

        self.subscribed_channels.update(subscription_args)
        self.logger.info(f"✅ Initial subscription done for {len(subscription_args)} channels.")



    def stop(self):
        self.running = False
        send_webhook(config.WEBHOOK, "WebSocket Bot has stopped.")