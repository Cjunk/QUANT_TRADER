import websocket
import threading
import json
import redis
import time
import logging
import requests
import datetime
import config.config_ws as config
from utils.logger import setup_logger

class WebSocketBot:
    """📡 High-Performance WebSocket Bot for Bybit (Quant Trading Ready)."""

    def __init__(self):
        self.logger = setup_logger(config.LOG_FILENAME)
        self.running = True  # Control flag
        self.last_trade_log_time = {}  # Track last trade log per symbol
        self.redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True
        )
        self.last_snapshot_time = {}
        self.webhook = config.WEBHOOK
        self.userid = '360612543443501056' 
        self.send_webhook(f"Websocket bot has <@{self.userid}> started .....")
        self.symbols = set()  # Track currently subscribed symbols
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(config.COIN_CHANNEL)  # ✅ Listen for coin updates
        print("✅ WebSocket Bot is ready and listening for coin updates...")
    def fetch_order_book_snapshot(self, symbol):
        """📸 Fetches full depth order book snapshot from Bybit API."""
        url = "https://api.bybit.com/v5/market/orderbook"
        params = {"category": "spot", "symbol": symbol, "limit": 200}  # ✅  levels

        try:
            response = requests.get(url, params=params)
            data = response.json()
            if "result" in data and "b" in data["result"] and "a" in data["result"]:
                bids = [(float(bid[0]), float(bid[1])) for bid in data["result"]["b"]]
                asks = [(float(ask[0]), float(ask[1])) for ask in data["result"]["a"]]

                # ✅ Store full snapshot in Redis
                self.redis_client.delete(f"orderbook:{symbol}:bids")
                self.redis_client.delete(f"orderbook:{symbol}:asks")
                for price, volume in bids:
                    self.redis_client.zadd(f"orderbook:{symbol}:bids", {str(price): volume})
                for price, volume in asks:
                    self.redis_client.zadd(f"orderbook:{symbol}:asks", {str(price): volume})
                
                self.last_snapshot_time[symbol] = time.time()  # record snapshot time
            else:
                self.logger.warning(f"⚠️ Failed to fetch order book snapshot for {symbol}")
        except Exception as e:
            self.logger.error(f"❌ Error fetching order book snapshot for {symbol}: {e}")
    def update_subscriptions(self, new_symbols):
        """
        Updates `config.SUBSCRIPTIONS` dynamically with new symbols.
        """
        config.SUBSCRIPTIONS["spot"] = {
            "trades": [f"publicTrade.{symbol}" for symbol in new_symbols],
            "orderbook": [f"orderbook.{config.ORDER_BOOK_DEPTH}.{symbol}" for symbol in new_symbols],
            "kline": (
                [f"kline.1.{symbol}" for symbol in new_symbols] +
                [f"kline.5.{symbol}" for symbol in new_symbols] +
                [f"kline.60.{symbol}" for symbol in new_symbols] +
                [f"kline.D.{symbol}" for symbol in new_symbols]
            ),
        }
        self.logger.info(f"🔄 Updated subscriptions for symbols: {new_symbols}")

    def unsubscribe_all(self, ws):
        """
        Unsubscribes from all currently active WebSocket channels.
        """
        if not self.symbols:
            self.logger.info("⚠️ No active subscriptions to remove.")
            return

        channels = []
        for key, channel_list in config.SUBSCRIPTIONS["spot"].items():
            channels.extend(channel_list)

        if channels:
            msg = {"op": "unsubscribe", "args": channels}
            try:
                ws.send(json.dumps(msg))
                self.logger.info(f"🔴 Unsubscribed from: {', '.join(self.symbols)}")
            except Exception as e:
                self.logger.error(f"❌ Failed to unsubscribe: {e}")

        self.symbols.clear()
    def _on_message(self, ws, message, name):
        """📥 Handle incoming WebSocket messages."""
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
                    self.redis_client.publish("kline_updates", json.dumps(kline_data))

        except json.JSONDecodeError:
            self.logger.error(f"❌ Failed to decode {name} WebSocket message.")
    def _process_trade(self, data):
        """🔄 Process trade data (Efficient storage)."""
        trade = data["data"][-1]
        symbol, price, volume, trade_time = (
            trade["s"],
            float(trade["p"]),
            float(trade["v"]),
            int(trade["T"]) / 1000
        )
        system_time = time.time()
        delay = max(0, system_time - trade_time)  # Prevent negative delay
        if delay > 5:
            return  # Skip stale trades

        # ✅ Store latest price
        self.redis_client.set(f"latest_trade:{symbol}", price)

        # ✅ Store recent trades (LPUSH the last 100)
        self.redis_client.lpush(
            f"trades:{symbol}",
            json.dumps({"price": price, "volume": volume, "time": trade_time})
        )
        self.redis_client.ltrim(f"trades:{symbol}", 0, 999)
        # ✅ Publish trade event for PostgreSQL service bot
        self.redis_client.publish(f"trade_channel", json.dumps({"symbol": symbol, "trade": data}))

        # ✅ Log only every 10 seconds per symbol
        #if system_time - self.last_trade_log_time.get(symbol, 0) >= config.LOG_TRADE_PRICE_PERIOD:
            #self.logger.info(f"🟢 {symbol} Trade | Price: {price:.2f} | Delay: {delay:.2f}s")
            #self.last_trade_log_time[symbol] = system_time

    def update_order_book(self, symbol, bids, asks):
        """🔄 Incrementally update order book in Redis without full deletion."""

        redis_key_bids = f"orderbook:{symbol}:bids"
        redis_key_asks = f"orderbook:{symbol}:asks"
        if not bids and not asks:
            return  # No updates to process

        # Fetch only top 20 for efficiency (modify as needed)
        existing_bids = {
            float(price): float(vol)
            for price, vol in self.redis_client.zrevrange(redis_key_bids, 0, 19, withscores=True)
        }
        existing_asks = {
            float(price): float(vol)
            for price, vol in self.redis_client.zrange(redis_key_asks, 0, 19, withscores=True)
        }

        # Only modify existing bids/asks instead of full delete
        pipeline = self.redis_client.pipeline()

        for price, volume in bids:
            price = round(float(price), 8)  # Ensure precision
            volume = round(float(volume), 8)
            if volume > 0:
                pipeline.zadd(redis_key_bids, {str(price): volume})
            else:
                pipeline.zrem(redis_key_bids, str(price))  # Remove zero volume bids
        pipeline.zremrangebyscore(redis_key_asks, "-inf", "inf")  # 🚀 Clear all asks

        for price, volume in asks:
            price = round(float(price), 8)  # Ensure precision
            volume = round(float(volume), 8)
            if volume > 0:
                pipeline.zadd(redis_key_asks, {str(price): volume})
            else:
                pipeline.zrem(redis_key_asks, str(price))  # Remove zero volume asks

        if pipeline.command_stack:
            pipeline.execute()
            print(f"[DEBUG] ✅ Successfully wrote to Redis for {symbol}")  # 🚀 Ensure pipeline ran

        # Verify the updated state in Redis
        updated_bids = self.redis_client.zrevrange(redis_key_bids, 0, 9, withscores=True)
        updated_asks = sorted(
            self.redis_client.zrange(redis_key_asks, 0, 9, withscores=True),
            key=lambda x: float(x[0])
            )

        print(f"\n[DEBUG] Updated Bids in Redis for {symbol}: {updated_bids[:5]}")
        print(f"[DEBUG] Updated Asks in Redis for {symbol}: {updated_asks[:5]}")
        
    def _process_order_book(self, data):
        """🔄 Handle incremental order book updates and log debugging info."""
        raw_topic = data.get("topic", "")
        #self.logger.debug(f"Received order book update. Raw topic: {raw_topic}")
        # Expect topic format like "orderbook.200.BTCUSDT" or "orderbook.200.SOLUSDT"
        symbol = raw_topic.split(".")[-1]
        ob = data.get("data", {})
        if not ob.get("b") or not ob.get("a"):
            self.logger.warning(f"No bids or asks found in update for {symbol}: {data}")
            return

        # Update Redis order book
            # 🔍 Debug raw incoming data **before processing**
        #if symbol == "BTCUSDT":
            #print(f"\n[DEBUG] Raw Order Book Update for {symbol}: {ob}")
        #self.update_order_book(symbol, ob["b"], ob["a"])
        #self.logger.info(f"[{symbol}] Incoming Order Book Update:")
        #self.logger.info(f"Bids: {ob['b'][:5]} | Asks: {ob['a'][:5]}")
        self.update_order_book(symbol, sorted(ob["b"], key=lambda x: float(x[0]), reverse=True),
                                sorted(ob["a"], key=lambda x: float(x[0])))

        # Log top levels periodically (every 30 seconds)
        if time.time() - self.last_trade_log_time.get(symbol, 0) >= 10:
            redis_key_bids = f"orderbook:{symbol}:bids"
            redis_key_asks = f"orderbook:{symbol}:asks"
            top_bid = self.redis_client.zrange(redis_key_bids, -1, -1, withscores=True)
            top_ask = self.redis_client.zrange(redis_key_asks, 0, 0, withscores=True)
            self.last_trade_log_time[symbol] = time.time()

        # Refresh snapshot every 60 seconds to avoid stale data.
        if (time.time() - self.last_snapshot_time.get(symbol, 0)) > config.REFRESH_ORDER_BOOK_SNAPSHOT_PERIOD:
            self.fetch_order_book_snapshot(symbol)


    def subscribe_in_batches(self, ws, batch_size=10):
        """
        Subscribes to WebSocket channels in batches.
        """
        channels = []
        for key, channel_list in config.SUBSCRIPTIONS["spot"].items():
            channels.extend(channel_list)

        self.logger.info(f"🟢 Subscribing to {len(channels)} channels.")

        # Send subscribe messages in batches.
        for i in range(0, len(channels), batch_size):
            batch = channels[i:i + batch_size]
            msg = {"op": "subscribe", "args": batch}
            try:
                ws.send(json.dumps(msg))
                self.logger.info(f"✅ Subscribed to batch: {batch}")
            except Exception as e:
                self.logger.error(f"❌ Failed to subscribe: {e}")

    def listen_for_coin_updates(self, ws):
        """
        Continuously listens to Redis for updated coin lists and updates subscriptions dynamically.
        """
        self.logger.info("🎧 Listening for coin updates from Redis...")
        for message in self.pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])  # Expecting JSON data
                    new_symbols = data.get("symbols", [])

                    if new_symbols:
                        self.logger.info(f"📡 Received new coin list: {new_symbols}")
                        self.unsubscribe_all(ws)  # Unsubscribe from old coins
                        self.update_subscriptions(new_symbols)  # Update the config
                        self.subscribe_in_batches(ws)  # Subscribe to new ones
                        self.symbols = set(new_symbols)  # Update tracking set
                        self.logger.info(f"🚀 Updated subscriptions: {self.symbols}")
                    else:
                        self.logger.warning("⚠️ No valid symbols found in update.")

                except json.JSONDecodeError:
                    self.logger.error(f"❌ Invalid JSON received: {message['data']}")

    def start(self):
        """🚀 Start WebSocket connections."""
        self.logger.info(f"✅ WebSocket Bot Started, waiting for coin updates...")

        for url, subs, name in [
            (config.SPOT_WEBSOCKET_URL, config.SUBSCRIPTIONS["spot"], "Spot")
        ]:
            threading.Thread(
                target=self._run_websocket,
                args=(url, subs, name),
                daemon=True
            ).start()

    def _run_websocket(self, url, subs, name):
        """🔄 Handle WebSocket connection."""
        while self.running:
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda ws: self._on_open(ws, subs, name),
                    on_message=lambda ws, msg: self._on_message(ws, msg, name),
                    on_error=lambda ws, err: self.logger.error(f"❌ {name} WebSocket error: {err}"),
                    on_close=lambda ws, *_: self.logger.warning("🔴 WebSocket disconnected. Reconnecting...")
                )

                # ✅ Start Redis Listener in a separate thread
                threading.Thread(
                    target=self.listen_for_coin_updates,
                    args=(ws,),
                    daemon=True
                ).start()

                ws.run_forever(ping_interval=30)

            except Exception as e:
                self.logger.error(f"❌ {name} WebSocket error: {e}")
                time.sleep(config.RECONNECT_DELAY)

    def _on_open(self, ws, subs, name):
        """📡 Subscribe to WebSocket feeds initially."""
        self.subscribe_in_batches(ws)
        self.logger.info(f"✅ Subscribed to Bybit {name} WebSocket feeds!")

    def send_webhook(self, message):
        data = {
            "content": message,
            "username": "Webby01",
        }
        response = requests.post(self.webhook, json=data)
        if response.status_code == 204:
            self.logger.info("✅ Discord Message sent successfully!")
        else:
            self.logger.error(f"❌ Failed to send Discord message: {response.text}")

    def stop(self):
        """⏹ Stop the WebSocket bot."""
        self.running = False
        self.send_webhook("Websocket bot has stopped.")        
        self.logger.info("🛑 WebSocket Bot Stopped.")




