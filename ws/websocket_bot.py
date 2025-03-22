import websocket
import threading
import json,time,redis,datetime,os
import logging
import requests
import config.config_ws as config
import config.config_redis as config_redis
from utils.logger import setup_logger
"""
#TODOS
1   When  there is a new coin list detected it should clear the memory of the old coin list sequence numbers other wise it will detect a huge gap if that coin is later subscribed to again.

"""
class WebSocketBot:
    """📡 High-Performance WebSocket Bot for Bybit (Quant Trading Ready)."""
    """
    =-=-=-=-=-=-=- System bot functions in the running of the basic bot
    """
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
        self.userid = '360612543443501056' # Do not change as its specific for discord
        self.send_webhook(f"Websocket bot has <@{self.userid}> started .....")
        self.symbols = set()  # Track currently subscribed symbols
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.subscribe(config_redis.COIN_CHANNEL,config_redis.COIN_FEED_AUTO)  # ✅ Listen for coin updates
        self.last_sequences = {}
        self.status = {
                        "bot_name": "websocket_bot",
                        "status": "started",
                        "time": datetime.datetime.utcnow().isoformat(),
                        "auth_token": config.BOT_AUTH_TOKEN,  # each bot has its own
                        "metadata": {
                            "version": "1.2.0",
                            "pid": os.getpid(),
                            "strategy": "VWAP"
                        }
                    }
    def start(self):
        self._start()
    def stop(self):
        self._stop()
    def _start(self):
        """
        🚀 Launch WebSocket Bot
        - Registers bot status to Redis for PB tracking
        - Starts WebSocket listener threads
        - Waits for coin feed from PB Bot (manual or automatic)
        """
        
        # 🛰️ Notify PB bot that this bot is active
        self.redis_client.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(self.status))
        self.logger.info("✅ Published ACTIVE status to pb_bot (PostgreSQL Manager)")
        
        time.sleep(3)
        # If no coins have been received after X seconds, request resync
        if not self.symbols:
            self.logger.warning("⚠️ No coins received after startup — requesting resync...")
            self.request_coin_list()   

        # 👂 Wait for coin list before starting subscriptions
        self.logger.info("✅ WebSocket Bot Started — waiting for coin updates...")

        # 🚀 Launch WebSocket connection threads
        for url, subs, name in [
            (config.SPOT_WEBSOCKET_URL, config.SUBSCRIPTIONS["spot"], "Spot")
        ]:
            threading.Thread(
                target=self._run_websocket,
                args=(url, subs, name),
                daemon=True
            ).start()

    def _stop(self):
        """⏹ Stop the WebSocket bot."""
        self.status['status'] = 'stopped'
        self.redis_client.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(self.status))
        self.logger.info(f"✅ Published STOPPED status to postgresql bot....") 
        self.running = False
        self.send_webhook("Websocket bot has stopped.")        
        self.logger.info("🛑 WebSocket Bot Stopped.")            
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
                    target=self._listen_for_coin_updates,
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
 
    """
    =-=-=-=-=-=-=- Operations
    """
    def _listen_for_coin_updates(self, ws):
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
                        self.last_update_seq = {}
                    else:
                        self.logger.warning("⚠️ No valid symbols found in update.")

                except json.JSONDecodeError:
                    self.logger.error(f"❌ Invalid JSON received: {message['data']}")    
    def request_coin_list(self):
        """
        🆘 Fallback recovery: Ask pb_bot to resend current_coins
        (In case startup signal was missed or pb_bot crashed)
        """
        self.logger.info("📣 Requesting current coin list from pb_bot...")
        payload = {
            "bot_name": "websocket_bot",
            "request": "resend_coin_list"
        }
        self.redis_client.publish(config_redis.RESYNC_CHANNEL, json.dumps(payload))

 
    """
    =-=-=-=-=-=-=- Subscription Managment
    """     
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
    """
    =-=-=-=-=-=-=- Order book operations
    """  
    def _process_order_book(self, data):
        """
        High-level order book flow:
        1) Extract the symbol & sequence from 'data'.
        2) If it's a snapshot, overwrite entire order book.
        3) If it's a delta, do sequence checks and apply the update or re-sync.
        4) Periodically refresh snapshot to avoid drift.
        5) (Optional) Log top-level order book info.
        """
        #print("Entered the process order book function")
        symbol = data['data']['s']
        type = data['type']
        update_seq = self._extract_update_seq(data, symbol)
        #print(f'HERE IS YOUR SYMBOL {symbol} and type {type}')
        if type == 'snapshot':
            #print("This is the snapshot")
            #print(data['data'])
            self._handle_snapshot(symbol,data,0)
            return
        if symbol not in self.last_sequences or self.last_sequences[symbol] == 0:
                self.last_sequences[symbol] = update_seq
                
        else:   # Sequence is already in the list
                
                if update_seq is None:
                    self.logger.error("_process_order_book: update_seq is None. This is not meant to happen")
                    print(data)
                # DETECT GAP
                if update_seq > self.last_sequences[symbol] + 1:
                    print(f"GAP DETECTED  --{self.last_sequences[symbol]}--  GAP DETECTED ---GAP DETECTED  --{update_seq}--  GAP DETECTED --- ")
                    self.last_sequences[symbol] = update_seq 
                    self.fetch_order_book_snapshot(symbol)

                
                self._handle_delta(symbol, data, update_seq)
                self.last_sequences[symbol] = update_seq

        #print(f"{data}")
        #print(f"{data['type']}")
      
    def _process_order_book_BACKUP(self, data):
        """
        High-level order book flow:
        1) Extract the symbol & sequence from 'data'.
        2) If it's a snapshot, overwrite entire order book.
        3) If it's a delta, do sequence checks and apply the update or re-sync.
        4) Periodically refresh snapshot to avoid drift.
        5) (Optional) Log top-level order book info.
        """
        symbol = self._extract_symbol(data)
        # -- Grab the sequence number(s) from data --        
        update_seq = self._extract_update_seq(data, symbol)
        print(f"Update Sequence {update_seq}")
        # -- Determine if snapshot or delta --
        # ------------------------- Step 2) Snapshot or Delta ------------------------
        if self._is_snapshot(data):
            # Overwrite entire book
            self._handle_snapshot(symbol, data, update_seq)
        else:
            # Merge delta if sequence checks pass
            self._handle_delta(symbol, data, update_seq)

            # ------------------------- Step 3) Periodic Re-Sync ------------------------
                #if (time.time() - self.last_snapshot_time.get(symbol, 0)) > 777777:
                    #self.fetch_order_book_snapshot(symbol)
            # ------------------------- Step 4) Optional Log ----------------------------
        self._log_top_levels(symbol)

    def _overwrite_entire_order_book(self, symbol, bids, asks):
        """
        Completely remove the old bids/asks in Redis and then write the new snapshot.
        Use this only if 'bids'/'asks' contain the FULL current order book data.
        """
        self.logger.info (f"EXECUTUING FUNCTION __overwrite_entire_order_book for : {symbol}")
        redis_key_bids = f"orderbook:{symbol}:bids"
        redis_key_asks = f"orderbook:{symbol}:asks"

        # 1) Remove existing book
        self.redis_client.delete(redis_key_bids)
        self.redis_client.delete(redis_key_asks)

        # 2) Write new bids
        for price_str, volume_str in bids:
            price = round(float(price_str), 8)
            volume = float(volume_str)
            if volume > 0:
                self.redis_client.zadd(redis_key_bids, {str(price): volume})
        #print(f"\n ============================================= SNAPSHOT UPDATE: {symbol} New bids have now been written to redis bids\n")
        
        # 3) Write new asks
        for price_str, volume_str in asks:
            price = round(float(price_str), 8)
            volume = float(volume_str)
            if volume > 0:
                self.redis_client.zadd(redis_key_asks, {str(price): volume})
    def _update_order_book(self,symbol,bids,asks): #FIXME #TODO
        #print("Im not updating anything , no deltas")
        redis_key_bids = f"orderbook:{symbol}:bids"
        redis_key_asks = f"orderbook:{symbol}:asks"  
         # Fetch the *entire* existing order book from Redis, not just top 100
        # Trim the sorted set to keep only top 200 levels:
        #self.redis_client.zremrangebyrank(redis_key_bids, 199, -1) 
        #self.redis_client.zremrangebyrank(redis_key_asks, 199, -1)        
        # Now fetch the top 200 levels:
        existing_bids = {
            float(price): float(vol)
            for price, vol in self.redis_client.zrevrange(redis_key_bids, 0, 199, withscores=True)
        }
        existing_asks = {
            float(price): float(vol)
            for price, vol in self.redis_client.zrange(redis_key_asks, 0, 199, withscores=True)}          
        #print(f"EXISTING BIDS (From Redis){existing_bids}\n")
        #print(f"DELTA ASKS {asks}\n\n")
        #print(f"````````````````````````````````   EXISTING ASKS (From Redis) PRIOR to any DELTA UPDATES\n {existing_asks}\n")
                # --- Process Bids (merge deltas) ---
        for price_str, delta_str in bids:
            # Convert to float
            price = round(float(price_str), 8)
            delta = float(delta_str)
            old_volume = existing_bids.get(price, 0.0)
            new_volume = old_volume + delta

            if new_volume <= 0:
                # Remove from order book
                self.redis_client.zrem(redis_key_bids, str(price))
            else:
                # Update (ZADD stores 'new_volume' as the score)
                self.redis_client.zadd(redis_key_bids, {str(price): new_volume})
        # --- Process Asks (merge deltas) ---
        for price_str, delta_str in asks:
            price = round(float(price_str), 8)
            delta = float(delta_str)
            old_volume = existing_asks.get(price, 0.0)
            new_volume = old_volume + delta
            #print(f"HERE IS THE UPDATE: PRICE = {price}   old_volume {old_volume}  new_volume{new_volume}\n")
            if new_volume <= 0:
                self.redis_client.zrem(redis_key_asks, str(price))
            else:
                self.redis_client.zadd(redis_key_asks, {str(price): new_volume})
        self.redis_client.zremrangebyrank(redis_key_bids, 199, -1) 
        self.redis_client.zremrangebyrank(redis_key_asks, 199, -1) 
    def _update_order_book2(self, symbol, bids, asks):
        """
        🔄 Incrementally update order book in Redis by *merging* new volumes.
        If Bybit sends a delta of X at price P, we add X to the old volume in Redis.
        If the result <= 0, remove that price.
        """
        redis_key_bids = f"orderbook:{symbol}:bids"
        redis_key_asks = f"orderbook:{symbol}:asks"

        # Fetch the *entire* existing order book from Redis, not just top 100
        existing_bids = {
            float(price): float(vol)
            for price, vol in self.redis_client.zrevrange(redis_key_bids, 0, -1, withscores=True)
        }
        existing_asks = {
            float(price): float(vol)
            for price, vol in self.redis_client.zrange(redis_key_asks, 0, -1, withscores=True)
        }

        # --- Process Bids (merge deltas) ---
        for price_str, delta_str in bids:
            # Convert to float
            price = round(float(price_str), 8)
            delta = float(delta_str)
            old_volume = existing_bids.get(price, 0.0)
            new_volume = old_volume + delta

            if new_volume <= 0:
                # Remove from order book
                self.redis_client.zrem(redis_key_bids, str(price))
            else:
                # Update (ZADD stores 'new_volume' as the score)
                self.redis_client.zadd(redis_key_bids, {str(price): new_volume})

        # --- Process Asks (merge deltas) ---
        for price_str, delta_str in asks:
            price = round(float(price_str), 8)
            delta = float(delta_str)
            old_volume = existing_asks.get(price, 0.0)
            new_volume = old_volume + delta

            if new_volume <= 0:
                self.redis_client.zrem(redis_key_asks, str(price))
            else:
                self.redis_client.zadd(redis_key_asks, {str(price): new_volume})
    def fetch_order_book_snapshot(self, symbol):
        """📸 Fetch full depth order book snapshot from Bybit API (spot, v5)."""
        url = "https://api.bybit.com/v5/market/orderbook"
        params = {"category": "spot", "symbol": symbol, "limit": 200}

        try:
            response = requests.get(url, params=params)
            data = response.json()

            if "result" in data and "b" in data["result"] and "a" in data["result"]:
                bids = [(float(bid[0]), float(bid[1])) for bid in data["result"]["b"]]
                asks = [(float(ask[0]), float(ask[1])) for ask in data["result"]["a"]]

                # 1) Clear old data
                self.redis_client.delete(f"orderbook:{symbol}:bids")
                self.redis_client.delete(f"orderbook:{symbol}:asks")

                # 2) Write snapshot
                for price, volume in bids:
                    if volume > 0:
                        self.redis_client.zadd(f"orderbook:{symbol}:bids", {str(price): volume})
                for price, volume in asks:
                    if volume > 0:
                        self.redis_client.zadd(f"orderbook:{symbol}:asks", {str(price): volume})

                # 3) Because Bybit v5 REST doesn’t give a sequence, just reset it to 0 (or to some placeholder).
                self.last_update_seq[symbol] = 0
                self.last_snapshot_time[symbol] = time.time()

            else:
                self.logger.warning(f"⚠️ Failed to fetch order book snapshot for {symbol} - data: {data}")

        except Exception as e:
            self.logger.error(f"❌ Error fetching order book snapshot for {symbol}: {e}")
  
    """
    =-=-=-=-=-=-=- Trade operations
    """
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
        if system_time - self.last_trade_log_time.get(symbol, 0) >= config.LOG_TRADE_PRICE_PERIOD:
            #self.logger.info(f"🟢 {symbol} Trade | Price: {price:.2f} | Delay: {delay:.2f}s")
            self.last_trade_log_time[symbol] = system_time
    def send_webhook(self, message):
        #  This function to be replaced with a dedicated message / alert bot #TODO
        data = {
            "content": message,
            "username": "Webby01",
        }
        response = requests.post(self.webhook, json=data)
        if response.status_code == 204:
            self.logger.info("✅ Discord Message sent successfully!")
        else:
            self.logger.error(f"❌ Failed to send Discord message: {response.text}")
    # ----------------------------------------------------------------------
    #                   Helper Methods
    # ----------------------------------------------------------------------

    def _extract_symbol(self, data):
        """Pulls the symbol from the 'topic' field, e.g. 'orderbook.200.BTCUSDT'."""
        raw_topic = data.get("topic", "")
        symbol = raw_topic.split(".")[-1]
        return symbol

    def _extract_update_seq(self, data, symbol):
        """
        Bybit delta typically has 'u' for the sequence. 
        Return it or log a warning and return None if missing.
        """
        ob = data.get("data", {})
        update_seq = ob.get("u")
        if update_seq is None:
            self.logger.warning(f"No sequence found in update for {symbol}. Full data: {data}")
            return None        
        
        #if not ob.get("b") or not ob.get("a"):
            #self.logger.warning(f"No bids or asks found in update for {symbol}: {data}")
            #return None
        return update_seq

    def _is_snapshot(self, data):
        """
        Bybit often sets 'type': 'snapshot' or 'action': 'snapshot' for a full snapshot.
        Returns True if that's the case.
        """
        ob = data.get("data", {})
        message_type = data.get("type") or ob.get("action")
        return (message_type == "snapshot")

    def _handle_snapshot(self, symbol, data, update_seq):
        """Overwrite Redis order book with full snapshot, set last seq, and record snapshot time."""
        ob = data.get("data", {})
        
        bids = ob.get("b", [])
        asks = ob.get("a", [])
        #self.logger.info(f"HANDLE SNAPSHOT: This is the snapshot BIDS{bids}\n\n")
        #self.logger.info(f"HANDLE SNAPSHOT: This is the snapshot ASKS{asks}\n\n")
        #print(f"\nBIDS {bids}\n")
        #print(f"\ASKS {asks}\n")

        #self._overwrite_entire_order_book(symbol, bids, asks)
        #self.last_snapshot_time[symbol] = time.time()
        #self.last_update_seq[symbol] = update_seq
        #self.logger.info(f"[_HANDLE_SNAPSHOT] Overwrote entire order book for {symbol}, seq={update_seq}")

    def _handle_delta(self, symbol, data, update_seq):
        """
        1) Check if we have a stored last_seq.
        2) If none, do an immediate snapshot or set baseline.
        3) If out-of-order or big gap, fetch snapshot.
        4) Otherwise, apply the delta.
        """
        last_seq = self.last_sequences.get(symbol, None)       
        #print(f"HANDELING DELTA: {update_seq}  :  last_seq = {last_seq}")
        # If update_seq <= last_seq, it's old/duplicate
        if update_seq <= last_seq:
            self.logger.debug(f"Ignoring out-of-order or duplicate update for {symbol}. update_seq={update_seq}, last_seq={last_seq}")
            return
        # Otherwise, contiguous -> apply delta
        ob = data.get("data", {})
        bids = ob.get("b", [])
        asks = ob.get("a", [])
        #print(f"Here are the bids: {bids}\n")
        asks = ob.get("a", [])
        #print(f"Here are the asks: {asks}\n")
        self._update_order_book(symbol, bids, asks)
        self.last_update_seq[symbol] = update_seq

    def _periodic_snapshot_refresh(self, symbol):
        """If it's been too long since last snapshot, force a fresh one."""
        if (time.time() - self.last_snapshot_time.get(symbol, 0)) > config.REFRESH_ORDER_BOOK_SNAPSHOT_PERIOD:
            self.fetch_order_book_snapshot(symbol)

    def _log_top_levels(self, symbol):
        """
        Every 10s or so, log top-level bid & ask to keep track of changes.
        """
        if time.time() - self.last_trade_log_time.get(symbol, 0) >= 10:
            redis_key_bids = f"orderbook:{symbol}:bids"
            redis_key_asks = f"orderbook:{symbol}:asks"
            top_bid = self.redis_client.zrevrange(redis_key_bids, 0, 0, withscores=True)
            top_ask = self.redis_client.zrange(redis_key_asks, 0, 0, withscores=True)
            self.last_trade_log_time[symbol] = time.time()
            self.logger.info(f"Top bid for {symbol}: {top_bid}, Top ask for {symbol}: {top_ask}")





