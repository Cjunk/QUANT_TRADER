# --- preprocessor_core.py ---
"""
PreprocessorBot Core Logic
Author: Jericho

Processes raw kline, trade, and orderbook data from Redis, enriches it, and republishes to downstream channels.
Maintains liveness via a modular HeartBeat service.
Tracks klines processed per market type for monitoring.
"""

import sys, os, time, json, threading, datetime, logging, pytz
from collections import deque
import pandas as pd
from utils.logger import setup_logger
from utils.global_indicators import GlobalIndicators
import config.config_redis as config_redis
import config.config_auto_preprocessor_bot as config_auto
from utils.redis_handler import RedisHandler
from utils.HeartBeatService import HeartBeat
import psutil  # Add this import for memory usage


# === Debug Switch ===
DEBUG_MODE = True  # Set to False to disable debug logging

class PreprocessorBot:
    """
    Processes raw kline, trade, and orderbook data from Redis, enriches it, and republishes to downstream channels.
    Maintains liveness via a modular HeartBeat service.
    Tracks klines processed per market type for monitoring.
    """

    def __init__(self, log_filename=config_auto.LOG_FILENAME):
        print(f"PreprocessorBot __init__ called. PID: {os.getpid()}")
        """
        Initialize the PreprocessorBot, set up logging, Redis, heartbeat, and data structures.
        """
        log_level = logging.DEBUG if DEBUG_MODE else getattr(logging, config_auto.LOG_LEVEL.upper(), logging.WARNING)
        self.logger = setup_logger(
            config_auto.LOG_FILENAME,
            log_level
        )
        self.bot_name = config_auto.BOT_NAME
        self.auth_token = config_auto.BOT_AUTH_TOKEN
        self.running = True
        self.GlobalIndicators = GlobalIndicators()
        self.kline_windows = {}   # {(symbol, interval, market): deque}
        self.trade_windows = {}   # {(symbol, minute): [trades]}

        self.market_channels = {
            "linear": {
                "kline": config_redis.REDIS_CHANNEL["linear.kline_out"],
                "trade": config_redis.REDIS_CHANNEL["linear.trade_out"],
                "orderbook": config_redis.REDIS_CHANNEL["linear.orderbook_out"]
            },
            "spot": {
                "kline": config_redis.REDIS_CHANNEL["spot.kline_out"],
                "trade": config_redis.REDIS_CHANNEL["spot.trade_out"],
                "orderbook": config_redis.REDIS_CHANNEL["spot.orderbook_out"]
            },
            "derivatives": {
                "kline": config_redis.REDIS_CHANNEL.get("derivatives.kline_out"),
                "trade": config_redis.REDIS_CHANNEL.get("derivatives.trade_out"),
                "orderbook": config_redis.REDIS_CHANNEL.get("derivatives.orderbook_out")
            }
        }
        # Track klines processed per market type
        self.klines_processed = {market: 0 for market in self.market_channels}
        self.nans_last_heartbeat = 0
        self.nans_this_interval = 0

        # Redis handler setup
        self.redis_handler = RedisHandler(config_redis, self.logger)
        self.redis_handler.connect()
        self.redis_client = self.redis_handler.client

        # Heartbeat setup
        self.status = {
            "bot_name": self.bot_name,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": config_auto.VERSION,
                "pid": os.getpid(),
                "description": config_auto.DESCRIPTION,
                "strategy": getattr(config_auto, "STRATEGY_NAME", "N/A"),
                "vitals": {
                    "klines_processed": self.klines_processed.copy()
                },
            }
        }
        self.heartbeat = HeartBeat(
            bot_name=self.bot_name,
            auth_token=self.auth_token,
            logger=self.logger,
            redis_handler=self.redis_handler,
            metadata=self.status
        )

        # Startup report
        self._startup_report()


    def _startup_report(self):
        import platform
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / 1024 / 1024
        self.logger.info("========== PreprocessorBot Startup Report ==========")
        self.logger.info(f"Bot Name: {self.bot_name}")
        self.logger.info(f"Version: {getattr(config_auto, 'VERSION', 'N/A')}")
        self.logger.info(f"Strategy: {getattr(config_auto, 'STRATEGY_NAME', 'N/A')}")
        self.logger.info(f"Window Size: {getattr(config_auto, 'WINDOW_SIZE', 'N/A')}")
        self.logger.info(f"Log Level: {'DEBUG' if DEBUG_MODE else config_auto.LOG_LEVEL.upper()}")
        self.logger.info(f"Process ID: {os.getpid()}")
        self.logger.info(f"Platform: {platform.platform()}")
        self.logger.info(f"Python: {platform.python_version()}")
        self.logger.info(f"Memory Used: {mem_mb:.2f} MB")
        self.logger.info(f"Subscribed Redis Channels: {list(self.market_channels['linear'].values()) + list(self.market_channels['spot'].values()) + list(self.market_channels['derivatives'].values())}")
        self.logger.info("===================================================")

    # =========================
    # Redis Connection & Subscription
    # =========================
    def _connect_redis(self):
        """
        Connect to Redis and subscribe to relevant channels.
        """
        self.pubsub = self.redis_client.pubsub()
        channels_to_sub = [
            v for k, v in config_redis.REDIS_CHANNEL.items()
            if any(suffix in k for suffix in (".kline_out", ".trade_out", ".orderbook_out"))
        ]
        self.pubsub.subscribe(*channels_to_sub)
        self.logger.info(f"✅ Connected to Redis and subscribed to: {channels_to_sub}")

    def _listen_redis(self):
        """
        Listen to Redis channels and route messages for processing.
        """
        while self.running:
            try:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message and message['type'] == 'message':
                    #self.logger.info(f"[DEBUG] Received message on channel: {message['channel']}")
                    try:
                        payload = json.loads(message['data'])
                        #self.logger.info(f"[DEBUG] Payload received: {payload}")
                    except Exception as e:
                        self.logger.error(f"[DEBUG] Failed to decode JSON payload: {e} RAW: {message['data']}")
                        continue
                    self._route_message(message['channel'], payload)
            except Exception as e:
                self.logger.error(f"❌ Failed to handle Redis message: {e}")

    def _route_message(self, channel, payload):
        """
        Route incoming Redis messages to the appropriate handler based on channel.
        """
        #self.logger.info(f"[DEBUG] Routing message from channel: {channel} payload: {payload}")
        for market, chans in self.market_channels.items():
            if chans["kline"] == channel:
                #self.logger.info(f"[DEBUG] Detected kline channel for market: {market}")
                self._process_kline(payload, market)
                return
            # Comment out trade and orderbook processing for now
            if chans["trade"] == channel:
                #self.logger.info(f"[DEBUG] Detected trade channel for market: {market}")
                self._process_trade(payload, market)
                return
            if chans["orderbook"] == channel:
                #self.logger.info(f"[DEBUG] Detected orderbook channel for market: {market}")
                self._process_orderbook(payload, market)
                return

    # =========================
    # Trade Window Management
    # =========================
    def _flush_old_trades(self):
        """
        Periodically flush old trades and publish trade summaries.
        """
        while self.running:
            try:
                current_minute = pd.Timestamp.utcnow().floor('min')
                expired = [k for k in self.trade_windows if k[1] < current_minute]
                for key in expired:
                    self._publish_trade_summary(key, self.trade_windows.pop(key))
            except Exception as e:
                self.logger.error(f"❌ Error flushing old trades: {e}")
            time.sleep(1)

    def _publish_trade_summary(self, key, trades):
        """
        Publish a summary of trades for a given symbol and minute.
        """
        symbol, minute_start = key
        total_volume = sum(t['volume'] for t in trades)
        vwap = sum(t['price'] * t['volume'] for t in trades) / total_volume if total_volume > 0 else 0
        max_trade = max(trades, key=lambda t: t['volume'], default={"volume": 0, "price": 0})

        summary = {
            "symbol": symbol,
            "minute_start": minute_start.isoformat(),
            "total_volume": total_volume,
            "vwap": vwap,
            "trade_count": len(trades),
            "largest_trade_volume": max_trade['volume'],
            "largest_trade_price": max_trade['price']
        }
        out_channel = config_redis.PRE_PROC_TRADE_CHANNEL
        self.redis_handler.publish(out_channel, json.dumps(summary))

    def _final_flush(self):
        """
        Flush all remaining trades before shutdown.
        """
        self.logger.info("🔄 Flushing remaining trades...")
        for key, trades in list(self.trade_windows.items()):
            self._publish_trade_summary(key, trades)
        self.trade_windows.clear()

    # =========================
    # Kline, Trade, and Orderbook Processing
    # =========================
    def _preload_kline_window(self, symbol, interval, market):
        """
        Preload kline window from Redis for a given symbol, interval, and market.
        """
        key = (symbol, interval, market)
        redis_key = f"kline_window:{market}:{symbol}:{interval}"
        self.kline_windows[key] = deque(maxlen=config_auto.WINDOW_SIZE)
        items = self.redis_client.lrange(redis_key, -config_auto.WINDOW_SIZE, -1)
        if items:
            for item in items:
                self.kline_windows[key].append(json.loads(item))
            return True
        else:
            self.logger.info(f"No Redis window for {market}.{symbol}.{interval}. Ready to request from DB if needed.")
            return False

    def _process_kline(self, payload, market):
        """
        Process a kline message, enrich it, and publish the result.
        Also updates the klines_processed counter for the market.
        """
        self.logger.info(f"[DEBUG] Processing kline for {market}: {payload}")
        symbol, interval = payload['symbol'], payload['interval']
        redis_key = f"kline_window:{market}:{symbol}:{interval}"

        # Fetch current window from Redis
        items = self.redis_client.lrange(redis_key, -config_auto.WINDOW_SIZE, -1)
        window = [json.loads(item) for item in items] if items else []

        # Check for duplicate
        if window:
            last = window[-1]
            if last['start_time'] == payload['start_time'] and last['close'] == payload['close'] and payload['market'] == last['market'] and payload['interval'] == last['interval']:
                self.logger.info(f"[DEBUG] Duplicate kline detected for {market}.{symbol}.{interval}. Skipping.")
                return

        payload['market'] = market
        self.redis_client.rpush(redis_key, json.dumps(payload))
        self.redis_client.ltrim(redis_key, -config_auto.WINDOW_SIZE, -1)

        try:
            # Re-fetch window for enrichment
            items = self.redis_client.lrange(redis_key, -config_auto.WINDOW_SIZE, -1)
            window = [json.loads(item) for item in items] if items else []
            df = pd.DataFrame(window)
            df[["open", "close", "high", "low", "volume", "turnover"]] = df[["open", "close", "high", "low", "volume", "turnover"]].astype(float)
            enriched_df = self.GlobalIndicators.compute_indicators(df.copy())
            enriched_kline = enriched_df.iloc[-1].to_dict()
            enriched_kline['start_time'] = df.iloc[-1]['start_time']
            enriched_kline['market'] = market
            nans = sum(pd.isnull(list(enriched_kline.values())))
            self.nans_this_interval += nans
            self.klines_processed[market] += 1
            self.status["metadata"]["vitals"]["klines_processed"] = self.klines_processed.copy()
            out_channel = config_redis.PRE_PROC_KLINE_UPDATES
            self.redis_handler.publish(out_channel, json.dumps(enriched_kline))
        except Exception as e:
            self.logger.error(f"❌ Error processing kline for {market}: {e}")

    def _process_trade(self, payload, market=None):
        """
        Process a trade message and add it to the trade window.
        """
        try:
            symbol = payload['symbol']
            trade_time = pd.to_datetime(payload['trade_time'], utc=True).floor('min')
            key = (symbol, trade_time)
            self.trade_windows.setdefault(key, []).append({
                "price": payload['price'],
                "volume": payload['volume']
            })
        
            # 🔥 Emit full trade delta to DB via Redis
            payload["market"] = market
            self.redis_handler.publish(config_redis.RAW_TRADE_CHANNEL, json.dumps(payload))
            #self.logger.debug(f"📤 Published raw trade for {symbol} at {payload['price']}")
        except Exception as e:
            self.logger.error(f"❌ Error processing trade: {e}")

    def _process_orderbook(self, payload, market=None):
        """
        Process an orderbook message and publish it to the appropriate channel.
        """
        try:
            out_channel = config_redis.PRE_PROC_ORDER_BOOK_UPDATES
            self.redis_handler.publish(out_channel, json.dumps(payload))
        except Exception as e:
            self.logger.error(f"❌ Error processing orderbook for {market}: {e}")

    # =========================
    # Public Interface
    # =========================
    def stop(self):
        """
        Stop the bot and flush all remaining trades.
        """
        self.running = False
        self._final_flush()
        self.logger.info("🛑 Preprocessor Bot stopped.")

    def run(self):
        """
        Start the bot, connect to Redis, and begin processing messages.
        """
        self._connect_redis()
        threading.Thread(target=self._listen_redis, daemon=True).start()
        threading.Thread(target=self._flush_old_trades, daemon=True).start()
        self.logger.info("🚀 Preprocessor Bot is running...")
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.warning("🛑 Keyboard Interrupt received.")
            self.stop()


