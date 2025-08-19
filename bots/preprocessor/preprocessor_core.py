# --- preprocessor_core.py ---
"""
PreprocessorBot Core Logic
Author: Jericho

Processes raw kline, trade, and orderbook data from Redis, enriches it, and republishes to downstream channels.
Maintains liveness via a modular HeartBeat service.
Tracks klines processed per market type for monitoring.
"""

import os, time, json, threading, datetime, logging, pytz
from collections import deque
import pandas as pd
from utils.logger import setup_logger
from utils.global_indicators import GlobalIndicators
import config.config_redis as config_redis
import config.config_auto_preprocessor_bot as config_auto
from utils.redis_handler import RedisHandler
from utils.HeartBeatService import HeartBeat
import psutil


# === Master Debug Switch ===
DEBUG = False  # Set to False to disable debug logging
CLEAR_WINDOWS_ON_START = os.getenv("CLEAR_WINDOWS_ON_START", "0") == "1"
WINDOW_TTL_SECONDS = int(os.getenv("WINDOW_TTL_SECONDS", "0"))  # 0 = no TTL

class PreprocessorBot:
    """
    This service serves as the QUANT TRADER exchange data source for all data interpretations
    One instance handles all intervals and symbols
    Processes raw kline, trade, and orderbook data from Redis, enriches it, and republishes to downstream channels.
    Maintains liveness via a modular HeartBeat service.
    Tracks klines processed per market type for monitoring.
    publishes a snapshot of each rolling window to market and symbol specific redis channel
    """

    def __init__(self, log_filename=config_auto.LOG_FILENAME):
        """
        Initialize the PreprocessorBot, set up logging, Redis, heartbeat, and data structures.
        """
        log_level = logging.DEBUG if DEBUG else getattr(logging, config_auto.LOG_LEVEL.upper(), logging.INFO)
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

        self.market_channels = { # These are the channels for which raw data flows in
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
        self.klines_processed = {market: 0 for market in self.market_channels} # A Counter to track how many it has processed for each market
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
        self.logger.info(f"Log Level: {'DEBUG' if DEBUG else config_auto.LOG_LEVEL.upper()}")
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
                self.logger.debug(f"[DEBUG] Received Redis message: {message}")
                if message and message['type'] == 'message':
                    self.logger.debug(f"[DEBUG] Received message on channel: {message['channel']}")
                    try:
                        payload = json.loads(message['data'])
                        self.logger.debug(f"[DEBUG] Payload received: {payload}")
                    except Exception as e:
                        self.logger.info(f"[DEBUG] Failed to decode JSON payload: {e} RAW: {message['data']}")
                        continue
                    self._route_message(message['channel'], payload)
            except Exception as e:
                self.logger.info(f"❌ Failed to handle Redis message: {e}")

    def _route_message(self, channel, payload):
        """
        Route incoming Redis messages to the appropriate handler based on channel.
        """

        self.logger.debug(f"[DEBUG] Routing message from channel: {channel} payload: {payload}")
        for market, chans in self.market_channels.items():
            if chans["kline"] == channel:
                self.logger.debug(f"[DEBUG] Detected kline channel for market: {market}")
                self._process_kline(payload, market)
                return
            if chans["trade"] == channel:
                self.logger.debug(f"[DEBUG] Detected trade channel for market: {market}")
                self._process_trade(payload, market)
                return
            if chans["orderbook"] == channel:
                self.logger.debug(f"[DEBUG] Detected orderbook channel for market: {market}")
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
                self.logger.info(f"❌ Error flushing old trades: {e}")
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


    def _preload_kline_window(self, symbol, interval, market):
        # TODO: Rewrite this to get kline data from bybit.
        """
        Preload kline window for a given symbol, interval, and market.
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
    # =========================
    # Kline, Trade, and Orderbook Processing
    # =========================
    def _process_kline(self, payload, market):
        """
        Process a kline message, compute indicators on (window + new row),
        store the ENRICHED row in the Redis rolling window, and publish it.
        """
        try:
            symbol = payload['symbol']
            interval = payload['interval']
            redis_key = f"kline_window:{market}:{symbol}:{interval}"

            # read current window (enriched or not)
            items = self.redis_client.lrange(redis_key, -config_auto.WINDOW_SIZE, -1)
            window = [json.loads(item) for item in items] if items else []

            # duplicate guard (by start_time)
            if window and window[-1].get('start_time') == payload.get('start_time'):
                self.logger.debug(f"[DEBUG] Duplicate kline for {market}.{symbol}.{interval}. Skipping.")
                return

            # build combined df = existing window + new raw row
            combined = window + [payload]
            df = pd.DataFrame(combined)

            # numeric casting
            for c in ["open", "close", "high", "low", "volume", "turnover"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

            # compute indicators on the combined frame
            enriched_df = self.GlobalIndicators.compute_indicators(df.copy())
            enriched = enriched_df.iloc[-1].to_dict()

            # ensure id fields (keep original timing/ids)
            enriched["symbol"] = symbol
            enriched["interval"] = interval
            enriched["market"] = market
            enriched["start_time"] = payload.get("start_time")

            # write ENRICHED row to the rolling window (single source of truth)
            pipe = self.redis_client.pipeline()
            pipe.rpush(redis_key, json.dumps(enriched))
            pipe.ltrim(redis_key, -config_auto.WINDOW_SIZE, -1)
            if WINDOW_TTL_SECONDS > 0:
                pipe.expire(redis_key, WINDOW_TTL_SECONDS)            
            pipe.execute()

            # counters/metrics
            nans = sum(pd.isnull(list(enriched.values())))
            self.nans_this_interval += nans
            self.klines_processed[market] += 1
            self.status["metadata"]["vitals"]["klines_processed"] = self.klines_processed.copy()

            # publish enriched tick to downstream consumers
            out_channel = config_redis.PRE_PROC_KLINE_UPDATES
            self.redis_handler.publish(out_channel, json.dumps(enriched))

        except Exception as e:
            self.logger.info(f"❌ Error processing kline for {market}: {e}")


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
            self.logger.debug(f"📤 Published raw trade for {symbol} at {payload['price']}")
        except Exception as e:
            self.logger.info(f"❌ Error processing trade: {e}")

    def _process_orderbook(self, payload, market=None):
        """
        Process an orderbook message and publish it to the appropriate channel.
        """
        try:
            out_channel = config_redis.PRE_PROC_ORDER_BOOK_UPDATES
            self.redis_handler.publish(out_channel, json.dumps(payload))
            self.logger.debug(f"📤 Published orderbook update for {payload.get('symbol', 'unknown')}")
        except Exception as e:
            self.logger.info(f"❌ Error processing orderbook for {market}: {e}")
    def _clear_old_windows(self):
        patterns = ["kline_window:linear:*", "kline_window:spot:*", "kline_window:derivatives:*"]
        for pat in patterns:
            try:
                cursor, total = 0, 0
                while True:
                    cursor, keys = self.redis_client.scan(cursor=cursor, match=pat, count=500)
                    if keys:
                        pipe = self.redis_client.pipeline()
                        for k in keys:
                            pipe.delete(k)
                        pipe.execute()
                        total += len(keys)
                    if cursor == 0:
                        break
                self.logger.info(f"🧹 Cleared {total} keys for pattern {pat}")
            except Exception as e:
                self.logger.warning(f"⚠️ Failed clearing {pat}: {e}")

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
        if CLEAR_WINDOWS_ON_START:
            self._clear_old_windows()
        self._connect_redis()
        threading.Thread(target=self._listen_redis, daemon=True).start()
        threading.Thread(target=self._flush_old_trades, daemon=True).start()
        self.logger.info("🚀 Preprocessor Bot is running...")
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("🛑 Keyboard Interrupt received.")
            self.stop()


