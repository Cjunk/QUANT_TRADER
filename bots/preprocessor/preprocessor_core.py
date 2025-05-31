# --- preprocessor_core.py ---
"""
PreprocessorBot Core Logic
Author: Jericho

Redis Channels, Keys, and Important Variables Used:
--------------------------------------------------
- config_redis.REDIS_CHANNEL[...]
    Purpose: Maps all raw and processed Redis pub/sub channels for kline, trade, and orderbook data by market type (e.g., 'linear.kline_out').
    Used in: self.market_channels (for routing), _connect_redis(), _route_message(), and dynamic subscription logic.

- config_redis.SERVICE_STATUS_CHANNEL
    Purpose: Publishes bot status updates (started, stopped) and heartbeats.
    Used in: _register_bot(), stop(), _start_heartbeat()

- config_redis.PRE_PROC_KLINE_UPDATES
    Purpose: Publishes enriched/processed kline data for downstream bots/services.
    Used in: _process_kline()

- config_redis.PRE_PROC_ORDER_BOOK_UPDATES
    Purpose: Publishes processed orderbook data for downstream bots/services.
    Used in: _process_orderbook()

- config_redis.PRE_PROC_TRADE_CHANNEL
    Purpose: Publishes summarized trade data (per minute) for downstream bots/services.
    Used in: _publish_trade_summary()

- Redis List Keys (e.g., 'kline_window:{market}:{symbol}:{interval}')
    Purpose: Stores rolling window of recent klines for each symbol/interval/market.
    Used in: _preload_kline_window(), _process_kline()

- self.market_channels
    Purpose: Maps market types ('linear', 'spot', 'derivatives') to their respective Redis channels for kline, trade, and orderbook data.
    Used in: _route_message(), initialization

- self.kline_windows
    Purpose: In-memory rolling window of recent klines for each (symbol, interval, market).
    Used in: _preload_kline_window(), _process_kline(), _publish_trade_summary()

- self.trade_windows
    Purpose: In-memory rolling window of trades per (symbol, minute).
    Used in: _process_trade(), _flush_old_trades(), _publish_trade_summary()

- self.pubsub
    Purpose: Redis pubsub object for listening to raw data channels.
    Used in: _connect_redis(), _listen_redis()

- self.redis_client
    Purpose: Shared Redis client for all pub/sub and key operations.
    Used throughout the bot.

"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import time, json, threading, datetime, logging, pytz
import pandas as pd
from collections import deque
from utils.logger import setup_logger
from utils.global_indicators import GlobalIndicators
import config.config_redis as config_redis
import config.config_common as config_common
import config.config_auto_preprocessor_bot as config_auto
from utils.redis_client import get_redis
from utils.heartbeat import send_heartbeat

class PreprocessorBot:
    def __init__(self, log_filename=config_auto.LOG_FILENAME):
        self.logger = setup_logger(config_auto.LOG_FILENAME, getattr(logging, config_auto.LOG_LEVEL.upper(), logging.WARNING))
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
            # Add derivatives if available
            "derivatives": {
                "kline": config_redis.REDIS_CHANNEL["derivatives.kline_out"] if "derivatives.kline_out" in config_redis.REDIS_CHANNEL else None,
                "trade": config_redis.REDIS_CHANNEL["derivatives.trade_out"] if "derivatives.trade_out" in config_redis.REDIS_CHANNEL else None,
                "orderbook": config_redis.REDIS_CHANNEL["derivatives.orderbook_out"] if "derivatives.orderbook_out" in config_redis.REDIS_CHANNEL else None
            }
        }
        self.total_klines_processed = 0
        self.nans_last_heartbeat = 0
        self.nans_this_interval = 0

    # =====================================================
    # Redis Connection & Subscription
    # =====================================================
    def _connect_redis(self):
        """
        # -----------------------------------------------------
        # Connect to Redis and subscribe to all relevant channels.
        # Subscribes to kline, trade, and orderbook output channels for all supported markets.
        # Sets up self.redis_client and self.pubsub for use throughout the bot.
        # -----------------------------------------------------
        """
        self.redis_client = get_redis()
        self.pubsub = self.redis_client.pubsub()
        # Dynamically subscribe to all kline, trade, and orderbook out channels
        channels_to_sub = [
            v for k, v in config_redis.REDIS_CHANNEL.items()
            if any(suffix in k for suffix in (".kline_out", ".trade_out", ".orderbook_out"))
        ]
        self.pubsub.subscribe(*channels_to_sub)
        self.logger.info(f"✅ Connected to Redis and subscribed to: {channels_to_sub}")

    # =====================================================
    # Bot Registration & Heartbeat
    # =====================================================
    def _register_bot(self):
        """
        # -----------------------------------------------------
        # Publish a 'started' status payload to Redis for monitoring/orchestration.
        # Includes version, PID, and description metadata.
        # -----------------------------------------------------
        """
        payload = {
            "bot_name": self.bot_name,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": config_auto.VERSION,
                "pid": os.getpid(),
                "description": config_auto.DESCRIPTION
            }
        }
        self.redis_client.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"🔐 Registered bot '{self.bot_name}' with status 'started'.")

    def _start_heartbeat(self):
        """
        # -----------------------------------------------------
        # Periodically send a heartbeat payload to Redis for liveness monitoring.
        # Uses the shared send_heartbeat utility. Runs in a background thread.
        # -----------------------------------------------------
        """
        # Heartbeat logic is now handled by the shared utility. This method is intentionally minimal.
        while self.running:
            try:
                payload = {
                    "bot_name": self.bot_name,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat(),
                    "auth_token": self.auth_token,
                    "metadata": {
                        "version": config_auto.VERSION,
                        "pid": os.getpid(),
                        "strategy": config_auto.STRATEGY_NAME,
                        "vitals": {
                            "total_klines_processed": int(self.total_klines_processed),
                            "nans_last_heartbeat": int(self.nans_last_heartbeat)
                        }
                    }
                }
                # Use the shared heartbeat utility; implementation is hidden from this file
                send_heartbeat(payload, status="heartbeat")
                self.logger.debug(f"❤️ Sent heartbeat. Vitals: {payload['metadata']['vitals']}")
                self.nans_last_heartbeat = self.nans_this_interval
                self.nans_this_interval = 0
            except Exception as e:
                self.logger.warning(f"⚠️ Heartbeat failed: {e}")
            time.sleep(config_common.HEARTBEAT_INTERVAL_SECONDS)

    # =====================================================
    # Redis Listening & Message Routing
    # =====================================================
    def _listen_redis(self):
        """
        # -----------------------------------------------------
        # Listen for messages on all subscribed Redis channels.
        # Routes each message to the appropriate handler based on channel type.
        # -----------------------------------------------------
        """
        while self.running:
            try:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message and message['type'] == 'message':
                    self._route_message(message['channel'], json.loads(message['data']))
            except Exception as e:
                self.logger.error(f"❌ Failed to handle Redis message: {e}")

    def _route_message(self, channel, payload):
        """
        # -----------------------------------------------------
        # Route incoming Redis messages to the correct processing function
        # (kline, trade, or orderbook) based on the channel.
        # -----------------------------------------------------
        """
        # Route by channel for kline, trade, orderbook
        
        for market, chans in self.market_channels.items():
            if chans["kline"] == channel:
                #print(f"🔄 Routing message on channel: {channel}")
                self._process_kline(payload, market)
                return
            if chans["trade"] == channel:
                self._process_trade(payload, market)
                return
            if chans["orderbook"] == channel:
                self._process_orderbook(payload, market)
                return

    # =====================================================
    # Trade Window Management
    # =====================================================
    def _flush_old_trades(self):
        """
        # -----------------------------------------------------
        # Periodically flushes and publishes summaries for expired trade windows.
        # Ensures trade summaries are published once per minute per symbol.
        # -----------------------------------------------------
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
        # -----------------------------------------------------
        # Publish a summary of trades for a given (symbol, minute) window.
        # Calculates total volume, VWAP, and largest trade, then publishes to Redis.
        # -----------------------------------------------------
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
        # Publish to the correct processed channel for the market (if possible)
        # Try to infer market from the key if possible (not always possible, fallback to generic)
        market = None
        for mkt in self.market_channels:
            if mkt == "derivatives":
                continue
            # k is a tuple: (symbol, interval, market)
            for k in self.kline_windows:
                if isinstance(k, tuple) and len(k) >= 1 and k[0] == symbol:
                    if len(k) == 3 and k[2] == mkt:
                        market = mkt
                        break
            if market:
                break
        out_channel = f"{config_redis.PRE_PROC_TRADE_CHANNEL}:{market}" if market else config_redis.PRE_PROC_TRADE_CHANNEL
        self.redis_client.publish(out_channel, json.dumps(summary))

    def _final_flush(self):
        """
        # -----------------------------------------------------
        # Flushes and publishes all remaining trade summaries before shutdown.
        # -----------------------------------------------------
        """
        self.logger.info("🔄 Flushing remaining trades...")
        for key, trades in list(self.trade_windows.items()):
            self._publish_trade_summary(key, trades)
        self.trade_windows.clear()

    # =====================================================
    # Kline, Trade, and Orderbook Processing
    # =====================================================
    def _preload_kline_window(self, symbol, interval, market):
        """
        # -----------------------------------------------------
        # Preload kline window from Redis for a given symbol/interval/market.
        # Returns True if window was loaded from Redis, False if needs DB fetch.
        # -----------------------------------------------------
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
        # -----------------------------------------------------
        # Process a new kline payload: update in-memory window, persist to Redis,
        # compute indicators, enrich kline, and publish to processed channel.
        # -----------------------------------------------------
        """
        symbol, interval = payload['symbol'], payload['interval']
        key = (symbol, interval, market)
        redis_key = f"kline_window:{market}:{symbol}:{interval}"

        if key not in self.kline_windows:
            self._preload_kline_window(symbol, interval, market)

        if self.kline_windows[key]:
            last = self.kline_windows[key][-1]
            if last['start_time'] == payload['start_time'] and last['close'] == payload['close']:
                self.logger.debug(f"🔁 Duplicate kline detected for {market}.{symbol}.{interval}. Skipping.")
                return

        # Ensure the market type is always included in the kline payload
        payload['market'] = market
        self.kline_windows[key].append(payload)
        self.redis_client.rpush(redis_key, json.dumps(payload))
        self.redis_client.ltrim(redis_key, -config_auto.WINDOW_SIZE, -1)

        try:
            df = pd.DataFrame(self.kline_windows[key])
            df[["open", "close", "high", "low", "volume", "turnover"]] = df[["open", "close", "high", "low", "volume", "turnover"]].astype(float)
            enriched_df = self.GlobalIndicators.compute_indicators(df.copy())
            enriched_kline = enriched_df.iloc[-1].to_dict()
            enriched_kline['start_time'] = df.iloc[-1]['start_time']
            # Always include the market type in the enriched kline payload
            enriched_kline['market'] = market
            # Count NaNs/nulls in the enriched kline
            nans = sum(pd.isnull(list(enriched_kline.values())))
            self.nans_this_interval += nans
            self.total_klines_processed += 1
            # Publish to the correct processed channel for the market
            out_channel = f"{config_redis.PRE_PROC_KLINE_UPDATES}"
            self.redis_client.publish(out_channel, json.dumps(enriched_kline))
        except Exception as e:
            self.logger.error(f"❌ Error processing kline for {market}: {e}")

    def _process_trade(self, payload, market=None):
        """
        # -----------------------------------------------------
        # Process a new trade payload: update in-memory trade window for the symbol/minute.
        # -----------------------------------------------------
        """
        try:
            symbol = payload['symbol']
            trade_time = pd.to_datetime(payload['trade_time'], utc=True).floor('min')
            key = (symbol, trade_time)
            self.trade_windows.setdefault(key, []).append({
                "price": payload['price'],
                "volume": payload['volume']
            })
        except Exception as e:
            self.logger.error(f"❌ Error processing trade: {e}")

    def _process_orderbook(self, payload, market=None):
        """
        # -----------------------------------------------------
        # Process a new orderbook payload: publish to the processed orderbook channel for the market.
        # -----------------------------------------------------
        """
        try:
            # Publish to the correct processed channel for the market
            out_channel = f"{config_redis.PRE_PROC_ORDER_BOOK_UPDATES}:{market}" if market else config_redis.PRE_PROC_ORDER_BOOK_UPDATES
            self.redis_client.publish(out_channel, json.dumps(payload))
        except Exception as e:
            self.logger.error(f"❌ Error processing orderbook for {market}: {e}")

    # =====================================================
    # Public Interface
    # =====================================================
    def stop(self):
        """
        # -----------------------------------------------------
        # Stop the bot, flush all remaining trades, and publish a 'stopped' status to Redis.
        # -----------------------------------------------------
        """
        self.running = False
        self._final_flush()
        payload = {
            "bot_name": self.bot_name,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis_client.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info("🛑 Preprocessor Bot stopped.")

    def run(self):
        """
        # -----------------------------------------------------
        # Main run loop: connect to Redis, register, start heartbeat, listen for messages,
        # and flush old trades. Handles graceful shutdown on KeyboardInterrupt.
        # -----------------------------------------------------
        """
        self._connect_redis()
        self._register_bot()
        threading.Thread(target=self._start_heartbeat, daemon=True).start()
        threading.Thread(target=self._listen_redis, daemon=True).start()
        threading.Thread(target=self._flush_old_trades, daemon=True).start()
        self.logger.info("🚀 Preprocessor Bot is running...")
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.warning("🛑 Keyboard Interrupt received.")
            self.stop()

if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is NOT activated. Please activate it.")
        sys.exit(1)
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting Preprocessor Bot...")
    PreprocessorBot().run()
