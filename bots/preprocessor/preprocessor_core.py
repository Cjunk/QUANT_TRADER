# --- preprocessor_core.py ---

import os, sys, time, json, redis, threading, datetime, logging, pytz
import pandas as pd
from collections import deque
from utils.logger import setup_logger
from utils.global_indicators import GlobalIndicators

# Redis + Bot Configs
from config.config_redis import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    KLINE_UPDATES, TRADE_CHANNEL, ORDER_BOOK_UPDATES,
    PRE_PROC_KLINE_UPDATES, PRE_PROC_TRADE_CHANNEL, PRE_PROC_ORDER_BOOK_UPDATES,
    SERVICE_STATUS_CHANNEL, HEARTBEAT_CHANNEL
)

from config.config_auto_preprocessor_bot import (
    LOG_FILENAME, LOG_LEVEL, BOT_NAME, BOT_AUTH_TOKEN,
    HEARTBEAT_INTERVAL, WINDOW_SIZE,
    STRATEGY_NAME, VERSION, DESCRIPTION
)

from config.config_common import HEARTBEAT_INTERVAL_SECONDS

class PreprocessorBot:
    def __init__(self, log_filename=LOG_FILENAME):
        self.logger = setup_logger(LOG_FILENAME, getattr(logging, LOG_LEVEL.upper(), logging.WARNING))
        self.bot_name = BOT_NAME
        self.auth_token = BOT_AUTH_TOKEN
        self.running = True
        self.GlobalIndicators = GlobalIndicators()
        self.kline_windows = {}   # {(symbol, interval, market): deque}
        self.trade_windows = {}   # {(symbol, minute): [trades]}
        self.market_channels = {
            "linear": r_cfg.REDIS_CHANNEL["linear.kline_out"],
            "spot": r_cfg.REDIS_CHANNEL["spot.kline_out"],
            # Add derivatives if available
            "derivatives": r_cfg.REDIS_CHANNEL["derivatives.kline_out"] if "derivatives.kline_out" in r_cfg.REDIS_CHANNEL else None
        }

    def _connect_redis(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        # Subscribe to all available kline channels
        channels_to_sub = [ch for ch in self.market_channels.values() if ch]
        self.pubsub.subscribe(*channels_to_sub, TRADE_CHANNEL, ORDER_BOOK_UPDATES)
        self.logger.info(f"✅ Connected to Redis and subscribed to: {channels_to_sub + [TRADE_CHANNEL, ORDER_BOOK_UPDATES]}")

    def _register_bot(self):
        payload = {
            "bot_name": self.bot_name,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": VERSION,
                "pid": os.getpid(),
                "description": DESCRIPTION
            }
        }
        self.redis_client.publish(SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"🔐 Registered bot '{self.bot_name}' with status 'started'.")

    def _start_heartbeat(self):
        while self.running:
            try:
                payload = {
                    "bot_name": self.bot_name,
                    "heartbeat": True,
                    "time": datetime.datetime.now(pytz.timezone("Australia/Sydney")).isoformat(),
                    "metadata": {
                        "version": VERSION,
                        "pid": os.getpid(),
                        "strategy": STRATEGY_NAME
                    }
                }
                self.redis_client.publish(HEARTBEAT_CHANNEL, json.dumps(payload))
                self.logger.debug("❤️ Sent heartbeat.")
            except Exception as e:
                self.logger.warning(f"⚠️ Heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)

    def _listen_redis(self):
        while self.running:
            try:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message and message['type'] == 'message':
                    self._route_message(message['channel'], json.loads(message['data']))
            except Exception as e:
                self.logger.error(f"❌ Failed to handle Redis message: {e}")

    def _flush_old_trades(self):
        while self.running:
            try:
                current_minute = pd.Timestamp.utcnow().floor('min')
                expired = [k for k in self.trade_windows if k[1] < current_minute]
                for key in expired:
                    self._publish_trade_summary(key, self.trade_windows.pop(key))
            except Exception as e:
                self.logger.error(f"❌ Error flushing old trades: {e}")
            time.sleep(1)

    def _route_message(self, channel, payload):
        # Route by channel for kline, trade, orderbook
        for market, ch in self.market_channels.items():
            if ch and channel == ch:
                self._process_kline(payload, market)
                return
        if channel == TRADE_CHANNEL:
            self._process_trade(payload)
        elif channel == ORDER_BOOK_UPDATES:
            self._process_orderbook(payload)

    def _preload_kline_window(self, symbol, interval, market):
        """
        Preload kline window from Redis. If empty, ready to request from database bot.
        Returns True if window was loaded from Redis, False if needs DB fetch.
        """
        key = (symbol, interval, market)
        redis_key = f"kline_window:{market}:{symbol}:{interval}"
        self.kline_windows[key] = deque(maxlen=WINDOW_SIZE)
        items = self.redis_client.lrange(redis_key, -WINDOW_SIZE, -1)
        if items:
            for item in items:
                self.kline_windows[key].append(json.loads(item))
            return True
        else:
            self.logger.info(f"No Redis window for {market}.{symbol}.{interval}. Ready to request from DB if needed.")
            return False

    def _process_kline(self, payload, market):
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

        self.kline_windows[key].append(payload)
        self.redis_client.rpush(redis_key, json.dumps(payload))
        self.redis_client.ltrim(redis_key, -WINDOW_SIZE, -1)

        try:
            df = pd.DataFrame(self.kline_windows[key])
            df[["open", "close", "high", "low", "volume", "turnover"]] = df[["open", "close", "high", "low", "volume", "turnover"]].astype(float)
            enriched_df = self.GlobalIndicators.compute_indicators(df.copy())
            enriched_kline = enriched_df.iloc[-1].to_dict()
            enriched_kline['start_time'] = df.iloc[-1]['start_time']
            # Publish to the correct processed channel for the market
            out_channel = f"{PRE_PROC_KLINE_UPDATES}:{market}"
            self.redis_client.publish(out_channel, json.dumps(enriched_kline))
        except Exception as e:
            self.logger.error(f"❌ Error processing kline for {market}: {e}")

    def _process_trade(self, payload):
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

    def _publish_trade_summary(self, key, trades):
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
        self.redis_client.publish(PRE_PROC_TRADE_CHANNEL, json.dumps(summary))

    def _process_orderbook(self, payload):
        self.redis_client.publish(PRE_PROC_ORDER_BOOK_UPDATES, json.dumps(payload))

    def _final_flush(self):
        self.logger.info("🔄 Flushing remaining trades...")
        for key, trades in list(self.trade_windows.items()):
            self._publish_trade_summary(key, trades)
        self.trade_windows.clear()

    def stop(self):
        self.running = False
        self._final_flush()
        payload = {
            "bot_name": self.bot_name,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis_client.publish(SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info("🛑 Preprocessor Bot stopped.")

    def run(self):
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
