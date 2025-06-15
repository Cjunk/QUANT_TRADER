import json
from datetime import datetime, timedelta
from utils.redis_handler import RedisHandler
from utils.logger import setup_logger
from trade_window import TradeWindow
import config.config_redis as config_redis

# 🔕 Disable debug unless running standalone or during dev testing
DEBUG = False
import unicodedata
class TradeRadar:
    """
    TradeRadar processes incoming trade data, maintains rolling analysis windows, and produces 
    statistical and trend insights (VWAP, CVD, Buy/Sell volume). Designed to be used by trading bots 
    for market context and decision support.
    """

    def __init__(self, max_duration_secs=900, max_idle_secs=600, redis_handler=None, 
                 intervals_secs=[60, 300, 600]):
        """
        Initialize the radar with configurable time horizons and data source hooks.

        Args:
            max_duration_secs (int): Max duration to retain trades in seconds.
            max_idle_secs (int): Duration of inactivity after which a symbol is purged.
            redis_handler (RedisHandler): Optional Redis client handler.
            intervals_secs (List[int]): List of time intervals to compute stats over.
        """
        self.windows = {}  # {(symbol, market): TradeWindow}
        self.last_trade_time = {}  # Last seen timestamp per (symbol, market)
        self.max_duration = max_duration_secs
        self.max_idle = timedelta(seconds=max_idle_secs)
        self.intervals_secs = intervals_secs

        # Set up Redis/logging
        if redis_handler is None:
            self.logger = setup_logger("traderadar.log")
            self.redis = RedisHandler(config_redis, self.logger)
            self.redis.connect()
        else:
            self.redis = redis_handler
            self.logger = getattr(redis_handler, 'logger', setup_logger("traderadar.log"))

        self.logger.setLevel("DEBUG" if DEBUG else "INFO")

        # Startup Log
        self.logger.info("[TradeRadar] 🚦 TradeRadar engine initialized")
        self.logger.info(f"[TradeRadar] 📌 Start Time: {datetime.utcnow().isoformat()}")
        self.logger.info(f"[TradeRadar] ⚙️ Config -> Max Duration: {self.max_duration}s | Idle Timeout: {self.max_idle}s")
        self.logger.info(f"[TradeRadar] 📊 Intervals Active: {self.intervals_secs}")
        self.logger.info(f"[TradeRadar] 🛠️ Debug Mode: {'ON' if DEBUG else 'OFF'}")

    def add_trade(self, symbol, market, price, volume, side, timestamp=None):
        """
        Ingests a trade into the correct trade window and updates its state.

        Args:
            symbol (str): e.g., "BTCUSDT"
            market (str): e.g., "spot" or "linear"
            price (float)
            volume (float)
            side (str): "Buy" or "Sell"
            timestamp (datetime): optional override; defaults to now
        """
        key = (symbol, market)
        now = timestamp or datetime.utcnow()
        if key not in self.windows:
            
            self.logger.info(f"[TradeRadar] 🆕 Creating new TradeWindow for {key}")
            self.windows[key] = TradeWindow(self.max_duration)
        self.windows[key].add_trade(price, volume, side, now)
        self.last_trade_time[key] = now
        self._purge_inactive_symbols(now)

    @staticmethod
    def determine_trend_bias(vwap_start, vwap_end, cvd_start, cvd_end):
        """
        Determine market bias based on VWAP and CVD direction over time.
        Returns a clean, raw string without emojis.
        """
        try:
            vwap_trend = vwap_end - vwap_start
            cvd_trend = cvd_end - cvd_start
            if vwap_trend > 0 and cvd_trend > 0:
                return "Bullish"
            elif vwap_trend < 0 and cvd_trend < 0:
                return "Bearish"
            elif vwap_trend > 0 and cvd_trend < 0:
                return "Divergence: Price Up, CVD Down"
            elif vwap_trend < 0 and cvd_trend > 0:
                return "Divergence: Price Down, CVD Up"
            else:
                return "Neutral"
        except Exception as e:
            if DEBUG:
                print(f"[TradeRadar] Error in determine_trend_bias: {e}")
            return "Error"

    def get_stats(self, symbol, market, include_trend_bias=False):
        """
        Compute VWAP, CVD, and volume stats over configured intervals for a given market-symbol pair.

        Args:
            symbol (str)
            market (str)
            include_trend_bias (bool): Whether to include start-end VWAP/CVD analysis.

        Returns:
            dict: Nested dictionary keyed by interval (e.g., "60s") with metric values.
        """
        key = (symbol, market)
        if key not in self.windows:
            return {}

        result = {}
        window = self.windows[key]

        for interval in self.intervals_secs:
            stats = window.compute_stats(interval)
            result[f"{interval}s"] = stats

            if include_trend_bias:
                vwap_start, vwap_end, cvd_start, cvd_end = window.get_trend_inputs(interval)
                result[f"{interval}s"]["Bias"] = (
                    self.determine_trend_bias(vwap_start, vwap_end, cvd_start, cvd_end)
                    if vwap_start is not None else "❓ Not enough data"
                )
        return result

    def _purge_inactive_symbols(self, now=None):
        """
        Cleans up old TradeWindow entries for symbols that have been inactive beyond the idle threshold.
        """
        now = now or datetime.utcnow()
        to_remove = [k for k, last_seen in self.last_trade_time.items() if now - last_seen > self.max_idle]
        for key in to_remove:
            if DEBUG:
                self.logger.debug(f"[TradeRadar] 🧹 Purging inactive {key}")
            del self.windows[key]
            del self.last_trade_time[key]

    

    def clean_bias(self,text):
        try:
            return unicodedata.normalize('NFKC', text)
        except:
            return text

    def round_stats(self,stats_dict):
        for key in ["VWAP", "CVD", "Buy Volume", "Sell Volume", "Total Volume"]:
            if key in stats_dict:
                stats_dict[key] = round(stats_dict[key], 4)

    def publish_stats(self, symbol, market, include_trend_bias=False, channel_prefix="market_facts"):
        stats = self.get_stats(symbol, market, include_trend_bias=include_trend_bias)
        if not stats:
            if DEBUG:
                self.logger.debug(f"[TradeRadar] No stats to publish for {symbol}-{market}")
            return

        # Sanitize/round values
        for interval_key in stats:
            self.round_stats(stats[interval_key])
            if "Bias" in stats[interval_key]:
                stats[interval_key]["Bias"] = self.clean_bias(stats[interval_key]["Bias"])

        message = {
            "symbol": symbol,
            "market": market,
            "timestamp": datetime.utcnow().isoformat(),
            "stats": stats
        }

        channel = f"{channel_prefix}:{market}:{symbol}"
        json_message = json.dumps(message)

        if self.redis:
            if DEBUG:
                self.logger.debug(f"[TradeRadar] 📡 Publishing to '{channel}':\n{json.dumps(message, indent=2)}")
            self.redis.publish(channel, json_message)
        else:
            self.logger.warning("[TradeRadar] No Redis handler available for publishing.")

