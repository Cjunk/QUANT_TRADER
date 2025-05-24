"""
Macro Metrics Services Bot Core

Description:
    Collects macroeconomic and crypto market metrics (dominance, market cap, volume, sentiment, etc.) from public APIs and publishes them to Redis for downstream bots/services. Maintains a heartbeat and status reporting for monitoring and orchestration.

Redis Channels & Keys:
    - config_redis.SERVICE_STATUS_CHANNEL: Publishes bot status (started, stopped) and heartbeats.
    - config_redis.MACRO_METRICS_CHANNEL: Publishes macro metrics reports for consumption by other bots/services.

Key Variables:
    - BOT_NAME: Name of the bot (from config).
    - BOT_AUTH_TOKEN: Authentication token for status payloads.
    - LOG_FILENAME, LOG_LEVEL: Logging configuration.
    - FETCH_INTERVAL: How often to fetch/publish metrics (seconds).
    - MAX_MEMORY_REPORTS: Max number of reports to keep in memory.
    - HEARTBEAT_INTERVAL_SECONDS: Heartbeat interval (seconds).

Usage:
    Instantiate Macro_metrics_services and call run().
    Handles graceful shutdown on KeyboardInterrupt.

Example:
    bot = Macro_metrics_services()
    bot.run()
"""

# =====================================================
# Imports and Configuration
# =====================================================
import time, os, threading, json, sys, datetime, logging, requests
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from collections import deque
from utils.logger import setup_logger
from config.config_auto_macro_metrics_services import BOT_NAME, BOT_AUTH_TOKEN
from config.config_common import HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_CHANNEL
from config.config_auto_macro_metrics_services import LOG_FILENAME, LOG_LEVEL, FETCH_INTERVAL, MAX_MEMORY_REPORTS
from fear_and_greed import FearAndGreedIndex
from bots.utils.redis_client import get_redis
from bots.utils.heartbeat import send_heartbeat
from bots.config import config_redis

# =====================================================
# Macro Metrics Services Bot Class
# =====================================================
class Macro_metrics_services:
    """
    Collects macro and crypto market metrics, publishes to Redis, and maintains a heartbeat.
    """
    def __init__(self, log_filename=LOG_FILENAME):
        """
        Initialize the bot, logger, Redis connection, and in-memory report queue.
        Args:
            log_filename (str): Path to the log file.
        """
        self.auth_token = BOT_AUTH_TOKEN
        self.logger = setup_logger(LOG_FILENAME, getattr(logging, LOG_LEVEL.upper(), logging.WARNING))
        self.redis = get_redis()
        self.reports = deque(maxlen=MAX_MEMORY_REPORTS)
        self.running = True

    # =====================================================
    # Internal Utility Methods (underscore-prefixed)
    # =====================================================
    def _register(self):
        """
        Publish a 'started' status payload to Redis for monitoring/orchestration.
        Includes version, PID, and config metadata.
        """
        payload = {
            "bot_name": BOT_NAME,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": "1.0",
                "pid": os.getpid(),
                "role": "TEMPLATE",
                "metadata": {
                    "Heartbeat": HEARTBEAT_INTERVAL_SECONDS,
                    "LOG_LEVEL": LOG_LEVEL,
                    "value 3": "  "
                }
            }
        }
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"Registered bot '{BOT_NAME}' with status 'started'.")

    def _heartbeat(self):
        """
        Periodically send a heartbeat payload to Redis for liveness monitoring.
        Uses the shared send_heartbeat utility. Runs in a background thread.
        """
        while self.running:
            try:
                payload = {
                    "bot_name": BOT_NAME,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat(),
                    "auth_token": self.auth_token,
                    "metadata": {}
                }
                send_heartbeat(payload, status="heartbeat")
            except Exception as e:
                self.logger.warning(f"Heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)

    def _stop(self):
        """
        Internal stop logic for clean shutdown. Use self.stop() for public interface.
        """
        self.running = False
        self.logger.info("Bot is shutting down...")
        payload = {
            "bot_name": BOT_NAME,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"Sent shutdown status for bot '{BOT_NAME}'.")

    # =====================================================
    # Public Interface Methods
    # =====================================================
    def run(self):
        """
        Main loop: register, start heartbeat, fetch/publish metrics at interval.
        Handles KeyboardInterrupt for graceful shutdown.
        """
        self.logger.info(f"{BOT_NAME} is running...")
        self._register()
        threading.Thread(target=self._heartbeat, daemon=True).start()
        try:
            while self.running:
                self.fetch_metrics()
                time.sleep(FETCH_INTERVAL)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("Bot process exited.")

    def stop(self):
        """
        Public method to stop the bot. Calls internal _stop().
        """
        self._stop()

    # =====================================================
    # Metrics Fetching & Publishing
    # =====================================================
    def fetch_metrics(self):
        """
        Fetch macro and crypto market metrics from APIs, build a report, and publish to Redis.
        Publishes to config_redis.MACRO_METRICS_CHANNEL. Keeps a rolling in-memory history.
        Before: No new metrics in Redis. After: New metrics published and available for other bots.
        """
        try:
            # Fetch global crypto metrics from CoinGecko
            response = requests.get("https://api.coingecko.com/api/v3/global")
            data = response.json().get("data", {})

            # Fetch Fear & Greed Index
            fng = FearAndGreedIndex()
            current_value = fng.get_current_value()
            classification = fng.get_current_classification()

            # Optional: Bitcoin Open Interest
            # open_interest = self.get_binance_open_interest("BTCUSDT")

            # Optional: Inflation Data
            # inflation_data = self.get_inflation_data("US", "YOUR_API_TOKEN")

            report = {
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "btc_dominance": data.get("market_cap_percentage", {}).get("btc"),
                "eth_dominance": data.get("market_cap_percentage", {}).get("eth"),
                "total_market_cap": data.get("total_market_cap", {}).get("usd"),
                "total_volume": data.get("total_volume", {}).get("usd"),
                "active_cryptos": data.get("active_cryptocurrencies"),
                "markets": data.get("markets"),
                "fear_greed_index": current_value,
                "market_sentiment": classification,
                # "btc_open_interest": open_interest,
                # "us_inflation": inflation_data,
            }

            self.reports.append(report)
            self.redis.publish(config_redis.MACRO_METRICS_CHANNEL, json.dumps(report))
            self.logger.info(f"Published macro metrics at {report['timestamp']}")
        except Exception as e:
            self.logger.error(f"Error fetching macro metrics: {e}")

    # =====================================================
    # Static Utility Methods
    # =====================================================
    @staticmethod
    def _get_binance_open_interest(symbol):
        """
        Internal static method to fetch open interest for a symbol from Binance Futures API.
        """
        url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
        response = requests.get(url)
        data = response.json()
        return data['openInterest']

    @staticmethod
    def _get_inflation_data(country, api_token):
        """
        Internal static method to fetch inflation data for a country from EOD Historical Data API.
        """
        url = f"https://eodhd.com/api/macro-indicator/{country}?indicator=inflation_consumer_prices_annual&api_token={api_token}&fmt=json"
        response = requests.get(url)
        data = response.json()
        return data

    # =====================================================
    # Public Static Utility Methods (if needed)
    # =====================================================
    @staticmethod
    def get_binance_open_interest(symbol):
        """
        Public static method to fetch open interest for a symbol from Binance Futures API.
        Usage: Macro_metrics_services.get_binance_open_interest('BTCUSDT')
        """
        return Macro_metrics_services._get_binance_open_interest(symbol)

    @staticmethod
    def get_inflation_data(country, api_token):
        """
        Public static method to fetch inflation data for a country from EOD Historical Data API.
        Usage: Macro_metrics_services.get_inflation_data('US', 'TOKEN')
        """
        return Macro_metrics_services._get_inflation_data(country, api_token)