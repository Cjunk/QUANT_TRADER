"""
Macro Metrics Services Bot Core
Author: Jericho | 2025-05-24

Collects macro/crypto market metrics (dominance, cap, volume, sentiment, etc.) from public APIs and publishes to Redis. Maintains heartbeat/status for monitoring.

Usage:
    bot = MacroMetricsServices(); bot.run()
"""
# === Imports ===
import datetime, json, logging, os, time
from collections import deque
import requests
from utils import setup_logger
from config import config_redis
from utils.redis_handler import RedisHandler
from utils.HeartBeatService import HeartBeat
from config.config_common import HEARTBEAT_INTERVAL_SECONDS
from config_auto_macro_metrics_services import (
    BOT_NAME, BOT_AUTH_TOKEN, LOG_FILENAME, LOG_LEVEL, FETCH_INTERVAL, MAX_MEMORY_REPORTS
)
from fear_and_greed import FearAndGreedIndex

# === Bot Class ===
class MacroMetricsServices:
    def __init__(self, log_filename=LOG_FILENAME):
        self.auth_token = BOT_AUTH_TOKEN
        self.logger = setup_logger(LOG_FILENAME, getattr(logging, LOG_LEVEL.upper(), logging.WARNING))
        self.redis_handler = RedisHandler(config_redis, self.logger)
        self.redis_handler.connect()
        self.redis_client = self.redis_handler.client
        self.reports = deque(maxlen=MAX_MEMORY_REPORTS)
        self.running = True
        self.status = {
            "bot_name": BOT_NAME,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": "1.0",
                "pid": os.getpid(),
                "role": "macro_metrics",
                "vitals": {},
                "config": {
                    "HEARTBEAT_INTERVAL_SECONDS": HEARTBEAT_INTERVAL_SECONDS,
                    "LOG_LEVEL": LOG_LEVEL,
                    "FETCH_INTERVAL": FETCH_INTERVAL,
                    "MAX_MEMORY_REPORTS": MAX_MEMORY_REPORTS
                }
            }
        }
        self.heartbeat = HeartBeat(
            bot_name=BOT_NAME,
            auth_token=self.auth_token,
            logger=self.logger,
            redis_handler=self.redis_handler,
            metadata=self.status
        )

    # --- Public ---
    def run(self):
        self.logger.info(f"{BOT_NAME} is running...")
        try:
            while self.running:
                self.fetch_metrics()
                time.sleep(FETCH_INTERVAL)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("Bot process exited.")

    def stop(self):
        self.running = False
        self.logger.info("Bot is shutting down...")

    # --- Metrics ---
    def fetch_metrics(self):
        try:
            data = requests.get("https://api.coingecko.com/api/v3/global").json().get("data", {})
            fng = FearAndGreedIndex()
            report = {
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "btc_dominance": data.get("market_cap_percentage", {}).get("btc"),
                "eth_dominance": data.get("market_cap_percentage", {}).get("eth"),
                "total_market_cap": data.get("total_market_cap", {}).get("usd"),
                "total_volume": data.get("total_volume", {}).get("usd"),
                "active_cryptos": data.get("active_cryptocurrencies"),
                "markets": data.get("markets"),
                "fear_greed_index": fng.get_current_value(),
                "market_sentiment": fng.get_current_classification(),
            }
            self.reports.append(report)
            # --- Update vitals before heartbeat ---
            self.status["metadata"]["vitals"] = {
                "reports_in_memory": len(self.reports),
                "last_report_time": report["timestamp"],
                "total_reports": getattr(self, "total_reports", 0) + 1
            }
            self.total_reports = self.status["metadata"]["vitals"]["total_reports"]
            self.redis_handler.publish(config_redis.MACRO_METRICS_CHANNEL, json.dumps(report))
            self.logger.info(f"Published macro metrics at {report['timestamp']}")
        except Exception as e:
            self.logger.error(f"Error fetching macro metrics: {e}")

    # --- Static Utils ---
    @staticmethod
    def get_binance_open_interest(symbol):
        url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
        return requests.get(url).json()['openInterest']

    @staticmethod
    def get_inflation_data(country, api_token):
        url = f"https://eodhd.com/api/macro-indicator/{country}?indicator=inflation_consumer_prices_annual&api_token={api_token}&fmt=json"
        return requests.get(url).json()