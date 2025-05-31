"""
Macro Metrics Services Bot Core
Author: Jericho | 2025-05-24

Collects macro/crypto market metrics (dominance, cap, volume, sentiment, etc.) from public APIs and publishes to Redis. Maintains heartbeat/status for monitoring.

Usage:
    bot = MacroMetricsServices(); bot.run()
"""
# === Imports ===
import datetime, json, logging, os, threading, time
from collections import deque
import requests
from utils import setup_logger, get_redis, send_heartbeat
from config import config_redis
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
        self.redis = get_redis()
        self.reports = deque(maxlen=MAX_MEMORY_REPORTS)
        self.running = True

    # --- Public ---
    def run(self):
        self.logger.info(f"{BOT_NAME} is running...")
        self.fetch_metrics()
        self._register()
        threading.Thread(target=self._heartbeat, daemon=True).start()
        try:
            while self.running:
                self.fetch_metrics(); time.sleep(FETCH_INTERVAL)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("Bot process exited.")
    def stop(self): self._stop()

    # --- Private ---
    def _register(self):
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(self._status_payload(status="started")))
        self.logger.info(f"Registered bot '{BOT_NAME}' with status 'started'.")
    def _heartbeat(self):
        while self.running:
            try:
                send_heartbeat(self._status_payload(heartbeat=True), status="heartbeat")
            except Exception as e:
                self.logger.warning(f"Heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)
    def _stop(self):
        self.running = False
        self.logger.info("Bot is shutting down...")
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(self._status_payload(status="stopped")))
        self.logger.info(f"Sent shutdown status for bot '{BOT_NAME}'.")
    def _status_payload(self, status=None, heartbeat=False):
        p = {
            "bot_name": BOT_NAME,
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": "1.0",
                "pid": os.getpid(),
                "role": "macro_metrics",
                "vitals": {
                    "reports_in_memory": len(self.reports),
                    "last_report_time": self.reports[-1]["timestamp"] if self.reports else "-",
                    "fetch_interval": FETCH_INTERVAL,
                    "latest_metrics": self.reports[-1] if self.reports else {}
                },
                "config": {
                    "HEARTBEAT_INTERVAL_SECONDS": HEARTBEAT_INTERVAL_SECONDS,
                    "LOG_LEVEL": LOG_LEVEL,
                    "FETCH_INTERVAL": FETCH_INTERVAL,
                    "MAX_MEMORY_REPORTS": MAX_MEMORY_REPORTS
                }
            }
        }
        if status: p["status"] = status
        if heartbeat: p["heartbeat"] = True
        return p

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
            self.redis.publish(config_redis.MACRO_METRICS_CHANNEL, json.dumps(report))
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