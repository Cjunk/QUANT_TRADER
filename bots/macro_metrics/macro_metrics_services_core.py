# 📦 Authenticated Bot Template
# Location: templates/authenticated_bot_template/

import time,os
import threading
import json
import redis
import sys
import datetime
import logging
import requests
import config.config_redis as config_redis

import pytz  # For timezone handling
from collections import deque
from utils.logger import setup_logger
from config.config_auto_macro_metrics_services import BOT_NAME, BOT_AUTH_TOKEN, HEARTBEAT_INTERVAL
from config.config_auto_macro_metrics_services import LOG_FILENAME, LOG_LEVEL,FETCH_INTERVAL,MAX_MEMORY_REPORTS
from fear_and_greed import FearAndGreedIndex



class Macro_metrics_services:
    def __init__(self, log_filename=LOG_FILENAME):
        self.auth_token = BOT_AUTH_TOKEN
        self.logger = setup_logger(LOG_FILENAME,getattr(logging, LOG_LEVEL.upper(), logging.WARNING)) # Set up logger and retrieve the logger type from config 
        self.redis = redis.Redis(
            host=config_redis.REDIS_HOST,
            port=config_redis.REDIS_PORT,
            db=config_redis.REDIS_DB,
            decode_responses=True
        )
        self.reports = deque(maxlen=MAX_MEMORY_REPORTS)
        self.running = True

    def _register(self):
        payload = {
            "bot_name": BOT_NAME,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": "1.0",
                "pid": os.getpid(),
                "role": "TEMPLATE",
                "metadata": {  # Its here where bot specific metadata can be added
                    "Heartbeat": HEARTBEAT_INTERVAL,
                    "LOG_LEVEL": LOG_LEVEL,
                    "value 3": "  "                  
            }
       
        }}
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"🔐 Registered bot '{BOT_NAME}' with status 'started'.")
    def get_binance_open_interest(symbol):
        url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
        response = requests.get(url)
        data = response.json()
        return data['openInterest']
    def get_inflation_data(country, api_token):
        url = f"https://eodhd.com/api/macro-indicator/{country}?indicator=inflation_consumer_prices_annual&api_token={api_token}&fmt=json"
        response = requests.get(url)
        data = response.json()
        return data
    def _heartbeat(self):
        while self.running:
            try:
                payload = {
                    "bot_name": BOT_NAME,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat()
                }
                self.redis.publish(config_redis.HEARTBEAT_CHANNEL, json.dumps(payload))
                #self.logger.debug("❤️ Sent heartbeat.")
            except Exception as e:
                self.logger.warning(f"Heartbeat failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL)

    def stop(self):
        self.running = False
        self.logger.info("🛑 Bot is shutting down...")
        payload = {
            "bot_name": BOT_NAME,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"✅ Sent shutdown status for bot '{BOT_NAME}'.")

    def run(self):
        self.logger.info(f"🚀 {BOT_NAME} is running...")
        self._register()
        threading.Thread(target=self._heartbeat, daemon=True).start()     
        try:
            while self.running:
                self.fetch_metrics()
                time.sleep(FETCH_INTERVAL)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("👋 Bot process exited.")
    def fetch_metrics(self):
        try:
            response = requests.get("https://api.coingecko.com/api/v3/global")
            data = response.json().get("data", {})
            
            # Fear & Greed Index
            fng = FearAndGreedIndex()
            current_value = fng.get_current_value()
            classification = fng.get_current_classification()

            # Bitcoin Open Interest
            #open_interest = get_binance_open_interest("BTCUSDT")

            # Inflation Data
            #inflation_data = get_inflation_data("US", "YOUR_API_TOKEN")

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
                #"btc_open_interest": open_interest,
                #"us_inflation": inflation_data,
            }

            self.reports.append(report)
            self.redis.publish(config_redis.MACRO_METRICS_CHANNEL, json.dumps(report))
            self.logger.info(f"Published macro metrics at {report['timestamp']}")
        except Exception as e:
            self.logger.error(f"Error fetching macro metrics: {e}")

    def get_binance_open_interest(symbol):
        url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
        response = requests.get(url)
        data = response.json()
        return data['openInterest']