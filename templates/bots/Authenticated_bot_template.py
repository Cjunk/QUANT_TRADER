# 📦 Authenticated Bot Template
# Location: templates/authenticated_bot_template/

import time,os
import threading
import json
import redis
import datetime
import logging
import config.config_redis as config_redis
import pytz  # For timezone handling
from utils.logger import setup_logger
from config.{{CONFIG_MODULE}} import BOT_NAME, BOT_AUTH_TOKEN, HEARTBEAT_INTERVAL
from config.{{CONFIG_MODULE}} import LOG_FILENAME, LOG_LEVEL

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class AuthenticatedBot:
    def __init__(self, logger):
        self.auth_token = BOT_AUTH_TOKEN
        self.logger = setup_logger(LOG_FILENAME,getattr(logging, LOG_LEVEL.upper(), logging.WARNING)) # Set up logger and retrieve the logger type from config 
        self.redis = redis.Redis(
            host=config_redis.REDIS_HOST,
            port=config_redis.REDIS_PORT,
            db=config_redis.REDIS_DB,
            decode_responses=True
        )
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
                "role": "TEMPLATE"      
        }}
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"🔐 Registered bot '{BOT_NAME}' with status 'started'.")

    def _heartbeat(self):
        while self.running:
            try:
                payload = {
                    "bot_name": BOT_NAME,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat()
                }
                self.redis.publish(config_redis.HEARTBEAT_CHANNEL, json.dumps(payload))
                self.logger.debug("❤️ Sent heartbeat.")
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
                # Main loop work goes here
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("👋 Bot process exited.")
