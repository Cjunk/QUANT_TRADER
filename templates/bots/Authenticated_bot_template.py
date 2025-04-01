# 📦 Authenticated Bot Template
# Location: templates/authenticated_bot_template/

import time,os
import threading
import json
import redis
import datetime
import config.config_redis as config_redis
from utils.logger import setup_logger
from config.{{CONFIG_MODULE}} import BOT_NAME, BOT_AUTH_TOKEN, HEARTBEAT_INTERVAL


class AuthenticatedBot:
    def __init__(self, logger):
        self.bot_name = BOT_NAME
        self.auth_token = BOT_AUTH_TOKEN
        self.logger = logger
        self.redis = redis.Redis(
            host=config_redis.REDIS_HOST,
            port=config_redis.REDIS_PORT,
            db=config_redis.REDIS_DB,
            decode_responses=True
        )
        self.running = True
        self.heartbeat_interval = HEARTBEAT_INTERVAL  # seconds

    def register(self):
        payload = {
            "bot_name": self.bot_name,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": "1.0",
                "pid": os.getpid()
            }
            
        }
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"🔐 Registered bot '{self.bot_name}' with status 'started'.")

    def heartbeat(self):
        while self.running:
            try:
                payload = {
                    "bot_name": self.bot_name,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat()
                }
                self.redis.publish(config_redis.HEARTBEAT_CHANNEL, json.dumps(payload))
                self.logger.debug("❤️ Sent heartbeat.")
            except Exception as e:
                self.logger.warning(f"Heartbeat failed: {e}")
            time.sleep(self.heartbeat_interval)

    def stop(self):
        self.running = False
        self.logger.info("🛑 Bot is shutting down...")
        payload = {
            "bot_name": self.bot_name,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"✅ Sent shutdown status for bot '{self.bot_name}'.")

    def run(self):
        self.logger.info(f"🚀 {self.bot_name} is running...")
        self.register()
        threading.Thread(target=self.heartbeat, daemon=True).start()
        
        try:
            while self.running:
                # Main loop work goes here
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("👋 Bot process exited.")
