# 📦 Wallet Service Bot 
# Location: templates/authenticated_bot_template/

import time, os, threading, json, datetime
from utils.logger import setup_logger
from utils.redis_handler import RedisHandler
from utils.HeartBeatService import HeartBeat
from config.config_wallet import BOT_NAME, BOT_AUTH_TOKEN
import config.config_redis as config_redis

class WalletService:
    def __init__(self, logger):
        self.bot_name = BOT_NAME
        self.auth_token = BOT_AUTH_TOKEN
        self.logger = logger
        self.redis_handler = RedisHandler(config_redis, self.logger)
        self.redis_handler.connect()
        self.running = True
        self.status = {
            "bot_name": self.bot_name,
            "status": "started",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token,
            "metadata": {
                "version": "1.0",
                "pid": os.getpid()
            }
        }
        self.heartbeat = HeartBeat(
            bot_name=self.bot_name,
            auth_token=self.auth_token,
            logger=self.logger,
            redis_handler=self.redis_handler,
            metadata=self.status
        )

    def register(self):
        self.redis_handler.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(self.status))
        self.logger.info(f"🔐 Registered bot '{self.bot_name}' with status 'started'.")

    def stop(self):
        self.running = False
        self.logger.info("🛑 Bot is shutting down...")
        payload = {
            "bot_name": self.bot_name,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis_handler.publish(config_redis.SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"✅ Sent shutdown status for bot '{self.bot_name}'.")

    def run(self):
        self.logger.info(f"🚀 {self.bot_name} is running...")
        self.register()
        try:
            while self.running:
                # Main loop work goes here
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("👋 Bot process exited.")

if __name__ == "__main__":
    logger = setup_logger("wallet_service.log")
    WalletService(logger).run()
