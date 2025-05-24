"""
📦 Trade Supervisor Bot – Quant Trading Platform
------------------------------------------------
This bot is responsible for managing trading operations at a high level.
It registers itself on startup, sends heartbeats to Redis, and maintains a persistent runtime loop.

✅ Core Tasks:
-------------
- Register itself with Redis using a unique bot name and auth token
- Send heartbeat messages periodically to Redis
- Maintain a lightweight loop, ready for future expansions
- Gracefully shut down with status update

📁 Filename  : trade_core.py
🔧 Entry     : trade_runner.py
🧠 Created by: Jericho Sharman
📅 Year      : 2025
"""

import os
import time
import json
import threading
import datetime
from wallet_sync import get_bybit_wallet, extract_wallet_balances
from config.config_redis import REDIS_HOST, REDIS_PORT, REDIS_DB, SERVICE_STATUS_CHANNEL, HEARTBEAT_CHANNEL
from config.config_ts import BOT_NAME, BOT_AUTH_TOKEN, LOG_FILENAME
from utils.logger import setup_logger
from bots.utils.redis_client import get_redis
from bots.utils.heartbeat import send_heartbeat  # Import the shared heartbeat utility

class TradeSupervisor:
    def __init__(self, log_filename=None):
        """
        Initialize the Trade Supervisor:
        - Set up logger
        - Connect to Redis
        - Initialize control flags
        """
        log_file = log_filename or (LOG_FILENAME + ".log")  # Ensure extension
        self.logger = setup_logger(LOG_FILENAME, log_level="DEBUG")
        self.bot_name = BOT_NAME
        self.auth_token = BOT_AUTH_TOKEN
        self.running = True
        self.heartbeat_interval = 120  # Seconds

        self.redis = get_redis()

    def register(self):
        """
        Register the bot to the Redis service channel with status = started.
        Includes bot name, auth token, timestamp, PID, and version.
        """
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
        self.redis.publish(SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"🔐 Registered bot '{self.bot_name}' with status 'started'.")

    def heartbeat(self):
        """
        Send periodic heartbeat signals to Redis for liveness checks.
        Runs in a background thread.
        """
        # Heartbeat logic is now handled by the shared utility. This method is intentionally minimal.
        while self.running:
            try:
                payload = {
                    "bot_name": self.bot_name,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat(),
                    "auth_token": getattr(self, "auth_token", None),
                    "metadata": {
                        "version": getattr(self, "version", "1.0.0"),
                        "pid": os.getpid(),
                        "strategy": getattr(self, "strategy", "-"),
                        "vitals": {}
                    }
                }
                send_heartbeat(payload, status="heartbeat")
                self.logger.debug("❤️ Sent heartbeat.")
            except Exception as e:
                self.logger.warning(f"Heartbeat failed: {e}")
            time.sleep(self.heartbeat_interval)

    def stop(self):
        """
        Set bot status to 'stopped' and notify Redis before shutting down.
        """
        self.running = False
        self.logger.info("🛑 Stopping bot...")

        payload = {
            "bot_name": self.bot_name,
            "status": "stopped",
            "time": datetime.datetime.utcnow().isoformat(),
            "auth_token": self.auth_token
        }
        self.redis.publish(SERVICE_STATUS_CHANNEL, json.dumps(payload))
        self.logger.info(f"✅ Shutdown notice sent for bot '{self.bot_name}'.")

    def run(self):
        """
        Entry point for the main loop. Starts the heartbeat thread.
        """
        self.logger.info(f"🚀 {self.bot_name} is starting...")
        self.register()

        threading.Thread(target=self.heartbeat, daemon=True).start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()
        finally:
            self.logger.info("👋 Supervisor bot exited cleanly.")


