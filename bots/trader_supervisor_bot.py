"""
🧠 Supervisor Bot — Quant Trading Platform
------------------------------------------

The Supervisor Bot is responsible for managing a team of trader bots. It does **not** perform trades itself.
Instead, it handles operational control, launching, stopping, and monitoring trader bots under its command.

📌 Core Responsibilities:
-------------------------
✅ Register itself on startup (with auth token)
✅ Maintain a heartbeat to Redis for liveness checks
✅ Monitor, launch, and control trader bots
✅ Assign strategies, coins, and risk profiles from DB or Redis
✅ Listen to Redis channels for:
   - New assignments
   - Updates from trader bots
   - Instructions from the Manager Bot (if implemented later)

🔁 Supervisor Lifecycle:
-------------------------
1. Authenticates and registers with the DB Bot
2. Sends heartbeats periodically (e.g., every 30 sec)
3. Subscribes to Redis `SUPERVISOR_COMMAND_CHANNEL`
4. Processes commands like:
    - `start_bot`
    - `stop_bot`
    - `resync_strategy`
5. Launches trader bots as subprocesses or threads
6. Monitors statuses, logs errors, escalates if needed

🧱 Requirements:
----------------
- Auth token stored securely in config
- Must have a `bot_name` entry in `bot_roles` table (e.g., `supervisor`)
- PostgreSQL permissions to read from `current_coins`, `bots`, etc.
- Redis channels:
    - `SERVICE_STATUS_CHANNEL`
    - `HEARTBEAT_CHANNEL`
    - `SUPERVISOR_COMMAND_CHANNEL`

🔮 Future Extensions:
---------------------
- Accept commands from a Manager Bot (strategic control)
- Enforce team budget limits from a DB table (e.g., `bot_funds`)
- Restart failed trader bots automatically
- Report performance summaries

👤 Created by: Jericho Sharman
📅 Year: 2025
"""
#   Can manage up to 10 trader bots. 
import time,os
import threading
import json
import redis
import datetime
import config.config_redis as config_redis
from utils.logger import setup_logger
from config.config_ts import BOT_NAME, BOT_AUTH_TOKEN
class TradeSupervisor:
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
        self.heartbeat_interval = 120  # seconds

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
