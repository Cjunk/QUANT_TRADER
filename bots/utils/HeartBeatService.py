import threading
import time
import datetime
import os
import json
from config.config_common import HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_CHANNEL
class HeartBeat:
    def __init__(self, bot_name, auth_token, logger, redis_handler, metadata=None, channel=HEARTBEAT_CHANNEL):
        self.bot_name = bot_name
        self.auth_token = auth_token
        self.logger = logger
        self.redis_handler = redis_handler
        self.interval = HEARTBEAT_INTERVAL_SECONDS
        self.metadata = metadata or {}
        self.channel = channel or "REDIS_CHANNEL_IS_MISSING"
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while self.running:
            try:
                payload = {
                    "bot_name": self.bot_name,
                    "heartbeat": True,
                    "time": datetime.datetime.utcnow().isoformat(),
                    "auth_token": self.auth_token,
                    "metadata": self.metadata
                }
                self.redis_handler.publish(self.channel, json.dumps(payload))
                #self.logger.info(f"Heartbeat sent for {self.bot_name} at {payload['time']}, published on REDIS on channel {self.channel} with {payload['metadata']}")
            except Exception as e:
                self.logger.warning(f"Failed to send heartbeat: {e}")

            time.sleep(self.interval)

    def stop(self):
        self.running = False
        if self.thread.is_alive():
            self.thread.join(timeout=self.interval + 2)
        self.logger.info("Heartbeat service stopped.")