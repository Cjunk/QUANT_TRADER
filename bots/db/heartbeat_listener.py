import threading
import time
import json
import datetime
import redis

class HeartbeatListener:
    """
    Listens for heartbeat messages on Redis and updates bot status in the database.
    """
    def __init__(self, bot, config_redis, heartbeat_channel, logger):
        self.bot = bot  # Reference to PostgresDBBot for DB and status updates
        self.config_redis = config_redis
        self.heartbeat_channel = heartbeat_channel
        self.logger = logger
        self.running = True
        self.heartbeat_timeout = bot.heartbeat_timeout

    def start(self):
        threading.Thread(target=self._listen_heartbeat, daemon=True).start()

    def stop(self):
        self.running = False

    def _listen_heartbeat(self):
        heartbeat_client = redis.Redis(
            host=self.config_redis.REDIS_HOST,
            port=self.config_redis.REDIS_PORT,
            db=self.config_redis.REDIS_DB,
            decode_responses=True
        )
        pubsub = heartbeat_client.pubsub()
        pubsub.subscribe(self.heartbeat_channel)

        while self.running:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            now = datetime.datetime.utcnow()
            if message and message["type"] == "message":
                try:
                    payload = json.loads(message["data"])
                    bot_name = payload.get("bot_name")
                    timestamp = payload.get("time")
                    if bot_name and timestamp:
                        if bot_name not in self.bot.status or self.bot.status[bot_name]['status'] == 'stopped':
                            status_obj = {
                                "bot_name": bot_name,
                                "status": "started",
                                "time": timestamp,
                                "auth_token": payload.get("auth_token", ""),
                                "metadata": payload.get("metadata", {})
                            }
                            self.bot.status_handler.handle_bot_status_update(status_obj)
                        self.bot._update_bot_last_seen(bot_name, timestamp)
                        self.bot.bot_status[bot_name] = {'last_seen': now, 'status': 'started'}
                except Exception as e:
                    self.logger.error(f"Failed to handle heartbeat message: {e}")
            for bot, info in list(self.bot.bot_status.items()):
                last_seen = info['last_seen']
                if not last_seen or (now - last_seen).total_seconds() > self.heartbeat_timeout:
                    if info['status'] != 'stopped':
                        self.logger.warning(f"No heartbeat from {bot} for over {self.heartbeat_timeout} seconds. Marking as stopped.")
                        self.bot._mark_bot_stopped(bot, now)
                        self.bot.bot_status[bot]['status'] = 'stopped'
            time.sleep(0.5)