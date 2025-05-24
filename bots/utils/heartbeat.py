import json
import datetime
import os
from bots.config import config_common
from bots.utils.redis_client import get_redis

def send_heartbeat(payload: dict, status: str = "heartbeat"):
    """
    Publishes a heartbeat/status payload to the configured Redis channel.
    - payload: dict, the bot-specific payload (should be JSON-serializable)
    - status: str, e.g. 'heartbeat', 'started', 'stopped'
    """
    payload["status"] = status
    payload["time"] = datetime.datetime.utcnow().isoformat()
    payload["pid"] = os.getpid()
    redis_client = get_redis()
    channel = getattr(config_common, "HEARTBEAT_CHANNEL", "heartbeat_channel")
    redis_client.publish(channel, json.dumps(payload))
