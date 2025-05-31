# bots/utils/redis_client.py
"""
Centralised Redis connection factory.

Every bot should import `get_redis()` instead of instantiating redis.Redis
directly, so they all talk to the same host / port / db — and you only
need to change the settings in one place.
"""
import os, redis

from config.config_redis import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    PRE_PROC_KLINE_UPDATES,TRIGGER_QUEUE_CHANNEL
) 
# Optional: one global connection pool (recommended for many threads)
_pool = redis.ConnectionPool(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
)

def get_redis() -> redis.Redis:
    """Return a Redis client that shares the global connection-pool."""
    return redis.Redis(connection_pool=_pool)