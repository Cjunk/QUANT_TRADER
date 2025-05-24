# bots/utils/__init__.py
from .logger import setup_logger      # re-export
from .redis_client import get_redis   # re-export
from .heartbeat import send_heartbeat  # re-export
__all__ = ["setup_logger", "get_redis", "send_heartbeat"]  # (optional) for linters / autocomplete