import logging, requests
from config.config_ws import LOG_LEVEL

def setup_logger(logfile, level):
    logger = logging.getLogger("ws")
    handler = logging.FileHandler(logfile)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s")
    handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

def send_webhook(url, message):
    try:
        return requests.post(url, json={"content": message, "username": "Webby01"}).status_code == 204
    except Exception:
        return False

def extract_symbol(data):
    return data.get("topic", "").split(".")[-1]

def extract_update_seq(data, symbol):
    return data.get("data", {}).get("u")

def is_snapshot(data):
    d = data.get("data", {})
    return data.get("type") == "snapshot" or d.get("action") == "snapshot"