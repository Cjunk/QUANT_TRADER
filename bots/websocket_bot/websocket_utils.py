import logging, requests
from config_websocket_bot import LOG_LEVEL


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