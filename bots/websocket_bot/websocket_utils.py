import requests
from config_websocket_bot import LOG_LEVEL
import time

def send_webhook(url, message, retries=3, delay=2):
    """
    Sends a Discord webhook message with error logging and retry logic.
    
    Args:
        url (str): Discord webhook URL
        message (str): Message content to send
        retries (int): Number of retries on failure
        delay (int): Delay (in seconds) between retries
    """
    payload = {"content": message}

    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, json=payload, timeout=5)
            
            if response.status_code == 204:
                print(f"✅ Webhook sent: {message}")
                return True
            else:
                print(f"⚠️ Webhook failed (status={response.status_code}): {response.text}")
        
        except Exception as e:
            print(f"❌ Exception sending webhook (attempt {attempt}): {e}")

        time.sleep(delay)

    print(f"🚫 Final webhook send failed after {retries} attempts.")
    return False

def extract_symbol(data):
    return data.get("topic", "").split(".")[-1]

def extract_update_seq(data, symbol):
    return data.get("data", {}).get("u")

def is_snapshot(data):
    d = data.get("data", {})
    return data.get("type") == "snapshot" or d.get("action") == "snapshot"