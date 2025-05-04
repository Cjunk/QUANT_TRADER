import time, hmac, hashlib, requests
from datetime import datetime

def get_open_positions(api_key, api_secret):
    url = "https://api.bybit.com/v5/position/list"
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    category = "linear"  # USDT perpetuals
    params = {
        "category": "linear",
        "settleCoin": "USDT"
    }
    query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    payload = f"{timestamp}{api_key}{recv_window}{query_string}"

    signature = hmac.new(
        api_secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
    }

    response = requests.get(
        url,
        params = {
          "category": category,
          "settleCoin": "USDT"
      },
        headers=headers
    )
    print("=== RAW POSITION RESPONSE ===")
    print(response.text)
    return response.json()
