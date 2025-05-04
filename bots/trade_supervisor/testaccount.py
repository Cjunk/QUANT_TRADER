import requests
import time
import hmac
import hashlib

API_KEY = "SlvilcnCd2pj4ngges"
API_SECRET = "QyWstzzgLTVSN3srvkJp3Xnip9vgeR6dtZtX"

def get_bybit_wallet():
    url = "https://api.bybit.com/v5/account/wallet-balance"
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    account_type = "UNIFIED"

    query_string = f"accountType={account_type}"
    payload = f"{timestamp}{API_KEY}{recv_window}{query_string}"

    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    headers = {
        "X-BAPI-API-KEY": API_KEY,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
    }

    response = requests.get(url, params={"accountType": account_type}, headers=headers)
    return response.json()

if __name__ == "__main__":
    result = get_bybit_wallet()
    print("=== RAW RESPONSE ===")
    print(result)