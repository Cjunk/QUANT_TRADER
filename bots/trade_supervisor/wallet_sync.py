import time
import hmac
import hashlib
import requests
from datetime import datetime
from position_sync import get_open_positions
# ========== 🔐 Signing & Request Helpers ==========

def _build_v5_headers(api_key, api_secret, query_string=""):
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
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
    return headers


# ========== 🌐 API Calls ==========

def get_v5_wallet_balances(api_key, api_secret, account_type):
    query_string = f"accountType={account_type}"
    headers = _build_v5_headers(api_key, api_secret, query_string)

    response = requests.get(
        "https://api.bybit.com/v5/account/wallet-balance",
        params={"accountType": account_type},
        headers=headers
    )
    return response.json(), account_type


def get_funding_wallet_v5(api_key, api_secret):
    url = "https://api.bybit.com/v5/asset/transfer/query-account-coins-balance"
    account_type = "FUND"

    query_string = f"accountType={account_type}"
    headers = _build_v5_headers(api_key, api_secret, query_string)

    response = requests.get(
        url,
        params={"accountType": account_type},
        headers=headers
    )

    return response.json(), "FUNDING"


# ========== 🧹 Normalizer ==========

def normalize_wallet_response(api_response, wallet_type):
    balances = []
    timestamp = datetime.utcnow().isoformat()

    if wallet_type in ["UNIFIED", "CONTRACT"]:
        accounts = api_response.get("result", {}).get("list", [])
        for account in accounts:
            for coin_data in account.get("coin", []):
                coin = coin_data.get("coin")
                total = float(coin_data.get("walletBalance", "0") or 0)
                available = float(coin_data.get("availableToWithdraw", "0") or 0)
                if available == 0:
                    available = float(coin_data.get("equity", "0") or 0)

                balances.append({
                    "coin": coin,
                    "wallet_type": wallet_type,
                    "total": total,
                    "available": available,
                    "timestamp": timestamp
                })

    elif wallet_type == "FUNDING":
        for coin_data in api_response.get("result", {}).get("balance", []):
            balances.append({
                "coin": coin_data["coin"],
                "wallet_type": wallet_type,
                "total": float(coin_data.get("walletBalance", "0")),
                "available": float(coin_data.get("transferBalance", "0")),
                "timestamp": timestamp
            })

    return balances


# ========== 🔁 Merge All Wallet Types ==========

def get_all_wallet_balances(api_key, api_secret):
    all_balances = []

    for wallet_type in ["UNIFIED", "CONTRACT"]:
        resp, acc_type = get_v5_wallet_balances(api_key, api_secret, account_type=wallet_type)
        if resp.get("retCode") == 0:
            all_balances.extend(normalize_wallet_response(resp, wallet_type))

    resp, acc_type = get_funding_wallet_v5(api_key, api_secret)
    if resp.get("retCode") == 0:
        all_balances.extend(normalize_wallet_response(resp, acc_type))

    return all_balances


# ========== 📄 Pretty Print Test ==========

def pretty_print_balances(balances):
    if not balances:
        print("❌ No balances found.")
        return

    print("=== Wallet Balances ===")
    for b in balances:
        print(f"[{b['wallet_type']}] {b['coin']} — Total: {b['total']:.8f} | Available: {b['available']:.8f}")
def extract_positions(api_response):
    positions = []
    timestamp = datetime.utcnow().isoformat()

    pos_list = api_response.get("result", {}).get("list", [])
    for p in pos_list:
        size = float(p.get("size", "0"))
        if size == 0:
            continue  # Skip closed positions

        positions.append({
            "symbol": p["symbol"],
            "side": p["side"],
            "entry_price": float(p.get("avgPrice", "0")),
            "size": size,
            "leverage": float(p.get("leverage", "1")),
            "unrealised_pnl": float(p.get("unrealisedPnl", "0")),
            "position_value": float(p.get("positionValue", "0")),
            "take_profit": float(p.get("takeProfit", "0") or 0),
            "stop_loss": float(p.get("stopLoss", "0") or 0),
            "timestamp": timestamp
        })

    return positions


if __name__ == "__main__":
    # TEMP: hardcode one account's API key/secret for testing
    API_KEY = "SlvilcnCd2pj4ngges"
    API_SECRET = "QyWstzzgLTVSN3srvkJp3Xnip9vgeR6dtZtX"
    balances = get_all_wallet_balances(API_KEY, API_SECRET)
    pretty_print_balances(balances)
    #raw_response = get_bybit_wallet(API_KEY, API_SECRET)

    # Print the full response in formatted JSON
    import json
    print("=== RAW BYBIT WALLET RESPONSE ===")
    raw = get_open_positions(API_KEY, API_SECRET)
    positions = extract_positions(raw)

    print("=== OPEN POSITIONS ===")
    for p in positions:
        print(f"{p['symbol']} | {p['side']} | Size: {p['size']} | Entry: {p['entry_price']} | PnL: {p['unrealised_pnl']} | SL: {p['stop_loss']} | TP: {p['take_profit']}")



