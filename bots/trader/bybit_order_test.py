import os
import math
import time
import datetime
from pybit.unified_trading import HTTP

# Load your Bybit API credentials
API_KEY = os.getenv("BYBIT_API_KEY", "your_real_key")
API_SECRET = os.getenv("BYBIT_API_SECRET", "your_real_secret")

symbol = "BTCUSDT"
leverage = 10
allocation_percent = 1.0  # 100% of available balance
limit_offset_percent = 0.01  # Enter slightly better than current market
qty_increment = 0.001  # Adjust if needed

session = HTTP(api_key=API_KEY, api_secret=API_SECRET)

try:
    print("\n🔌 Fetching wallet balance...")
    balance_res = session.get_wallet_balance(accountType="UNIFIED")
    coins = balance_res.get("result", {}).get("list", [{}])[0].get("coin", [])
    usdt_info = next((c for c in coins if c["coin"] == "USDT"), {})
    available_str = usdt_info.get("availableToWithdraw") or usdt_info.get("walletBalance")
    usdt_balance = float(available_str or 0.0)
    print(f"💰 Available USDT: {usdt_balance:.2f}")

    print("⚙️ Setting leverage...")
    try:
        session.set_leverage(category="linear", symbol=symbol,
                             buyLeverage=str(leverage), sellLeverage=str(leverage))
        print(f"✅ Leverage set to {leverage}x")
    except Exception as e:
        if "leverage not modified" in str(e):
            print("ℹ️ Leverage already set")
        else:
            print(f"🔥 ERROR: {e}")

    print("📈 Fetching orderbook...")
    orderbook = session.get_orderbook(category="linear", symbol=symbol)
    bid = float(orderbook["result"]["b"][0][0])
    ask = float(orderbook["result"]["a"][0][0])
    limit_price = ask * (1 - limit_offset_percent / 100)
    print(f"📉 Best Ask: {ask:.2f} | 🏷️ Limit Price: {limit_price:.2f}")

    allocation = usdt_balance * allocation_percent
    raw_qty = (allocation * leverage) / limit_price
    qty = math.floor(raw_qty / qty_increment) * qty_increment
    print(f"🧮 Qty Raw: {raw_qty:.8f} | Rounded: {qty:.4f}")

    print("📝 Submitting LIMIT BUY order...")
    response = session.place_order(
        category="linear",
        symbol=symbol,
        side="Buy",
        orderType="Limit",
        qty="f"{qty:.4f}"",
        price=f"{limit_price:.2f}",
        timeInForce="PostOnly"
    )
    print(f"🚀 Order placed: {response}")

except Exception as ex:
    print(f"💥 UNHANDLED ERROR: {ex}")
