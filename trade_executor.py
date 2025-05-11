import json, time, threading, datetime, os, sys, math, logging, signal
from collections import defaultdict, deque

import websocket
from pybit.unified_trading import HTTP
from dotenv import load_dotenv,find_dotenv
from colorama import init, Fore, Style

import redis
from bots.utils.logger import setup_logger
from bots.utils.redis_client import get_redis
from config_trade_executor import (
    API_KEY, API_SECRET, ACCOUNT_TYPE,
    LEVERAGE, ORDER_VALUE_USDT, MIN_BALANCE_USDT,
    CONFIRM_SIGNALS, CONFIDENCE_MIN,
    TRIGGER_CHANNEL, HEARTBEAT_CHANNEL,
    SERVICE_STATUS_CH, BOT_NAME, LOG_LEVEL, LOG_FILE
)
STOP_LOSS_PERCENT = 0.5  # 0.5% stop-loss
TAKE_PROFIT_PERCENT = 1.0  # 1.0% take-profit
# Load .env
load_dotenv()
dotenv_path = find_dotenv()
loaded = load_dotenv(dotenv_path)

print("Dotenv loaded from:", dotenv_path, "Success:", loaded)
print("BYBIT_API_KEY:", os.getenv("BYBIT_API_KEY"))
print("BYBIT_API_SECRET:", os.getenv("BYBIT_API_SECRET"))
print("Loaded LOG_FILE:", os.getenv("LOG_FILE"))
# Setup Logger
lg = setup_logger(LOG_FILE, getattr(logging, LOG_LEVEL.upper()))
lg.info("✅ Logger initialized with level: %s", LOG_LEVEL)

# Ensure API credentials
if not (API_KEY and API_SECRET):
    lg.error("❌ API_KEY/API_SECRET missing – exiting.")
    sys.exit(1)
else:
    lg.debug("✅ API keys loaded.")

# Set up Bybit session
lg.debug("🔧 Initializing Bybit HTTP session...")
session = HTTP(api_key=API_KEY, api_secret=API_SECRET)

# Set up Redis
lg.debug("🔧 Connecting to Redis...")
redis_cli = get_redis()
pubsub = redis_cli.pubsub()
pubsub.subscribe(TRIGGER_CHANNEL)
open_positions = {}

lg.info("✅ Connected to Redis queue: %s", TRIGGER_CHANNEL)

recent_dir = defaultdict(lambda: deque(maxlen=CONFIRM_SIGNALS))
lg.debug("📦 Recent signals deque initialized (maxlen=%s)", CONFIRM_SIGNALS)


init(autoreset=True)
def display_positions(open_positions):
    for symbol, data in open_positions.items():
        side = data.get('side', 'Unknown').capitalize()
        side_icon = "📈" if side.lower() == "buy" else "📉"
        side_color = Fore.GREEN if side.lower() == "buy" else Fore.RED
        pnl = float(data.get('unrealisedPnl', 0))
        pnl_color = Fore.GREEN if pnl >= 0 else Fore.RED
        status_icon = "✅" if data.get('positionStatus') == 'Normal' else "⚠️"

        print(f"{Fore.CYAN}📌 Open Position: {Fore.YELLOW}{symbol}")
        print(f"  {side_icon} Side:         {side_color}{side.upper()}")
        print(f"  💼 Size:         {Fore.WHITE}{data.get('size')} {symbol[:-4]}")
        print(f"  💰 Avg Price:    {Fore.WHITE}${float(data.get('avgPrice', 0)):,.2f}")
        print(f"  📍 Mark Price:   {Fore.WHITE}${float(data.get('markPrice', 0)):,.2f}")
        print(f"  📊 Unrealized PnL: {pnl_color}${pnl:.4f}")
        print(f"  📉 Cum Realized PnL: {Fore.MAGENTA}${float(data.get('cumRealisedPnl', 0)):,.4f}")
        print(f"  ⚖️ Leverage:     {Fore.WHITE}{data.get('leverage')}x")
        print(f"  🚨 Liquidation:  {Fore.WHITE}${float(data.get('liqPrice', 0)):,.2f}")
        print(f"  📌 Position Value: {Fore.WHITE}${float(data.get('positionValue', 0)):,.2f}")
        print(f"  {status_icon} Status:       {Fore.WHITE}{data.get('positionStatus')}\n")
# Heartbeat Thread
def heartbeat():
    lg.debug("💓 Heartbeat thread started.")
    while True:
        msg = {
            "bot_name": BOT_NAME,
            "heartbeat": True,
            "time": datetime.datetime.utcnow().isoformat()
        }
        redis_cli.publish(HEARTBEAT_CHANNEL, json.dumps(msg))
        lg.debug("💓 Heartbeat sent: %s", msg)
        time.sleep(30)
def update_open_positions():
    try:
        positions = session.get_positions(category="linear", settleCoin='USDT')
        current_positions = positions["result"]["list"]
        open_positions.clear()
        for pos in current_positions:
            if float(pos["size"]) > 0:
                open_positions[pos["symbol"]] = pos
        #lg.debug("🔄 Updated open positions: %s", open_positions)
        display_positions(open_positions)
    except Exception as e:
        lg.error("⚠️ Position update error: %s", e)

# Get USDT Balance
def get_available_usdt() -> float:
    lg.debug("🪙 Fetching available USDT balance...")
    try:
        r = session.get_wallet_balance(accountType=ACCOUNT_TYPE)
        lg.debug("🪙 Balance response: %s", r)
        for coin in r["result"]["list"][0]["coin"]:
            if coin["coin"] == "USDT":
                bal = float(coin.get("totalAvailableBalance") or coin["walletBalance"])
                lg.info("🪙 USDT Balance: %.2f", bal)
                return bal
    except Exception as e:
        lg.error("⚠️ Balance fetch failed: %s", e)
    return 0.0

# Set Leverage
def set_leverage(sym: str):
    lg.debug("🔧 Setting leverage for %s to %d", sym, LEVERAGE)
    try:
        resp = session.set_leverage(
            category="linear", symbol=sym,
            buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE)
        )
        lg.info("✅ Leverage set response: %s", resp)
    except Exception as e:
        if "leverage not modified" in str(e):
            lg.info("ℹ️ Leverage already set – ignoring.")
        else:
            lg.error("⚠️ Leverage error: %s", e)

# Place Market Order
STOP_LOSS_PERCENT = 0.5  # 0.5% stop-loss
TAKE_PROFIT_PERCENT = 1.0  # 1.0% take-profit

def place_market(sym: str, side: str, usdt: float):
    if sym in open_positions:
        lg.info("⚠️ %s already has open trade. Skipping...", sym)
        return

    lg.debug("🛒 Preparing market order: %s %s %.2f USDT", sym, side, usdt)
    try:
        price_data = session.get_tickers(category="linear", symbol=sym)
        lg.debug("📈 Price data: %s", price_data)
        price = float(price_data["result"]["list"][0]["lastPrice"])
        qty = math.floor(((usdt * LEVERAGE) / price) * 1000) / 1000
        lg.info("🛒 Calculated qty: %.3f at price: %.2f", qty, price)

        if qty < 0.001:
            lg.warning("⚠️ Calculated qty %.3f below min order size – skipping.", qty)
            return

        # Calculate Stop Loss and Take Profit
        if side == "long":
            sl_price = price * (1 - STOP_LOSS_PERCENT / 100)
            tp_price = price * (1 + TAKE_PROFIT_PERCENT / 100)
        else:
            sl_price = price * (1 + STOP_LOSS_PERCENT / 100)
            tp_price = price * (1 - TAKE_PROFIT_PERCENT / 100)

        resp = session.place_order(
            category="linear",
            symbol=sym,
            side="Buy" if side == "long" else "Sell",
            orderType="Market",
            qty=f"{qty:.3f}",
            timeInForce="IOC",
            stopLoss=f"{sl_price:.2f}",
            takeProfit=f"{tp_price:.2f}"
        )

        lg.info("🚀 Order placed: %s %s, Response: %s", sym, side, resp)

        open_positions[sym] = {
            "side": side,
            "order_id": resp["result"]["orderId"],
            "qty": qty,
            "sl_price": sl_price,
            "tp_price": tp_price,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        lg.info("🛡️ SL: %.2f | 🎯 TP: %.2f", sl_price, tp_price)

    except Exception as e:
        lg.error("⚠️ Order error: %s", e)


# Graceful shutdown handler
running = True
def signal_handler(sig, frame):
    global running
    lg.info("🚦 Received shutdown signal (Ctrl+C). Exiting gracefully...")
    running = False
    pubsub.close()

signal.signal(signal.SIGINT, signal_handler)

# Start heartbeat
threading.Thread(target=heartbeat, daemon=True).start()

# Replace main loop with Redis Queue (trigger_queue consumer)
lg.info("🟢 Starting Redis queue consumer loop...")
try:
    while running:
        update_open_positions()  # regularly update real open positions from Bybit
        msg = redis_cli.blpop(TRIGGER_CHANNEL, timeout=5)
        if msg:
            queue_name, payload_raw = msg
            lg.debug("📩 Popped message from queue '%s': %s", queue_name, payload_raw)

            payload = json.loads(payload_raw)
            lg.debug("📨 Parsed payload: %s", payload)

            sym = payload["symbol"]
            direction = payload.get("direction")
            conf = float(payload.get("confidence") or 0)

            if direction not in ("long", "short"):
                lg.debug("🚫 Ignored invalid direction: %s", direction)
                continue

            if sym in open_positions:
                lg.info("⚠️ %s already has open trade. Skipping...", sym)
                continue

            dq = recent_dir[sym]
            dq.append(direction)
            lg.debug("📌 Signal window [%s]: %s", sym, list(dq))

            if (len(dq) == CONFIRM_SIGNALS and
                all(x == direction for x in dq) and
                conf >= CONFIDENCE_MIN):

                bal = get_available_usdt()
                if bal < MIN_BALANCE_USDT:
                    lg.warning("⚠️ Low balance (%.2f USDT) – skipping trade.", bal)
                    continue

                set_leverage(sym)
                place_market(sym, direction, ORDER_VALUE_USDT)
                dq.clear()
                lg.debug("♻️ Cleared signal window after execution for %s", sym)

        else:
            lg.debug("⏳ No message received, polling again...")

except KeyboardInterrupt:
    lg.info("🚦 Ctrl+C pressed. Exiting gracefully...")
finally:
    lg.info("👋 Trade executor shutdown complete.")
