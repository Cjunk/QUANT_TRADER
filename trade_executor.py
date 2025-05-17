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
    CONFIRM_SIGNALS, CONFIRMATION_REQUIRED,CONFIDENCE_MIN,
    TRIGGER_CHANNEL, HEARTBEAT_CHANNEL,STOP_LOSS_PERCENT,TAKE_PROFIT_PERCENT,
    SERVICE_STATUS_CH, BOT_NAME, LOG_LEVEL, LOG_FILE,LIMIT_OFFSET_PERCENT
)
SYMBOL_QTY_INCREMENT = {
    'ETHUSDT': 0.01,
    'XRPUSDT': 1,
    'SOLUSDT': 0.01,
    'DOGEUSDT': 1,
    'ADAUSDT': 1,
    # add more symbols as needed
}
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

def round_qty(symbol: str, qty: float) -> float:
    increment = SYMBOL_QTY_INCREMENT.get(symbol, 0.001)  # default to 0.001 if missing
    return math.floor(qty / increment) * increment
init(autoreset=True)
def display_positions(open_positions):
    for symbol, data in open_positions.items():
        side = data.get('side', 'Unknown').capitalize()
        side_icon = "📈" if side.lower() == "buy" else "📉"
        side_color = Fore.GREEN if side.lower() == "buy" else Fore.RED
        pnl = float(data.get('unrealisedPnl', 0))
        pnl_color = Fore.GREEN if pnl >= 0 else Fore.RED
        status_icon = "✅" if data.get('positionStatus') == 'Normal' else "⚠️"

        print(f"{Fore.CYAN}📌 Open Position: {Fore.YELLOW}{symbol}    {side_icon} Side:         {side_color}{side.upper()}")
        print(f"  💼 Size:         {Fore.WHITE}{data.get('size')} {symbol[:-4]}")
        print(f"  💰 Avg Price:    {Fore.WHITE}${float(data.get('avgPrice', 0)):,.2f}   📍 Mark Price:   {Fore.WHITE}${float(data.get('markPrice', 0)):,.2f}")
        print(f"  📊 Unrealized PnL: {pnl_color}${pnl:.4f}      📉 Cum Realized PnL: {Fore.MAGENTA}${float(data.get('cumRealisedPnl', 0)):,.4f}")
        print(f"  ⚖️ Leverage:     {Fore.WHITE}{data.get('leverage')}x      🚨 Liquidation:  {Fore.WHITE}${float(data.get('liqPrice', 0)):,.2f}")
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

def place_limit_order(sym: str, side: str, usdt: float):
    if sym in open_positions:
        lg.info("⚠️ %s already has open trade. Skipping...", sym)
        return

    lg.debug("🛒 Preparing limit order: %s %s %.2f USDT", sym, side, usdt)
    try:
        orderbook = session.get_orderbook(category="linear", symbol=sym)
        best_bid = float(orderbook["result"]["b"][0][0])
        best_ask = float(orderbook["result"]["a"][0][0])

        limit_price = best_bid * (1 - LIMIT_OFFSET_PERCENT / 100) if side == "long" else best_ask * (1 + LIMIT_OFFSET_PERCENT / 100)
        raw_qty = (usdt * LEVERAGE) / limit_price
        qty = round_qty(sym, raw_qty)

        lg.info("🛒 Qty rounded: %.3f at limit price: %.4f", qty, limit_price)

        min_qty = SYMBOL_QTY_INCREMENT.get(sym, 0.001)
        if qty < min_qty:
            lg.warning("⚠️ Qty %.3f below min order size (%.3f). Skipping.", qty, min_qty)
            return

        # Calculate SL & TP
        sl_price = limit_price * (1 - STOP_LOSS_PERCENT / 100) if side == "long" else limit_price * (1 + STOP_LOSS_PERCENT / 100)
        tp_price = limit_price * (1 + TAKE_PROFIT_PERCENT / 100) if side == "long" else limit_price * (1 - TAKE_PROFIT_PERCENT / 100)

        resp = session.place_order(
            category="linear",
            symbol=sym,
            side="Buy" if side == "long" else "Sell",
            orderType="Limit",
            qty=f"{qty}",
            price=f"{limit_price:.4f}",
            timeInForce="PostOnly",
            stopLoss=f"{sl_price:.4f}",
            takeProfit=f"{tp_price:.4f}"
        )

        lg.info("🚀 Limit order placed: %s %s, Response: %s", sym, side, resp)

        open_positions[sym] = {
            "side": side,
            "order_id": resp["result"]["orderId"],
            "qty": qty,
            "limit_price": limit_price,
            "sl_price": sl_price,
            "tp_price": tp_price,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        lg.info("📌 Limit: %.4f | 🛡️ SL: %.4f | 🎯 TP: %.4f", limit_price, sl_price, tp_price)

    except Exception as e:
        lg.error("⚠️ Limit order error: %s", e)
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
recent_signals = defaultdict(lambda: deque(maxlen=CONFIRM_SIGNALS))
lg.debug("📦 Recent signals initialized (maxlen=%s)", CONFIRM_SIGNALS)
# Replace main loop with Redis Queue (trigger_queue consumer)
# Main loop (Redis queue consumer)
lg.info("🟢 Starting Redis queue consumer loop...")
try:
    while running:
        update_open_positions()
        msg = redis_cli.blpop(TRIGGER_CHANNEL, timeout=5)

        if msg:
            queue_name, payload_raw = msg
            lg.debug("📩 Popped message from '%s': %s", queue_name, payload_raw)

            payload = json.loads(payload_raw)
            sym = payload["symbol"]
            interval = payload["interval"]
            direction = payload.get("direction")
            conf = float(payload.get("confidence", 0))
            signal_type = payload.get("signal_type")
            ctx = payload["context"]
            close, rsi, macd = ctx["close"], ctx["rsi"], ctx["macd"]
            volume, volume_ma = ctx["volume"], ctx["volume_ma"]
            window_start, window_end = payload["window_start"][11:16], payload["window_end"][11:16]

            # Comprehensive logging
            log_msg = (f"📥 Signal: {sym} | {interval}m | {signal_type} | "
                       f"{'🚀 Long' if direction=='long' else '📉 Short'} | "
                       f"🎯 Conf: {conf:.2f}% (Min: {CONFIDENCE_MIN}%) | "
                       f"🕒 Window: {window_start}->{window_end} | Close: ${close:.2f} | "
                       f"RSI: {rsi:.2f} | MACD: {macd:.4f} | Vol: {volume:.2f} (Avg: {volume_ma:.2f})")

            reason_skipped = None
            if direction not in ("long", "short"):
                reason_skipped = "Invalid direction"
            elif sym in open_positions:
                reason_skipped = "Position already open"
            elif conf < CONFIDENCE_MIN:
                reason_skipped = f"Confidence below {CONFIDENCE_MIN}%"
            elif get_available_usdt() < MIN_BALANCE_USDT:
                reason_skipped = "Insufficient balance"

            if reason_skipped:
                lg.info(f"{log_msg} | ❌ Skipped: {reason_skipped}")
                continue

            # Store detailed signal info
            recent_signals[sym].append({
                "direction": direction,
                "signal_type": signal_type,
                "confidence": conf,
                "timestamp": datetime.datetime.utcnow()
            })

            lg.debug("📌 Recent signals for [%s]: %s", sym, list(recent_signals[sym]))

            # Check for required consecutive aligned signals
            required_signals = CONFIRMATION_REQUIRED.get(interval, CONFIRM_SIGNALS)
            signals_list = list(recent_signals[sym])

            if len(signals_list) >= required_signals:
                last_signals = signals_list[-required_signals:]
                directions_aligned = all(sig["direction"] == direction for sig in last_signals)

                if directions_aligned:
                    bal = get_available_usdt()
                    if bal < MIN_BALANCE_USDT:
                        lg.warning("⚠️ Low balance (%.2f USDT). Skipping.", bal)
                        continue

                    set_leverage(sym)
                    place_limit_order(sym, direction, ORDER_VALUE_USDT)
                    recent_signals[sym].clear()
                    lg.debug("♻️ Cleared signals after execution for %s", sym)
                else:
                    lg.debug("🟡 Signals not aligned yet: %s", last_signals)
            else:
                lg.debug("🟡 Waiting for more signals (%d/%d)", len(signals_list), required_signals)

        else:
            lg.debug("⏳ No message received, polling again...")

except KeyboardInterrupt:
    lg.info("🚦 Ctrl+C pressed. Exiting gracefully...")
finally:
    lg.info("👋 Shutdown complete.")
