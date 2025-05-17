
import json, time, threading, datetime, os, sys, math, logging, signal
from collections import defaultdict, deque

from pybit.unified_trading import HTTP
from dotenv import load_dotenv, find_dotenv
from colorama import init, Fore

import redis
from bots.utils.logger import setup_logger
from bots.utils.redis_client import get_redis
from config_trade_executor import (
    API_KEY, API_SECRET, ACCOUNT_TYPE, LEVERAGE, ORDER_VALUE_USDT, MIN_BALANCE_USDT,
    CONFIRM_SIGNALS, CONFIRMATION_REQUIRED, CONFIDENCE_MIN,
    TRIGGER_CHANNEL, HEARTBEAT_CHANNEL, STOP_LOSS_PERCENT, TAKE_PROFIT_PERCENT,
    BOT_NAME, LOG_LEVEL, LOG_FILE, LIMIT_OFFSET_PERCENT
)
# Percentage of available balance to use per trade
ORDER_SIZE_PERCENT = 10  # example: 10% of available balance per trade

# Default qty increment (works universally)
DEFAULT_QTY_INCREMENT = 0.001
SYMBOL_QTY_INCREMENT = {
    'ETHUSDT': 0.01, 'XRPUSDT': 1, 'SOLUSDT': 0.01, 'DOGEUSDT': 1, 'ADAUSDT': 1
}

load_dotenv(find_dotenv())
lg = setup_logger(LOG_FILE, getattr(logging, LOG_LEVEL.upper()))

session = HTTP(api_key=API_KEY, api_secret=API_SECRET, recv_window=10000)
redis_cli = get_redis()
pubsub = redis_cli.pubsub()
pubsub.subscribe(TRIGGER_CHANNEL)

open_positions = {}
recent_signals = defaultdict(lambda: deque(maxlen=CONFIRM_SIGNALS))

init(autoreset=True)

def round_qty(symbol: str, qty: float) -> float:
    increment = SYMBOL_QTY_INCREMENT.get(symbol, 0.001)
    return math.floor(qty / increment) * increment

def display_positions():
    try:
        positions = session.get_positions(category="linear", settleCoin='USDT')["result"]["list"]
        open_positions.clear()
        for pos in positions:
            if float(pos["size"]) > 0:
                open_positions[pos["symbol"]] = pos
                side = pos["side"].capitalize()
                pnl = float(pos["unrealisedPnl"])
                print(f"{Fore.CYAN}📌 {Fore.YELLOW}{pos['symbol']} {Fore.GREEN if side=='Buy' else Fore.RED}{side} "
                      f"{Fore.WHITE}{pos['size']} @ ${pos['avgPrice']} | PnL: {Fore.GREEN if pnl>=0 else Fore.RED}{pnl:.4f}")
    except Exception as e:
        lg.error("⚠️ Position update error: %s", e)

def heartbeat():
    while True:
        redis_cli.publish(HEARTBEAT_CHANNEL, json.dumps({
            "bot_name": BOT_NAME, "heartbeat": True, "time": datetime.datetime.utcnow().isoformat()
        }))
        time.sleep(30)

def get_available_usdt() -> float:
    try:
        coins = session.get_wallet_balance(accountType=ACCOUNT_TYPE)["result"]["list"][0]["coin"]
        for coin in coins:
            if coin["coin"] == "USDT":
                bal = float(coin.get("totalAvailableBalance") or coin["walletBalance"])
                lg.info(f"🪙 USDT Balance: {bal:.2f}")
                return bal
    except Exception as e:
        lg.error("⚠️ Balance fetch failed: %s", e)
    return 0.0

def set_leverage(sym: str):
    try:
        session.set_leverage(category="linear", symbol=sym, buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE))
    except Exception as e:
        if "leverage not modified" not in str(e):
            lg.error("⚠️ Leverage error: %s", e)

def place_limit_order(sym: str, side: str, usdt: float):
    if sym in open_positions:
        lg.info("⚠️ %s already has open trade. Skipping...", sym)
        return

    try:
        bal = get_available_usdt()
        trade_usdt = bal * (ORDER_SIZE_PERCENT / 100)
        
        orderbook = session.get_orderbook(category="linear", symbol=sym)
        best_bid = float(orderbook["result"]["b"][0][0])
        best_ask = float(orderbook["result"]["a"][0][0])

        limit_price = best_bid * (1 - LIMIT_OFFSET_PERCENT / 100) if side == "long" else best_ask * (1 + LIMIT_OFFSET_PERCENT / 100)
        raw_qty = trade_usdt * LEVERAGE / limit_price
        qty = math.floor(raw_qty / DEFAULT_QTY_INCREMENT) * DEFAULT_QTY_INCREMENT

        lg.info("🛒 Limit Order: %s | Side: %s | Qty: %.3f | Price: %.4f | Using %.2f%% of balance (%.2f USDT)",
                sym, side.upper(), qty, limit_price, ORDER_SIZE_PERCENT, trade_usdt)

        min_qty = DEFAULT_QTY_INCREMENT
        if qty < min_qty:
            lg.warning("⚠️ Qty %.3f below min order size (%.3f). Skipping.", qty, min_qty)
            return

        sl_price = limit_price * (1 - STOP_LOSS_PERCENT / 100) if side == "long" else limit_price * (1 + STOP_LOSS_PERCENT / 100)
        tp_price = limit_price * (1 + TAKE_PROFIT_PERCENT / 100) if side == "long" else limit_price * (1 - TAKE_PROFIT_PERCENT / 100)

        resp = session.place_order(
            category="linear",
            symbol=sym,
            side="Buy" if side == "long" else "Sell",
            orderType="Limit",
            qty=f"{qty:.3f}",
            price=f"{limit_price:.4f}",
            timeInForce="PostOnly",
            stopLoss=f"{sl_price:.4f}",
            takeProfit=f"{tp_price:.4f}"
        )

        lg.info("🚀 Order Placed: %s | %s | Qty: %.3f @ %.4f | SL: %.4f | TP: %.4f | Response: %s",
                sym, side.upper(), qty, limit_price, sl_price, tp_price, resp)

        open_positions[sym] = {
            "side": side,
            "order_id": resp["result"]["orderId"],
            "qty": qty,
            "limit_price": limit_price,
            "sl_price": sl_price,
            "tp_price": tp_price,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

    except Exception as e:
        lg.error("⚠️ Limit order error: %s", e)


running = True
def signal_handler(sig, frame):
    global running
    running = False
    pubsub.close()

signal.signal(signal.SIGINT, signal_handler)
threading.Thread(target=heartbeat, daemon=True).start()

lg.info("🟢 Redis consumer started")
try:
    while running:
        display_positions()
        msg = redis_cli.blpop(TRIGGER_CHANNEL, timeout=5)
        if msg:
            payload = json.loads(msg[1])
            sym, interval, direction = payload["symbol"], payload["interval"], payload["direction"]
            conf, signal_type, ctx = payload["confidence"], payload["signal_type"], payload["context"]
            close, macd = ctx["close"], ctx["macd"]
            window = f"{payload['window_start'][11:16]}→{payload['window_end'][11:16]}"
            lg.info(f"📩 {sym} | {interval}m | {signal_type} {direction.upper()} 🚦{conf:.1f}% | 💲{close:.2f} MACD:{macd:.4f} ⌛{window}")
            if conf < CONFIDENCE_MIN or sym in open_positions or get_available_usdt() < MIN_BALANCE_USDT:
                continue
            recent_signals[sym].append(direction)
            req = CONFIRMATION_REQUIRED.get(interval, CONFIRM_SIGNALS)
            if len(recent_signals[sym]) >= req and len(set(list(recent_signals[sym])[-req:])) == 1:
                set_leverage(sym)
                place_limit_order(sym, direction, ORDER_VALUE_USDT)
                recent_signals[sym].clear()
except KeyboardInterrupt:
    pass
finally:
    lg.info("👋 Shutdown complete.")
