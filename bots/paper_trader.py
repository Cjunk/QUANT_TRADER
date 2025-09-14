# paper_trader.py
import os, sys, json, time, threading, datetime as dt
from typing import Dict, Tuple
from dotenv import load_dotenv

# ----- Path + .env -----
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

# ----- Imports -----
import pytz
from utils.logger import setup_logger
from utils.redis_handler import RedisHandler
from utils.db_postgres import PostgresHandler
import config.config_redis as R  # must expose PRE_PROC_KLINE_UPDATES, TRIGGER_QUEUE_CHANNEL

# ----- Config (env overrides allowed) -----
NOTIONAL_PER_TRADE = float(os.getenv("PAPER_NOTIONAL", "1000"))   # $1k per trade
FEE_RATE           = float(os.getenv("PAPER_FEE_RATE", "0.0005")) # 0.05% per side
MIN_CONF           = float(os.getenv("PAPER_MIN_CONF", "30"))     # ignore weak signals
MAX_CONCURRENT     = int(os.getenv("PAPER_MAX_CON", "8"))         # cap positions
HOLD_BARS          = int(os.getenv("PAPER_HOLD_BARS", "8"))       # time stop in bars
ALLOWED_INTERVALS  = set(os.getenv("PAPER_ALLOWED_INTV", "1,5,60").split(","))
STOP_LOSS_PCT      = float(os.getenv("PAPER_STOP_LOSS_PCT", "0.03"))  # 3% stop loss
MAX_RISK_PER_TRADE = float(os.getenv("PAPER_MAX_RISK", "0.2"))  # 20% of notional per trade
COOLDOWN_SEC       = int(os.getenv("TRIGGER_COOLDOWN_SEC", "300"))  # 5 minutes
CONF_MARGIN        = float(os.getenv("TRIGGER_CONF_MARGIN", "15"))  # confidence must beat previous by this much

def _json_loads(data):
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="ignore")
    return json.loads(data)

class PaperTrader:
    def __init__(self):
        self.logger = setup_logger("paper_trader.log")
        # Redis
        self.redis = RedisHandler(R, self.logger)
        self.redis.connect()
        # Prices via PubSub
        self.pubsub_px = self.redis.client.pubsub()
        self.pubsub_px.subscribe(R.PRE_PROC_KLINE_UPDATES)

        # DB
        self.db = PostgresHandler(self.logger)
        self.conn = self.db.conn
        self._ensure_tables()

        # State
        self.last_close: Dict[Tuple[str, str], float] = {}  # (symbol, interval) -> last close
        self.positions: Dict[int, dict] = {}                # id -> position dict
        self.last_signal_state = {}  # (symbol, interval) -> {"side": ..., "confidence": ..., "timestamp": ..."}

        self.logger.info("✅ PaperTrader ready.")
        print("✅ PaperTrader constructed.")

    # ----- DB bootstrap -----
    def _ensure_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS trading;
            CREATE TABLE IF NOT EXISTS trading.paper_trades (
              id            BIGSERIAL PRIMARY KEY,
              opened_at     TIMESTAMPTZ NOT NULL,
              closed_at     TIMESTAMPTZ,
              symbol        TEXT NOT NULL,
              interval      TEXT NOT NULL,
              side          TEXT CHECK (side IN ('long','short')) NOT NULL,
              qty           DOUBLE PRECISION NOT NULL,
              entry_price   DOUBLE PRECISION NOT NULL,
              exit_price    DOUBLE PRECISION,
              holding_bars  INT DEFAULT 0,
              fees          DOUBLE PRECISION DEFAULT 0,
              pnl           DOUBLE PRECISION,
              signal_json   JSONB
            );
        """)
        self.conn.commit()
        cur.close()

    # ----- open/close -----
    def _open_position(self, symbol, interval, side, qty, price, hold_bars, signal_json):
        fee_open = price * qty * FEE_RATE
        # Calculate stop loss price
        if side == "long":
            stop_loss = price * (1 - STOP_LOSS_PCT)
        else:  # short
            stop_loss = price * (1 + STOP_LOSS_PCT)
        # Risk check: max loss per trade
        max_loss = NOTIONAL_PER_TRADE * MAX_RISK_PER_TRADE
        if side == "long":
            risk = (price - stop_loss) * qty
        else:
            risk = (stop_loss - price) * qty
        if risk > max_loss:
            self.logger.info(f"Risk for trade {symbol}-{interval}-{side} exceeds max allowed ({risk:.2f} > {max_loss:.2f}), skipping.")
            return

        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO trading.paper_trades
            (opened_at, symbol, interval, side, qty, entry_price, holding_bars, fees, signal_json)
            VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (symbol, interval, side, qty, price, 0, fee_open, json.dumps(signal_json)))
        pid = cur.fetchone()[0]
        self.conn.commit(); cur.close()

        self.positions[pid] = dict(
            symbol=symbol, interval=interval, side=side, qty=qty,
            entry=price, bars=0, hold_bars=hold_bars, fees=fee_open,
            stop_loss=stop_loss
        )
        self.logger.info(f"🟢 OPEN {pid} {side} {symbol}-{interval} @ {price:.6f} qty={qty:.6f} stop_loss={stop_loss:.6f} risk={risk:.2f}")

    def _close_position(self, pid, price, reason="time_stop"):
        pos = self.positions.pop(pid, None)
        if not pos:
            return
        side, qty, entry = pos["side"], float(pos["qty"]), float(pos["entry"])

        gross = (price - entry) * qty if side == "long" else (entry - price) * qty
        fee_close = price * qty * FEE_RATE
        pnl = gross - pos["fees"] - fee_close
        fees_total = pos["fees"] + fee_close

        cur = self.conn.cursor()
        cur.execute("""
            UPDATE trading.paper_trades
               SET closed_at = NOW(),
                   exit_price = %s,
                   pnl = %s,
                   fees = %s,
                   holding_bars = %s
             WHERE id = %s
        """, (price, pnl, fees_total, pos["bars"], pid))
        self.conn.commit(); cur.close()

        self.logger.info(f"🔴 CLOSE {pid} @{price:.6f} reason={reason} pnl={pnl:.2f} bars={pos['bars']}")

    # ----- price stream listener (PubSub) -----
    def _price_loop(self):
        for msg in self.pubsub_px.listen():
            if msg.get("type") != "message":
                continue
            try:
                k = _json_loads(msg["data"])
                sym = k.get("symbol"); itv = str(k.get("interval"))
                close = k.get("close")
                if sym and itv and close is not None:
                    close = float(close)
                    self.last_close[(sym, itv)] = close
                    # advance bars for open positions on this (sym,itv)
                    for pid, pos in list(self.positions.items()):
                        if pos["symbol"] == sym and pos["interval"] == itv:
                            pos["bars"] += 1
                            # --- STOP LOSS LOGIC ---
                            stop_loss = pos.get("stop_loss")
                            side = pos["side"]
                            if side == "long" and close <= stop_loss:
                                self._close_position(pid, close, reason="stop_loss")
                                continue
                            elif side == "short" and close >= stop_loss:
                                self._close_position(pid, close, reason="stop_loss")
                                continue
                            # --- TIME STOP LOGIC ---
                            if pos["bars"] >= pos["hold_bars"]:
                                self._close_position(pid, close, reason="time_stop")
            except Exception as e:
                self.logger.error(f"Price loop error: {e}", exc_info=False)

    # ----- signal listener (LIST -> BLPOP) -----
    def _signal_loop(self):
        chan = R.TRIGGER_QUEUE_CHANNEL
        self.logger.info(f"Listening for signals via BLPOP on list: {chan}")
        while True:
            try:
                item = self.redis.client.blpop(chan, timeout=5)
                if not item:
                    continue
                _, raw = item
                sig = _json_loads(raw)

                sym        = sig.get("symbol")
                itv        = str(sig.get("interval"))
                direction  = sig.get("direction")       # 'long' or 'short'
                conf       = float(sig.get("confidence") or 0.0)
                market     = sig.get("market", "spot")
                ts         = sig.get("timestamp", None)

                # --- Signal validation ---
                if not sym or itv not in ALLOWED_INTERVALS or conf < MIN_CONF or direction not in ("long","short"):
                    self.logger.debug(f"Signal failed basic validation: {sig}")
                    continue
                if market != "linear":
                    self.logger.debug(f"Skipping signal for market={market} (not linear).")
                    continue
                price = self.last_close.get((sym, itv))
                if price is None:
                    self.logger.debug(f"No price yet for {sym}-{itv}, skipping signal.")
                    continue
                # Check for duplicate/conflicting positions
                for pos in self.positions.values():
                    if pos["symbol"] == sym and pos["interval"] == itv and pos["side"] == direction:
                        self.logger.info(f"Already have {direction} position for {sym}-{itv}, skipping duplicate.")
                        break
                else:
                    # Check for stale signals (older than 2 bars)
                    now = dt.datetime.utcnow().timestamp()
                    if ts and now - float(ts) > 2 * 60:  # assuming 1-min bars
                        self.logger.info(f"Signal for {sym}-{itv} is stale, skipping.")
                        continue
                    if len(self.positions) >= MAX_CONCURRENT:
                        self.logger.info("Max concurrent positions reached; skipping new entry.")
                        continue
                    qty = max(NOTIONAL_PER_TRADE / price, 0.0)
                    if qty <= 0.0:
                        self.logger.debug(f"Qty calculation failed for {sym}-{itv}, skipping.")
                        continue

                    key = (sym, itv)
                    now = time.time()
                    last = self.last_signal_state.get(key)

                    block = False
                    if last:
                        # If opposite direction, check cooldown and confidence margin
                        if last["side"] != direction:
                            time_since = now - last["timestamp"]
                            if time_since < COOLDOWN_SEC:
                                if conf < last["confidence"] + CONF_MARGIN:
                                    self.logger.info(
                                        f"Blocking {direction} for {sym}-{itv}: cooldown active ({int(time_since)}s < {COOLDOWN_SEC}s) "
                                        f"and confidence {conf} < {last['confidence']}+{CONF_MARGIN}"
                                    )
                                    block = True
                    if block:
                        continue

                    # If not blocked, update state and process signal
                    self.last_signal_state[key] = {
                        "side": direction,
                        "confidence": conf,
                        "timestamp": now
                    }

                    # proceed to enqueue signal or trigger trade
                    self._open_position(sym, itv, direction, qty, price, HOLD_BARS, sig)

            except Exception as e:
                self.logger.error(f"Signal loop error: {e}", exc_info=False)

    # ----- run -----
    def run(self):
        t1 = threading.Thread(target=self._price_loop, daemon=True)
        t2 = threading.Thread(target=self._signal_loop, daemon=True)
        t1.start(); t2.start()
        self.logger.info("🚀 PaperTrader running...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("🛑 Stopping PaperTrader.")

if __name__ == "__main__":
    print("🚀 Starting PaperTrader...")
    trader = PaperTrader()
    trader.run()
