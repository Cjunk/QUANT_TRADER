# Step 2: minimal TriggerBot + analysis preview. Still Redis-only; now computes a confidence and can enqueue a test signal.
import os, sys, json, threading, time
from typing import Optional
import pandas as pd
# If running as a script inside /bots/trigger
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import setup_logger
from utils.redis_handler import RedisHandler
import config.config_redis as redis_cfg
from config.config_redis import PRE_PROC_KLINE_UPDATES, TRIGGER_QUEUE_CHANNEL
import pandas as pd
from utils.db_postgres import PostgresHandler

# Config (keep tiny)
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "25"))
LOG_FILENAME = os.getenv("LOG_FILENAME", "trigger_bot.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MIN_CONFIDENCE = 30
FORCE_SIGNAL = os.getenv("FORCE_SIGNAL", "0") == "1"

# one global logger to avoid duplicate handlers
logger = setup_logger(LOG_FILENAME, LOG_LEVEL)


class TriggerBot:
    """Minimal bot: subscribe to kline updates, check Redis window length, log it, and compute confidence."""

    def __init__(self, market: str):
        self.market = market
        self.logger = logger
        self.running = True
        self.db = PostgresHandler(self.logger)
        self.db_conn = self.db.conn
        # Redis
        self.redis = RedisHandler(redis_cfg, self.logger)
        self.redis.connect()
        self.client = self.redis.client
        self.pubsub = self.redis.pubsub
        self.pubsub.subscribe(PRE_PROC_KLINE_UPDATES)
        self.logger.info(f"[{self.market}] Subscribed to {PRE_PROC_KLINE_UPDATES!r}; WINDOW_SIZE={WINDOW_SIZE}")
        self.last_emitted = {}  # {(market, symbol, interval): last_candle_start_iso}

    # ---- Redis helpers ----
    def _window_key(self, symbol: str, interval: str) -> str:
        return f"kline_window:{self.market}:{symbol}:{interval}"

    def _llen(self, key: str) -> int:
        try:
            n = self.client.llen(key)
            return int(n or 0)
        except Exception as e:
            self.logger.warning(f"LLEN failed for {key}: {e}")
            return 0

    def _read_window(self, symbol: str, interval: str):
        key = self._window_key(symbol, interval)
        items = self.client.lrange(key, -WINDOW_SIZE, -1) or []
        out = []
        for it in items:
            try:
                out.append(json.loads(it))
            except Exception:
                pass
        return out
    def _insert_signal_log(self, *, symbol, interval, signal_type,
                        value=None, context=None, confidence=None,
                        direction=None, window_start=None, window_end=None):
        cur = self.db_conn.cursor()
        try:
            cur.execute("""
                INSERT INTO trading.signal_log
                (symbol, "interval", signal_type, value, context, confidence, direction, window_start, window_end)
                VALUES
                (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            """, (
                symbol, interval, signal_type,
                float(value) if value is not None else None,
                json.dumps(context or {}),
                float(confidence) if confidence is not None else None,
                direction,
                window_start,  # naive timestamp OK (table is TIMESTAMP WITHOUT TIME ZONE)
                window_end
            ))
            self.db_conn.commit()
            self.logger.info(f"🧠 logged to DB: {signal_type} {symbol}-{interval}")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"❌ DB insert failed: {e}", exc_info=True)
        finally:
            cur.close()

    # ---- Listener ----
    def listen(self):
        for message in self.pubsub.listen():
            if not self.running:
                break
            if message.get("type") != "message":
                continue
            try:
                payload = json.loads(message["data"])  # expected: {symbol, interval, market}
            except Exception as e:
                self.logger.error(f"JSON decode error: {e} | raw={message.get('data')!r}")
                continue

            mkt = payload.get("market")
            sym = payload.get("symbol")
            iv  = payload.get("interval")
            

            if not (mkt and sym and iv) or mkt != self.market:
                continue
            self.logger.info(f"[rx] {mkt} {sym}-{iv}")
            key = self._window_key(sym, iv)
            n = self._llen(key)
            ready = n >= WINDOW_SIZE
            status = "READY" if ready else f"{n}/{WINDOW_SIZE}"
            self.logger.info(f"[{self.market}] {sym}-{iv} window: {status} | {key}")

            if ready:
                try:
                    self.analyze(sym, iv)
                except Exception as e:
                    self.logger.error(f"analyze failed for {sym}-{iv}: {e}", exc_info=True)

    # ---- Analysis (preview) ----
    def analyze(self, symbol: str, interval: str):
        win = self._read_window(symbol, interval)
        if len(win) < WINDOW_SIZE:
            return
        df = pd.DataFrame(win)

        # -- only act on confirmed candles (if field exists)
        if "confirmed" in df.columns and not bool(df["confirmed"].iloc[-1]):
            return

        # -- dedupe: only one signal per candle start_time
        dedupe_ts = None
        if "start_time" in df.columns:
            last_ts = pd.to_datetime(df["start_time"].iloc[-1], utc=True, errors="coerce")
            if pd.notna(last_ts):
                dedupe_ts = last_ts.tz_convert(None).isoformat()
                key = (self.market, symbol, interval)
                if self.last_emitted.get(key) == dedupe_ts:
                    return

        # required + optional columns
        req = ["close", "volume", "Volume_MA", "RSI", "MACD", "MACD_Signal"]
        opt_bands = ["UpperBand", "LowerBand"]

        if any(c not in df.columns for c in req):
            self.logger.info(f"[{self.market}] {symbol}-{interval} waiting for indicators; have cols={sorted(df.columns)}")
            return

        # numeric cast
        for c in req:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        for c in opt_bands:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # require the 'req' columns to be non-null; bands are optional
        df = df.dropna(subset=req)
        if len(df) < WINDOW_SIZE:
            return

        # confidence preview
        rvol = float(df["volume"].iloc[-1]) / (float(df["Volume_MA"].iloc[-1]) + 1e-8)
        price_slope = (df["close"].iloc[-1] - df["close"].iloc[0]) / max(df["close"].iloc[0], 1e-8)
        rsi_slope   = (df["RSI"].iloc[-1]   - df["RSI"].iloc[0])   / 100.0
        macd_slope  = (df["MACD"].iloc[-1]  - df["MACD"].iloc[0])  / (abs(df["MACD"].iloc[0]) + 1e-8)

        price_score = min(max(price_slope, -0.05), 0.05) / 0.05
        rsi_score   = min(max(rsi_slope,   -0.5),  0.5)  / 0.5
        macd_score  = min(max(macd_slope,  -1.0),  1.0)
        rvol_score  = min(rvol / 3.0, 1.0)
        confidence  = (price_score*0.4 + rsi_score*0.25 + macd_score*0.2 + rvol_score*0.15) * 100.0

        # log (include bands if present)
        if all(b in df.columns for b in opt_bands):
            self.logger.info(
                f"[{self.market}] {symbol}-{interval} conf={confidence:.1f} "
                f"rvol={rvol:.2f} rsi={df['RSI'].iloc[-1]:.1f} macd={df['MACD'].iloc[-1]:.2f} "
                f"bb=({df['LowerBand'].iloc[-1]:.2f},{df['UpperBand'].iloc[-1]:.2f})"
            )
        else:
            self.logger.info(
                f"[{self.market}] {symbol}-{interval} conf={confidence:.1f} "
                f"rvol={rvol:.2f} rsi={df['RSI'].iloc[-1]:.1f} macd={df['MACD'].iloc[-1]:.2f}"
            )

        # emit if forced or over threshold
        if FORCE_SIGNAL or confidence >= MIN_CONFIDENCE:
            direction = "long" if df["close"].iloc[-1] >= df["close"].iloc[-2] else "short"

            # context (includes bands if present)
            context = {
                "close": float(df["close"].iloc[-1]),
                "volume": float(df["volume"].iloc[-1]),
                "volume_ma": float(df["Volume_MA"].iloc[-1]),
                "rsi": float(df["RSI"].iloc[-1]),
                "macd": float(df["MACD"].iloc[-1]),
                "macd_signal": float(df["MACD_Signal"].iloc[-1]),
                "market": self.market,
            }
            if "UpperBand" in df.columns:
                context["upper_band"] = float(df["UpperBand"].iloc[-1])
            if "LowerBand" in df.columns:
                context["lower_band"] = float(df["LowerBand"].iloc[-1])

            # window times
            window_start = window_end = None
            if "start_time" in df.columns:
                ts_all = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
                if len(ts_all.dropna()) > 0:
                    window_start = ts_all.min().tz_convert(None).to_pydatetime()
                    window_end   = ts_all.max().tz_convert(None).to_pydatetime()

            # queue payload
            payload = {
                "symbol": symbol,
                "interval": interval,
                "signal_type": "trend_confidence" if not FORCE_SIGNAL else "test_force",
                "value": round(float(confidence), 2),
                "confidence": round(float(confidence), 2),
                "market": self.market,
                "direction": direction,
                "context": context,
                "window_start": window_start.isoformat() if window_start else None,
                "window_end": window_end.isoformat() if window_end else None,
            }
            self.client.rpush(TRIGGER_QUEUE_CHANNEL, json.dumps(payload))
            self.logger.info(f"queued signal -> {payload}")

            # DB insert
            self._insert_signal_log(
                symbol=symbol, interval=interval, signal_type=payload["signal_type"],
                value=payload["value"], context=context, confidence=payload["confidence"],
                direction=direction, window_start=window_start, window_end=window_end
            )

            # mark dedupe
            if dedupe_ts:
                self.last_emitted[(self.market, symbol, interval)] = dedupe_ts


    # ---- Run ----
    def run(self):
        t = threading.Thread(target=self.listen, daemon=True)
        t.start()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False


if __name__ == "__main__":
    print(time.strftime('%Y-%m-%d %H:%M:%S'), "🚀 Starting TRIGGER_BOT step 1 (wire test)...")
    bot_linear = TriggerBot("linear")
    bot_spot   = TriggerBot("spot")

    t1 = threading.Thread(target=bot_linear.run)
    t2 = threading.Thread(target=bot_spot.run)
    t1.start(); t2.start()
    t1.join(); t2.join()

