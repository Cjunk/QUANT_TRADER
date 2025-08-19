# --- trigger_core.py (lean, Redis-first) ---
import os, sys, time, json, threading, datetime, logging, requests
import pandas as pd

# Ensure project imports work when run as script
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.name == "nt":
    sys.stdout.reconfigure(encoding="utf-8")
FORCE_SIGNAL = os.getenv("FORCE_SIGNAL", "0") == "1"
LOG_CONF = os.getenv("LOG_CONF", "1") == "1"

from config_trigger_bot import (
    WINDOW_SIZE, MA_WINDOW, MIN_CONFIDENCE, DEV_MODE,
    INTERVALS, BOT_NAME, LOG_FILENAME, LOG_LEVEL, BOT_AUTH_TOKEN
)
from utils.redis_handler import RedisHandler
from utils.db_postgres import PostgresHandler
from utils.logger import setup_logger
from utils.HeartBeatService import HeartBeat
import config.config_redis as redis_cfg
from config.config_redis import PRE_PROC_KLINE_UPDATES, TRIGGER_QUEUE_CHANNEL
try:
    from config.config_redis import SERVICE_STATUS_CHANNEL
except Exception:
    SERVICE_STATUS_CHANNEL = "SERVICE_STATUS_CHANNEL"

DB_BOT_BASE_URL = os.getenv("DB_BOT_BASE_URL", "http://db_bot:8001")


class TriggerBot:
    """Listens to pre-processed kline updates, pulls rolling windows from Redis, evaluates, and logs/emits signals."""

    def __init__(self, market: str = "spot"):
        self.market = market
        self.WINDOW_SIZE = WINDOW_SIZE
        self.MA_WINDOW = MA_WINDOW
        self.MIN_CONFIDENCE = MIN_CONFIDENCE
        self.DEV_MODE = DEV_MODE

        self.logger = setup_logger(LOG_FILENAME, LOG_LEVEL)
        self.running = True

        # Redis
        self.redis = RedisHandler(redis_cfg, self.logger)
        self.redis.connect()
        self.redis_client = self.redis.client
        self.pubsub = self.redis.pubsub
        self.pubsub.subscribe(PRE_PROC_KLINE_UPDATES)
        self.logger.info(f"[{self.market}] TriggerBot config: WINDOW_SIZE={self.WINDOW_SIZE}, "
                         f"MA_WINDOW={self.MA_WINDOW}, MIN_CONFIDENCE={self.MIN_CONFIDENCE}, DEV_MODE={self.DEV_MODE}")
        self.logger.info(f"Subscribed to Redis channel: {PRE_PROC_KLINE_UPDATES!r}")

        # DB (signals only)
        self.db = PostgresHandler(self.logger)
        self.db_conn = self.db.conn

        # State
        self.known_symbols: set[str] = set()      # populated from DB-bot API
        self.windows: dict[tuple[str, str], list[dict]] = {}  # optional mirror for logging/emit context

        # Heartbeat/status
        self.bot_name = BOT_NAME
        self.auth_token = BOT_AUTH_TOKEN
        self.version = "1.0.0"
        self.strategy = "trend_trigger"
        self.heartbeat = HeartBeat(
            bot_name=self.bot_name,
            auth_token=self.auth_token,
            logger=self.logger,
            redis_handler=self.redis,
            metadata={"version": self.version, "pid": os.getpid(), "strategy": self.strategy, "vitals": {}},
        )

    # -------------------- Symbols --------------------
    def _fetch_symbols_via_api(self) -> set[str]:
        """GET /subscriptions/{market} from DB-bot; accept list[str] or list[dict]."""
        url = f"{DB_BOT_BASE_URL}/subscriptions/{self.market}"
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json() or []
        if isinstance(data, list) and data and isinstance(data[0], dict):
            # common shapes: {"symbol": "...", "topic": "kline.1"} or {"symbol": "..."}
            return {item.get("symbol") for item in data if item.get("symbol")}
        elif isinstance(data, list):
            return set(map(str, data))
        else:
            return set()

    def _symbol_poll_loop(self):
        """Poll DB-bot every 10 min for subscribed symbols; log adds/removes."""
        while self.running:
            try:
                symbols = self._fetch_symbols_via_api()
                added = symbols - self.known_symbols
                removed = self.known_symbols - symbols
                if added:
                    self.logger.info(f"[{self.market}] New symbols: {sorted(added)}")
                if removed:
                    self.logger.info(f"[{self.market}] Removed symbols: {sorted(removed)}")
                self.known_symbols = symbols
            except Exception as e:
                self.logger.warning(f"[{self.market}] symbol poll failed: {e}")
            time.sleep(600)  # 10 minutes

    # -------------------- Redis helpers --------------------
    def _redis_window(self, symbol: str, interval: str) -> list[dict]:
        """Read the authoritative rolling window from Redis."""
        key = f"kline_window:{self.market}:{symbol}:{interval}"
        items = self.redis_client.lrange(key, -self.WINDOW_SIZE, -1)
        return [json.loads(i) for i in items] if items else []

    @staticmethod
    def _normalize_fields(rec: dict) -> dict:
        """Map snake_case to expected names (idempotent)."""
        mapping = {
            "rsi": "RSI", "macd": "MACD", "macd_signal": "MACD_Signal", "macd_hist": "MACD_Hist",
            "ma": "MA", "upper_band": "UpperBand", "lower_band": "LowerBand",
            "volume_ma": "Volume_MA", "volume_change": "Volume_Change", "volume_slope": "Volume_Slope",
            "rvol": "RVOL",
        }
        out = dict(rec)
        for src, dst in mapping.items():
            if src in rec and dst not in rec:
                out[dst] = rec[src]
        return out

    # -------------------- Preload (optional, Redis-only) --------------------
    def preload_recent_klines(self):
        """On boot: use current API symbols & check if any windows are already full in Redis."""
        try:
            self.known_symbols = self._fetch_symbols_via_api()
            loaded = 0
            for symbol in sorted(self.known_symbols):
                for interval in list(INTERVALS) + ["D"]:
                    win = self._redis_window(symbol, interval)
                    if len(win) == self.WINDOW_SIZE:
                        self.windows[(symbol, interval)] = win
                        loaded += 1
                    else:
                        # Just log; runtime will use Redis each tick anyway
                        msg = f"{len(win)}/{self.WINDOW_SIZE}" if win else "no Redis window"
                        self.logger.info(f"[{self.market}] {symbol}-{interval}: {msg}; waiting for live feed.")
            self.logger.info(f"[{self.market}] ✅ Preloaded {loaded} full windows from Redis.")
        except Exception as e:
            self.logger.error(f"❌ preload_recent_klines failed: {e}", exc_info=True)

    # -------------------- Listener --------------------
    def _start_redis_listener(self):
        threading.Thread(target=self.listen_redis, daemon=True).start()

    def listen_redis(self):
        for message in self.pubsub.listen():
            payload = json.loads(message["data"])
            self.logger.info(f"[rx] {payload.get('market')} {payload.get('symbol')}-{payload.get('interval')}") 
            if not self.running:
                break
            if message.get("type") != "message":
                continue
            try:
                payload = json.loads(message["data"])
                self.process_kline(payload)
            except Exception as e:
                self.logger.error(f"❌ Error handling kline message: {e} | Message: {message}", exc_info=True)

    # -------------------- Tick --------------------
    def process_kline(self, payload: dict):
        """On every kline message: read the window from Redis and evaluate (no local rolling)."""
        if payload.get("market") != self.market:
            return
        symbol = payload.get("symbol")
        interval = payload.get("interval")
        if not symbol or not interval:
            return

        # Pull authoritative window
        win = self._redis_window(symbol, interval)
        if len(win) != self.WINDOW_SIZE:
            self.logger.debug(f"[{self.market}] {symbol}-{interval} short ({len(win)}/{self.WINDOW_SIZE}); skip")
            return

        # Normalize + DataFrame
        win = [self._normalize_fields(r) for r in win]
        df = pd.DataFrame(win)

        # Daily bias from Redis 'D' if present
        daily_win = self._redis_window(symbol, "D")
        daily_df = pd.DataFrame([self._normalize_fields(r) for r in daily_win]) if daily_win else None

        # Keep a tiny mirror for emit/log context
        self.windows[(symbol, interval)] = win
        self.analyze_trend(symbol, interval, df, daily_df)

    # -------------------- Analysis --------------------
    def analyze_trend(self, symbol: str, interval: str, df_raw: pd.DataFrame, daily_df: pd.DataFrame | None):
        # Ensure required cols and enough candles
        alias = {"rsi": "RSI", "macd": "MACD", "macd_signal": "MACD_Signal",
                 "volume_ma": "Volume_MA", "upper_band": "UpperBand", "lower_band": "LowerBand"}
        df = df_raw.rename(columns={c: alias[c] for c in alias if c in df_raw.columns}).copy()
        required = ["close", "volume", "Volume_MA", "RSI", "MACD", "MACD_Signal", "UpperBand", "LowerBand"]
        if any(c not in df.columns for c in required) or len(df) < self.WINDOW_SIZE:
            self.logger.debug(f"[{self.market}] {symbol}-{interval}: missing/short -> "
                              f"{[c for c in required if c not in df.columns]}; len={len(df)}")
            return

        if "start_time" in df.columns:
            df.loc[:, "start_time"] = pd.to_datetime(df["start_time"], utc=True, errors="coerce")
        num_cols = ["close", "RSI", "MACD", "MACD_Signal", "Volume_MA", "volume", "UpperBand", "LowerBand"]
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
        df = df.dropna()
        if len(df) < self.WINDOW_SIZE:
            return

        # Simple composite confidence (placeholder – plug your rules here)
        rvol = float(df["volume"].iloc[-1]) / (float(df["Volume_MA"].iloc[-1]) + 1e-8)
        price_slope = (df["close"].iloc[-1] - df["close"].iloc[0]) / max(df["close"].iloc[0], 1e-8)
        rsi_slope = (df["RSI"].iloc[-1] - df["RSI"].iloc[0]) / 100.0
        macd_slope = (df["MACD"].iloc[-1] - df["MACD"].iloc[0]) / (abs(df["MACD"].iloc[0]) + 1e-8)

        price_score = min(max(price_slope, -0.05), 0.05) / 0.05
        rsi_score   = min(max(rsi_slope,   -0.5),  0.5) / 0.5
        macd_score  = min(max(macd_slope,  -1.0),  1.0)
        rvol_score  = min(rvol / 3.0, 1.0)
        confidence  = (price_score*0.4 + rsi_score*0.25 + macd_score*0.2 + rvol_score*0.15) * 100.0
        if LOG_CONF:
            self.logger.info(f"[{self.market}] {symbol}-{interval} conf={confidence:.1f} rvol={rvol:.2f} rsi={df['RSI'].iloc[-1]:.1f} macd={df['MACD'].iloc[-1]:.2f}")
        # Force a test signal regardless of threshold
        if FORCE_SIGNAL:
            window = (
                pd.to_datetime(df["start_time"].min()) if "start_time" in df else None,
                pd.to_datetime(df["start_time"].max()) if "start_time" in df else None,
            )
            direction = "up" if df["close"].iloc[-1] >= df["close"].iloc[-2] else "down"
            self.emit_signal(
                signal_type="test_force",
                symbol=symbol, interval=interval, df=df,
                value=float(confidence), direction=direction, confidence=float(confidence), window=window
            )
            return
        # Daily bias (optional/neutral)
        daily_bias = "neutral"
        if isinstance(daily_df, pd.DataFrame) and len(daily_df) >= self.MA_WINDOW and "close" in daily_df.columns:
            dd = daily_df.copy()
            dd["close"] = pd.to_numeric(dd["close"], errors="coerce")
            dd = dd.dropna()
            if len(dd) >= self.MA_WINDOW:
                ma = dd["close"].rolling(window=self.MA_WINDOW).mean().iloc[-1]
                if pd.notna(ma):
                    daily_bias = "bullish" if dd["close"].iloc[-1] > ma else "bearish"

        if confidence >= self.MIN_CONFIDENCE:
            # Minimal direction: last price vs previous price
            direction = "up" if df["close"].iloc[-1] >= df["close"].iloc[-2] else "down"
            window = (
                pd.to_datetime(df["start_time"].min()) if "start_time" in df else None,
                pd.to_datetime(df["start_time"].max()) if "start_time" in df else None,
            )
            self.emit_signal(
                signal_type="trend_confidence",
                symbol=symbol, interval=interval, df=df,
                value=float(confidence), direction=direction, confidence=float(confidence), window=window
            )

    # -------------------- Signal I/O --------------------
    def emit_signal(self, signal_type, symbol, interval, df, value=None, direction=None, confidence=None, window=None):
        try:
            context = {
                "close": float(df["close"].iloc[-1]),
                "volume": float(df["volume"].iloc[-1]),
                "volume_ma": float(df["Volume_MA"].iloc[-1]),
                "rsi": float(df["RSI"].iloc[-1]),
                "macd": float(df["MACD"].iloc[-1]),
                "macd_signal": float(df["MACD_Signal"].iloc[-1]),
                "upper_band": float(df["UpperBand"].iloc[-1]),
                "lower_band": float(df["LowerBand"].iloc[-1]),
            }
            self.log_signal(symbol, interval, signal_type, value, context, direction, confidence, window)
        except Exception as e:
            self.logger.error(f"❌ emit_signal failed: {e}", exc_info=True)

    def log_signal(self, symbol, interval, signal_type, value=None, context=None, direction=None, confidence=None, window=None):
        window_start, window_end = window if window else (None, None)
        window_start_str = window_start.isoformat() if window_start is not None else None
        window_end_str = window_end.isoformat() if window_end is not None else None

        # DB insert
        cur = self.db_conn.cursor()
        try:
            cur.execute("""
                INSERT INTO trading.signal_log
                (symbol, interval, signal_type, value, context, confidence, direction, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
            """, (
                symbol, interval, signal_type,
                float(value) if value is not None else None,
                json.dumps(context or {}), float(confidence) if confidence is not None else None,
                direction, window_start_str, window_end_str
            ))
            self.db_conn.commit()
            self.logger.info(f"🧠 Logged signal: {signal_type} for {symbol}")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"❌ Failed to insert signal: {e}", exc_info=True)
        finally:
            cur.close()

        # Push to trigger queue
        out = {
            "symbol": symbol, "interval": interval, "signal_type": signal_type,
            "value": float(value) if value is not None else None,
            "context": context or {}, "confidence": float(confidence) if confidence is not None else None,
            "direction": direction, "window_start": window_start_str, "window_end": window_end_str
        };
        self.redis_client.rpush(TRIGGER_QUEUE_CHANNEL, json.dumps(out))

    # -------------------- Main --------------------
    def run(self):
        self.logger.info(f"[{self.market}]🚀 Trigger Bot starting...")
        # Initial status
        started_payload = {
            "bot_name": self.bot_name, "status": "started",
            "time": datetime.datetime.utcnow().isoformat(), "auth_token": self.auth_token,
            "metadata": {"version": self.version, "pid": os.getpid(), "strategy": self.strategy, "vitals": {}},
        }
        self.redis_client.publish(SERVICE_STATUS_CHANNEL, json.dumps(started_payload))

        # Initial symbol snapshot + optional preload logs
        self.preload_recent_klines()

        # Threads: symbol polling + redis listener
        threading.Thread(target=self._symbol_poll_loop, daemon=True).start()
        threading.Thread(target=self.listen_redis, daemon=True).start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("🛑 Keyboard interrupt received. Stopping TriggerBot.")
        finally:
            self.running = False
            self.heartbeat.stop()


if __name__ == "__main__":
    if sys.prefix == sys.base_prefix:
        print("❌ Virtual environment is NOT activated. Please activate it first.")
        sys.exit(1)

    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 🚀 Starting TRIGGER_BOT...")

    bot_linear = TriggerBot("linear")
    bot_spot = TriggerBot("spot")

    t1 = threading.Thread(target=bot_linear.run)
    t2 = threading.Thread(target=bot_spot.run)
    t1.start(); t2.start()
    t1.join(); t2.join()