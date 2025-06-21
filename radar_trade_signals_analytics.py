import psycopg2
import pandas as pd
from datetime import timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SignalEvaluator")

# === CONFIG ===
DB_PARAMS = {
    "dbname": "your_db",
    "user": "your_user",
    "password": "xxx",
    "host": "localhost",
    "port": "5432",
}
LOOKAHEADS = [60, 300, 600, 1800]  # seconds: 1m, 5m, 10m, 30m
SIGNAL_INTERVALS = ["60s", "300s", "600s"]

def fetch_signals():
    sql = """
        SELECT id, timestamp, symbol, market, raw->'stats' AS stats
          FROM trading.radar_trade_signals
         ORDER BY timestamp;
    """
    with psycopg2.connect(**DB_PARAMS) as conn:
        return pd.read_sql(sql, conn, parse_dates=["timestamp"])

def fetch_price(symbol, market, moment):
    # Requires you have a price_history table
    sql = """
        SELECT price, ts
          FROM trading.price_history
         WHERE symbol=%s AND market=%s AND ts >= %s
         ORDER BY ts ASC LIMIT 1;
    """
    with psycopg2.connect(**DB_PARAMS) as conn:
        cur = conn.cursor()
        cur.execute(sql, (symbol, market, moment))
        return cur.fetchone()

def evaluate():
    signals = fetch_signals()
    records = []

    for _, sig in signals.iterrows():
        ts = sig.timestamp
        stats = sig.stats
        symbol, market = sig.symbol, sig.market
        p0 = fetch_price(symbol, market, ts)
        if not p0:
            continue

        price0, ts0 = p0

        for sec in LOOKAHEADS:
            future = fetch_price(symbol, market, ts + timedelta(seconds=sec))
            if not future:
                continue
            priceN, tsN = future
            ret = (priceN - price0) / price0

            rec = {
                "id": sig.id, "symbol": symbol, "market": market,
                "signal_ts": ts, "lookahead_s": sec,
                "price0": price0, "ts0": ts0,
                "priceN": priceN, "tsN": tsN,
                "return": ret
            }
            for intr in SIGNAL_INTERVALS:
                s = stats.get(intr)
                if s and s.get("has_enough_data"):
                    rec[f"{intr}_bias"] = s.get("Bias")
            records.append(rec)

    return pd.DataFrame(records)

def summary(df):
    rows = []
    for intr in SIGNAL_INTERVALS:
        sub = df[df[f"{intr}_bias"].notnull()]
        for bias in sub[f"{intr}_bias"].unique():
            ss = sub[sub[f"{intr}_bias"] == bias]
            rows.append({
                "interval": intr,
                "bias": bias,
                "count": len(ss),
                "avg_return": ss["return"].mean(),
                "win_rate": (ss["return"] > 0).mean(),
            })
    return pd.DataFrame(rows)

def main():
    logger.info("🔍 Starting evaluation")
    df = evaluate()
    logger.info(f"Evaluated {len(df)} samples")
    rpt = summary(df)
    print(rpt.sort_values(["interval","count"], ascending=[True, False]))

if __name__ == "__main__":
    main()
