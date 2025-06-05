import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta
import statistics

load_dotenv()

def connect():
    try:
        host = os.getenv("DB_HOST", "127.0.0.1")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        dbname = os.getenv("DB_DATABASE")
        port = int(os.getenv("DB_PORT", 5433))

        if not all([user, password, dbname]):
            raise ValueError("Missing DB_USER, DB_PASSWORD, or DB_DATABASE in environment variables.")

        return psycopg2.connect(
            host=host,
            user=user,
            password=password,
            dbname=dbname,
            port=port
        )
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        raise

def analyze_trade_orderbook_sync(conn, symbol):
    cur = conn.cursor()

    cur.execute("""
        SELECT trade_time, price, volume, side, is_buyer_maker
        FROM trading.raw_trade_data
        WHERE symbol = %s
        ORDER BY trade_time DESC
        LIMIT 4000
    """, (symbol,))
    trades = cur.fetchall()[::-1]  # oldest to newest

    matched = 0
    buy_count = 0
    sell_count = 0
    buy_volume = 0
    sell_volume = 0
    total_spread = 0
    price_volume_sum = 0
    total_volume = 0
    trade_prices = []
    trade_sizes = []
    insight = []
    clusters = []

    cluster_buffer = []
    last_time = None
    cluster_threshold = timedelta(milliseconds=200)

    for trade_time, price, volume, side, is_buyer_maker in trades:
        # Cluster detection
        if last_time is None or trade_time - last_time <= cluster_threshold:
            cluster_buffer.append((trade_time, price, volume, "sell" if is_buyer_maker else "buy"))
        else:
            if len(cluster_buffer) >= 3:
                direction = cluster_buffer[0][3]
                avg_price = sum([x[1] for x in cluster_buffer]) / len(cluster_buffer)
                clusters.append(f"🔥 {len(cluster_buffer)} {direction}s in {cluster_buffer[-1][0] - cluster_buffer[0][0]} near {avg_price:.2f}")
            cluster_buffer = [(trade_time, price, volume, "sell" if is_buyer_maker else "buy")]
        last_time = trade_time

        time_window_start = trade_time - timedelta(milliseconds=500)
        time_window_end = trade_time + timedelta(milliseconds=500)

        cur.execute("""
            SELECT price, volume, side
            FROM trading.orderbook_data
            WHERE symbol = %s AND created_at BETWEEN %s AND %s
        """, (symbol, time_window_start, time_window_end))

        book_rows = cur.fetchall()
        bids = [r for r in book_rows if r[2].lower() == "bid"]
        asks = [r for r in book_rows if r[2].lower() == "ask"]

        if not bids or not asks:
            continue

        highest_bid = max([b[0] for b in bids])
        lowest_ask = min([a[0] for a in asks])
        spread = lowest_ask - highest_bid
        total_spread += spread

        matched += 1
        price_volume_sum += price * volume
        total_volume += volume
        trade_prices.append(price)
        trade_sizes.append(volume)

        if is_buyer_maker:
            sell_count += 1
            sell_volume += volume
            trade_type = "sell"
        else:
            buy_count += 1
            buy_volume += volume
            trade_type = "buy"

        if price < highest_bid or price > lowest_ask:
            insight.append(f"⚠️ {trade_time}: {trade_type} trade at {price} outside bid/ask [{highest_bid} / {lowest_ask}]")

    if len(cluster_buffer) >= 3:
        direction = cluster_buffer[0][3]
        avg_price = sum([x[1] for x in cluster_buffer]) / len(cluster_buffer)
        clusters.append(f"🔥 {len(cluster_buffer)} {direction}s in {cluster_buffer[-1][0] - cluster_buffer[0][0]} near {avg_price:.2f}")

    cur.close()

    match_rate = matched / len(trades) * 100 if trades else 0
    avg_spread = total_spread / matched if matched else 0
    vwap = price_volume_sum / total_volume if total_volume else 0
    trade_imbalance = abs(buy_volume - sell_volume) / (buy_volume + sell_volume) if (buy_volume + sell_volume) > 0 else 0
    avg_trade_size = sum(trade_sizes) / len(trade_sizes) if trade_sizes else 0
    volatility = statistics.stdev(trade_prices) if len(trade_prices) > 1 else 0

    print("\n🔍 Trade-OrderBook Synchronization Report")
    print(f"Symbol: {symbol}")
    print(f"Total Trades Analyzed: {len(trades)}")
    print(f"Matched to Book: {matched} ({match_rate:.2f}%)")
    print(f"Buy Trades: {buy_count}, Sell Trades: {sell_count}")
    print(f"Buy Volume: {buy_volume:.2f}, Sell Volume: {sell_volume:.2f}")
    print(f"Average Spread During Trades: {avg_spread:.8f}")
    print(f"VWAP: {vwap:.2f}")
    print(f"Trade Imbalance: {trade_imbalance:.2%}")
    print(f"Average Trade Size: {avg_trade_size:.4f}")
    print(f"Price Volatility: {volatility:.6f}")

    print("\nInsights:")
    for i in insight[:10]:
        print(i)
    if len(insight) > 10:
        print(f"... and {len(insight) - 10} more unusual trades detected.")

    print("\nTrade Clusters:")
    for cluster in clusters:
        print(cluster)

def get_interval_delta(interval):
    if interval == "1":
        return timedelta(minutes=1)
    elif interval == "5":
        return timedelta(minutes=5)
    elif interval == "60":
        return timedelta(hours=1)
    elif interval.upper() == "D":
        return timedelta(days=1)
    else:
        raise ValueError(f"Unknown interval: {interval}")

def check_kline_gaps(conn, symbol, interval):
    cur = conn.cursor()
    cur.execute("""
        SELECT start_time FROM trading.kline_data
        WHERE symbol = %s AND interval = %s
        ORDER BY start_time ASC
    """, (symbol, interval))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return f"❌ trading.kline_data: No data for {symbol} {interval}."

    delta = get_interval_delta(interval)
    gaps = 0
    for i in range(1, len(rows)):
        expected = rows[i - 1][0] + delta
        if rows[i][0] != expected:
            gaps += 1
    return f"✅ {symbol} {interval}: {len(rows)} entries, {gaps} gap(s) found."

def check_orderbook_summary(conn, symbol):
    cur = conn.cursor()
    cur.execute("""
        SELECT price, volume, side FROM trading.orderbook_data
        WHERE symbol = %s
    """, (symbol,))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        return f"❌ trading.orderbook_data: No data for {symbol}."

    bids = [r for r in rows if r[2].lower() == "bid"]
    asks = [r for r in rows if r[2].lower() == "ask"]

    highest_bid = max([b[0] for b in bids], default=0)
    lowest_ask = min([a[0] for a in asks], default=0)
    spread = lowest_ask - highest_bid if lowest_ask > highest_bid else -1

    return f"✅ {symbol}: {len(bids)} bids, {len(asks)} asks, Spread: {spread:.8f}"

def check_trade_data(conn, symbol):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*), MIN(trade_time), MAX(trade_time)
        FROM trading.raw_trade_data
        WHERE symbol = %s
    """, (symbol,))
    row = cur.fetchone()
    cur.close()

    count, start_time, end_time = row
    if count == 0:
        return f"❌ trading.raw_trade_data: No trades for {symbol}."

    duration = end_time - start_time
    return f"✅ {symbol}: {count} trades from {start_time} to {end_time} ({duration})"

def run_report():
    conn = connect()
    symbols = ["BTCUSDT"]
    intervals = ["1", "5", "60", "D"]

    print("\n📊 Kline Table Checks:")
    for symbol in symbols:
        for interval in intervals:
            print(check_kline_gaps(conn, symbol, interval))

    print("\n📈 Order Book Checks:")
    for symbol in symbols:
        print(check_orderbook_summary(conn, symbol))

    print("\n💱 Raw Trade Data Checks:")
    for symbol in symbols:
        print(check_trade_data(conn, symbol))

    conn.close()

if __name__ == "__main__":
    symbols = ["BTCUSDT"]
    run_report()
    conn = connect()
    print("\n📈 Trade and Order Book Synchronization Checks:")
    for symbol in symbols:
        analyze_trade_orderbook_sync(conn, symbol)



