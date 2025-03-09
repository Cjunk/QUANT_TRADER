import psycopg2
import pandas as pd
import datetime
import config.config_db as db_config
import concurrent.futures
import numpy as np
import json
import time  # for timing the grid search

# --- General Configuration ---
INITIAL_BALANCE = 1000      # Starting bank balance (margin) in USD
SYMBOL = "BTCUSDT"
INTERVAL = "5"              # 5-minute candles
KLINE_LIMIT = 200

# --- Leverage Configuration ---
LEVERAGE = 5                # Leverage factor for positions

# --- Stop Loss Configuration ---
STOP_LOSS_MIN = 0.03        # Minimum stop loss (e.g. 3% drop)
STOP_LOSS_MAX = 1.0         # Maximum stop loss (100%)
STOP_LOSS_STEP = 0.1

# --- RSI Parameters ---
RSI_PERIOD = 14
RSI_BUY_MIN = 20            # Minimum RSI threshold for BUY signal
RSI_BUY_MAX = 41            # Exclusive upper bound for BUY threshold (tests from 10 to 40)
RSI_SELL_MIN = 70           # Minimum RSI threshold for SELL signal
RSI_SELL_MAX = 91           # Exclusive upper bound for SELL threshold (tests from 60 to 90)

# --- MACD Parameters ---
MACD_SHORT = 12
MACD_LONG = 26
MACD_SIGNAL = 9

# --- Bollinger Bands Parameters ---
BB_WINDOW = 20
BB_MULTIPLIER = 2

# --- Load Historical Data ---
def load_historical_klines(symbol, interval, limit=KLINE_LIMIT):
    conn = psycopg2.connect(
        host=db_config.DB_HOST,
        port=db_config.DB_PORT,
        database=db_config.DB_DATABASE,
        user=db_config.DB_USER,
        password=db_config.DB_PASSWORD
    )
    query = """
    SELECT start_time, open, close, high, low, volume 
    FROM kline_data 
    WHERE symbol = %s AND interval = %s
    ORDER BY start_time DESC
    LIMIT %s;
    """
    df = pd.read_sql(query, conn, params=(symbol, interval, limit))
    conn.close()
    return df

# --- Technical Indicator Calculations ---
def compute_wilder_rsi_loop(prices, period=RSI_PERIOD):
    if len(prices) < period + 1:
        return [None] * len(prices)
    rsi = [None] * len(prices)
    gains = []
    losses = []
    for i in range(1, period + 1):
        change = prices[i] - prices[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
    rsi[period] = 100 - (100 / (1 + rs))
    for i in range(period + 1, len(prices)):
        change = prices[i] - prices[i - 1]
        gain = max(change, 0)
        loss = abs(min(change, 0))
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
        rsi[i] = 100 - (100 / (1 + rs))
    return rsi

def compute_indicators(df):
    df['RSI'] = compute_wilder_rsi_loop(df['close'], period=RSI_PERIOD)
    ema_short = df['close'].ewm(span=MACD_SHORT, adjust=False).mean()
    ema_long = df['close'].ewm(span=MACD_LONG, adjust=False).mean()
    df['MACD'] = ema_short - ema_long
    df['MACD_Signal'] = df['MACD'].ewm(span=MACD_SIGNAL, adjust=False).mean()
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
    df['MA'] = df['close'].rolling(window=BB_WINDOW).mean()
    df['STD'] = df['close'].rolling(window=BB_WINDOW).std()
    df['UpperBand'] = df['MA'] + BB_MULTIPLIER * df['STD']
    df['LowerBand'] = df['MA'] - BB_MULTIPLIER * df['STD']
    return df

# --- Backtesting Function for Leveraged Positions with Stop Loss ---
def backtest(df, initial_balance, RSI_BUY, RSI_SELL, leverage, stop_loss):
    balance = initial_balance
    position = 0.0
    entry_price = None
    trade_log = []
    prev_hist = None
    for i in range(1, len(df)):
        row = df.iloc[i]
        time_stamp = row['start_time']
        close_price = row['close']
        rsi = row['RSI']
        macd_hist = row['MACD_Hist']
        lower_band = row['LowerBand']
        upper_band = row['UpperBand']
        macd_cross_up = False
        macd_cross_down = False
        if prev_hist is not None:
            if prev_hist < 0 and macd_hist > 0:
                macd_cross_up = True
            if prev_hist > 0 and macd_hist < 0:
                macd_cross_down = True
        # Modified Bollinger condition:
        # Allow entry if the close price is within 2% above the lower band for buys,
        # or within 2% below the upper band for sells.
        boll_buy = close_price <= lower_band * 1.10
        boll_sell = close_price >= upper_band * 0.90
        boll_buy = True
        if position > 0 and entry_price is not None:
            if close_price < entry_price * (1 - stop_loss):
                profit = position * (close_price - entry_price)
                balance += profit
                trade_log.append((time_stamp, "STOP_LOSS", close_price, position, profit))
                position = 0.0
                entry_price = None
                prev_hist = macd_hist
                continue
        if position == 0 and rsi is not None and rsi < RSI_BUY and boll_buy:
            position = (balance * leverage) / close_price
            entry_price = close_price
            trade_log.append((time_stamp, "BUY", close_price, position))
        elif position > 0 and rsi is not None and rsi > RSI_SELL and boll_sell:
            profit = position * (close_price - entry_price)
            balance += profit
            trade_log.append((time_stamp, "SELL", close_price, position, profit))
            position = 0.0
            entry_price = None
        prev_hist = macd_hist
    if position > 0 and entry_price is not None:
        final_price = df.iloc[-1]['close']
        profit = position * (final_price - entry_price)
        balance += profit
        trade_log.append((df.iloc[-1]['start_time'], "SELL_END", final_price, position, profit))
        position = 0.0
        entry_price = None
    total_profit = balance - initial_balance
    return balance, total_profit, trade_log

# --- Grid Search Backtest ---
def run_backtest_for_params(params):
    rsi_buy, rsi_sell, stop_loss = params
    final_balance, profit, trades = backtest(global_df, INITIAL_BALANCE, rsi_buy, rsi_sell, LEVERAGE, stop_loss)
    return (rsi_buy, rsi_sell, stop_loss, profit, final_balance, trades)

# --- Initializer for Worker Processes ---
def init_worker(df):
    global global_df
    global_df = df

# --- Detailed Trade Report Function ---
def generate_trade_report(trade_log):
    executed_trades = [trade for trade in trade_log if trade[1] in ("SELL", "SELL_END", "STOP_LOSS")]
    total_trades = len(executed_trades)
    winning_trades = [trade for trade in executed_trades if trade[-1] > 0]
    losing_trades = [trade for trade in executed_trades if trade[-1] <= 0]
    num_wins = len(winning_trades)
    num_losses = len(losing_trades)
    win_loss_ratio = num_wins / num_losses if num_losses != 0 else float('inf')
    total_profit = sum(trade[-1] for trade in executed_trades)
    avg_profit = total_profit / total_trades if total_trades > 0 else 0
    max_loss = min((trade[-1] for trade in executed_trades if trade[-1] < 0), default=0)
    sum_wins = sum(trade[-1] for trade in winning_trades)
    sum_losses = abs(sum(trade[-1] for trade in losing_trades))
    profit_factor = sum_wins / sum_losses if sum_losses != 0 else float('inf')
    risk_adjusted_profit = total_profit / abs(max_loss) if max_loss != 0 else float('inf')
    if executed_trades:
        trade_times = [trade[0] for trade in executed_trades]
        min_time = min(trade_times)
        max_time = max(trade_times)
        trade_duration = max_time - min_time
    else:
        trade_duration = None
    report = {
        "Total Trades": total_trades,
        "Winning Trades": num_wins,
        "Losing Trades": num_losses,
        "Win/Loss Ratio": win_loss_ratio,
        "Total Profit": total_profit,
        "Average Profit per Trade": avg_profit,
        "Maximum Loss on Single Trade": max_loss,
        "Profit Factor": profit_factor,
        "Risk-Adjusted Profit (Total Profit / |Max Loss|)": risk_adjusted_profit,
        "Trade Duration": trade_duration
    }
    return report

if __name__ == "__main__":
    df = load_historical_klines(SYMBOL, INTERVAL)
    if df.empty:
        print("No data loaded from database!")
        exit(1)
    if not pd.api.types.is_datetime64_any_dtype(df['start_time']):
        df['start_time'] = pd.to_datetime(df['start_time'])
    df = df.sort_values("start_time").reset_index(drop=True)
    df = compute_indicators(df)
    
    grid = [
        (buy, sell, sl)
        for buy in range(RSI_BUY_MIN, RSI_BUY_MAX)
        for sell in range(RSI_SELL_MIN, RSI_SELL_MAX)
        if buy < sell
        for sl in np.arange(STOP_LOSS_MIN, STOP_LOSS_MAX + STOP_LOSS_STEP, STOP_LOSS_STEP)
    ]
    print(f"Testing {len(grid)} parameter combinations...")
    
    start_time_perf = time.perf_counter()
    best_risk_adjusted_profit = -float('inf')
    best_params = None
    best_result = None
    with concurrent.futures.ProcessPoolExecutor(initializer=init_worker, initargs=(df,)) as executor:
        for res in executor.map(run_backtest_for_params, grid, chunksize=100):
            rsi_buy, rsi_sell, stop_loss, profit, final_balance, trades = res
            report = generate_trade_report(trades)
            risk_adj = report.get("Risk-Adjusted Profit (Total Profit / |Max Loss|)", -float('inf'))
            if risk_adj > best_risk_adjusted_profit:
                best_risk_adjusted_profit = risk_adj
                best_params = (rsi_buy, rsi_sell, stop_loss)
                best_result = res
    end_time_perf = time.perf_counter()
    elapsed_time = end_time_perf - start_time_perf
    
    return_pct = (best_result[3] / INITIAL_BALANCE) * 100
    print("\n=== Best Strategy Found (Based on Risk-Adjusted Profit) ===")
    print(f"RSI BUY threshold: {best_params[0]}")
    print(f"RSI SELL threshold: {best_params[1]}")
    print(f"Stop Loss: {best_params[2]:.2f}")
    print(f"Profit: ${best_result[3]:.2f} ({return_pct:.2f}%)")
    print(f"Grid Search Time: {elapsed_time:.2f} seconds")
    
    print("\n=== Trade Log for Best Strategy ===")
    trade_log = best_result[5]
    for trade in trade_log:
        if len(trade) == 5:
            time_stamp, action, price, quantity, pnl = trade
            print(f"{time_stamp}: {action} at ${price:.2f}, Quantity: {quantity:.4f}, PnL: ${pnl:.2f}")
        else:
            time_stamp, action, price, quantity = trade
            print(f"{time_stamp}: {action} at ${price:.2f}, Quantity: {quantity:.4f}")
    
    report = generate_trade_report(trade_log)
    print("\n=== Detailed Trade Report ===")
    for key, value in report.items():
        if isinstance(value, float):
            print(f"{key}: {value:.2f}")
        elif isinstance(value, datetime.timedelta):
            print(f"{key}: {value}")
        else:
            print(f"{key}: {value}")
    
    print("\nNOTE: This strategy risks the entire margin on each trade. In practice, it's typically more prudent to risk only a fraction of your capital.")
