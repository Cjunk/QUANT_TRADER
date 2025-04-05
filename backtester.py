import psycopg2
import pandas as pd
import datetime
import config.config_db as db_config
import numpy as np
import time
import optuna
from sqlalchemy import create_engine

# --- General Configuration ---
INITIAL_BALANCE = 2000      # Starting bank balance (margin) in USD
SYMBOL = "BTCUSDT"
INTERVAL = "5"              # 1-minute candles (or modify as needed)
KLINE_LIMIT = 12 * 12

# --- Leverage Configuration ---
LEVERAGE = 5               # Leverage factor for positions

# --- Stop Loss Configuration ---
STOP_LOSS_MIN = 0.02       # Minimum stop loss (e.g. 3% drop)
STOP_LOSS_MAX = 5.0         # Maximum stop loss (100%)
STOP_LOSS_STEP = 0.2

# --- RSI Parameters ---
RSI_BUY_MIN = 30            # Minimum RSI threshold for BUY signal
RSI_BUY_MAX = 70            # Exclusive upper bound for BUY threshold
RSI_SELL_MIN = 65           # Minimum RSI threshold for SELL signal
RSI_SELL_MAX = 80           # Exclusive upper bound for SELL threshold

# --- MACD Parameters ---
MACD_SHORT = 12
MACD_LONG = 26
MACD_SIGNAL = 8

# --- Bollinger Bands Parameters ---
BB_WINDOW = 20
BB_MULTIPLIER = 2

# --- Load Historical Data ---
def load_historical_klines(symbol, interval, limit=KLINE_LIMIT):
    try:
        # Establish the connection
        conn = psycopg2.connect(
            host=db_config.DB_HOST,
            port=db_config.DB_PORT,
            database=db_config.DB_DATABASE,
            user=db_config.DB_USER,
            password=db_config.DB_PASSWORD
        )
        print(f"Connected to database at {db_config.DB_HOST}:{db_config.DB_PORT}")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if connection fails
    
    query = f"""
    SELECT start_time, open, close, high, low, volume, 
           rsi, macd, macd_signal, macd_hist, ma, upper_band, lower_band
    FROM {db_config.DB_TRADING_SCHEMA}.kline_data
    WHERE symbol = %s AND interval = %s
    ORDER BY start_time DESC
    LIMIT %s;
    """
    
    try:
        # Attempt to execute the query and fetch the data
        print(f"Running query to load historical data: {query}")
        df = pd.read_sql(query, conn, params=(symbol, interval, limit))
        print(f"Fetched {len(df)} rows of data.")
    except Exception as e:
        print(f"Error executing query: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame if the query fails
    
    conn.close()  # Always close the connection
    return df

# --- Backtesting Function for Leveraged Positions with Stop Loss ---

def backtest(df, initial_balance, RSI_BUY, RSI_SELL, leverage, stop_loss, trailing_stop=0.02):
    balance = initial_balance
    position = 0.0
    entry_price = None
    entry_time = None
    stop_loss_price = None
    trade_log = []
    fake_profit = 0  # Fake profit calculation to simulate trading performance
    
    for i in range(1, len(df)):
        row = df.iloc[i]
        time_stamp = row['start_time']
        close_price = row['close']
        rsi = row['rsi']
        macd_hist = row['macd_hist']
        lower_band = row['lower_band']
        upper_band = row['upper_band']

        # Modified Bollinger condition:
        boll_buy = close_price <= lower_band * 1.10
        boll_sell = close_price >= upper_band * 0.90

        # Stop loss for long positions
        if position > 0 and entry_price is not None:
            # Trailing stop loss for long positions
            if close_price > entry_price * (1 + trailing_stop):
                stop_loss_price = entry_price * (1 + trailing_stop)  # Update trailing stop loss price
            # Check for stop loss condition
            if close_price < stop_loss_price:
                profit = position * (close_price - entry_price)
                balance += profit
                fake_profit += profit  # Track fake profit
                trade_log.append((time_stamp, "TRAILING_STOP", close_price, position, profit))
                position = 0.0
                entry_price = None
                stop_loss_price = None  # Reset stop loss price after exit
                continue
        
        # Stop loss for short positions
        elif position < 0 and entry_price is not None:
            # Trailing stop loss for short positions
            if close_price < entry_price * (1 - trailing_stop):
                stop_loss_price = entry_price * (1 - trailing_stop)  # Update trailing stop loss price
            # Check for stop loss condition
            if close_price > stop_loss_price:
                profit = position * (entry_price - close_price)
                balance += profit
                fake_profit += profit  # Track fake profit
                trade_log.append((time_stamp, "TRAILING_STOP_SHORT", close_price, position, profit))
                position = 0.0
                entry_price = None
                stop_loss_price = None  # Reset stop loss price after exit
                continue

        # Long buy condition based on RSI and Bollinger Bands
        if position == 0 and rsi is not None and rsi < RSI_BUY and boll_buy:
            position = (balance * leverage) / close_price
            entry_price = close_price
            stop_loss_price = close_price * (1 - stop_loss)  # Set initial stop loss price
            trade_log.append((time_stamp, "BUY", close_price, position))

        # Short sell condition based on RSI and Bollinger Bands
        elif position == 0 and rsi is not None and rsi > RSI_SELL and boll_sell:
            position = -(balance * leverage) / close_price  # Shorting the asset
            entry_price = close_price
            stop_loss_price = close_price * (1 + stop_loss)  # Set initial stop loss price for short
            trade_log.append((time_stamp, "SELL_SHORT", close_price, position))

        # Long sell condition based on RSI and Bollinger Bands
        elif position > 0 and rsi is not None and rsi > RSI_SELL and boll_sell:
            profit = position * (close_price - entry_price)
            balance += profit
            fake_profit += profit  # Track fake profit
            trade_log.append((time_stamp, "SELL", close_price, position, profit))
            position = 0.0
            entry_price = None
            stop_loss_price = None  # Reset stop loss price after exit

        # Short buy condition based on RSI and Bollinger Bands
        elif position < 0 and rsi is not None and rsi < RSI_BUY and boll_buy:
            profit = abs(position) * (entry_price - close_price)  # Profit for closing short position
            balance += profit
            fake_profit += profit  # Track fake profit
            trade_log.append((time_stamp, "BUY_TO_CLOSE_SHORT", close_price, position, profit))
            position = 0.0
            entry_price = None
            stop_loss_price = None  # Reset stop loss price after exit

    # Final sell if position is still open at the end of the period
    if position > 0 and entry_price is not None:
        final_price = df.iloc[-1]['close']
        profit = position * (final_price - entry_price)
        balance += profit
        fake_profit += profit  # Track fake profit
        trade_log.append((df.iloc[-1]['start_time'], "SELL_END", final_price, position, profit))
        position = 0.0
        entry_price = None
    elif position < 0 and entry_price is not None:
        final_price = df.iloc[-1]['close']
        profit = position * (entry_price - final_price)  # Closing short position
        balance += profit
        fake_profit += profit  # Track fake profit
        trade_log.append((df.iloc[-1]['start_time'], "BUY_TO_CLOSE_SHORT_END", final_price, position, profit))
        position = 0.0
        entry_price = None

    total_profit = balance - initial_balance
    return balance, total_profit, fake_profit, trade_log



# --- Generate Trade Report ---
def generate_trade_report(trade_log):
    executed_trades = [
        trade for trade in trade_log
        if trade[1] in (
            "SELL", "SELL_END", "TRAILING_STOP",
            "TRAILING_STOP_SHORT", "BUY_TO_CLOSE_SHORT",
            "BUY_TO_CLOSE_SHORT_END", "STOP_LOSS"
        )
    ]

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

# --- Optimization Objective (Optuna) ---
def objective(trial, df):
    # Define the hyperparameters for the optimization process
    rsi_buy = trial.suggest_int('RSI_BUY', RSI_BUY_MIN, RSI_BUY_MAX)
    rsi_sell = trial.suggest_int('RSI_SELL', RSI_SELL_MIN, RSI_SELL_MAX)
    stop_loss = trial.suggest_uniform('STOP_LOSS', STOP_LOSS_MIN, STOP_LOSS_MAX)
    leverage = trial.suggest_categorical('LEVERAGE', [1, 2, 3])

    # Run the backtest with the current parameters
    final_balance, profit, fake_profit, _ = backtest(df, INITIAL_BALANCE, rsi_buy, rsi_sell, leverage, stop_loss)
    
    # Return the negative profit because Optuna minimizes the objective function
    return -fake_profit

# --- Optuna Study (Optimization) ---
def optimize(df):
    study = optuna.create_study(direction='minimize')
    study.optimize(lambda trial: objective(trial, df), n_trials=100)  # Set trials

    # Return best parameters
    return study.best_params

if __name__ == "__main__":
    df = load_historical_klines(SYMBOL, INTERVAL)
    if df.empty:
        print("No data loaded from database!")
        exit(1)
    
    if not pd.api.types.is_datetime64_any_dtype(df['start_time']):
        df['start_time'] = pd.to_datetime(df['start_time'])
    df = df.sort_values("start_time").reset_index(drop=True)

    # Perform optimization to find the best strategy
    best_params = optimize(df)  # Hyperparameter optimization using Optuna

    print(f"\n=== Best Strategy Found ===")
    print(f"RSI BUY threshold: {best_params['RSI_BUY']}")
    print(f"RSI SELL threshold: {best_params['RSI_SELL']}")
    print(f"Stop Loss: {best_params['STOP_LOSS']:.2f}")

    # After optimization, run a final backtest with best params
    final_balance, total_profit, fake_profit, trade_log = backtest(df, INITIAL_BALANCE, best_params['RSI_BUY'], best_params['RSI_SELL'], best_params['LEVERAGE'], best_params['STOP_LOSS'])

    print(f"\nFinal Backtest Results:")
    print(f"Final Balance: ${final_balance:.2f}")
    print(f"Total Profit: ${total_profit:.2f}")
    print(f"Fake Profit: ${fake_profit:.2f}")

    print("\n=== Trade Log for Best Strategy ===")
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

