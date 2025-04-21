import psycopg2
import pandas as pd
import datetime
import json
import config.config_db as db_config
import matplotlib.pyplot as plt
# === STRATEGY DEFINITIONS ===
STRATEGIES = {
    "rsi_macd_bbands_volume": lambda row, params, short=False: (
        (row['rsi'] < params['rsi_buy'] and
         row['macd_hist'] > params['macd_hist_threshold'] and
         row['close'] <= row['lower_band'] and
         row['volume'] > row['volume_ma'] * params['volume_multiplier'])
        if not short else
        (row['rsi'] > params['rsi_sell'] and
         row['macd_hist'] < -params['macd_hist_threshold'] and
         row['close'] >= row['upper_band'] and
         row['volume'] > row['volume_ma'] * params['volume_multiplier'])
    ),
    "breakout_upper_band": lambda row, params, short=False: (
        (row['close'] > row['upper_band'] and
         row['volume'] > row['volume_ma'] * params['volume_multiplier'])
        if not short else
        (row['close'] < row['lower_band'] and
         row['volume'] > row['volume_ma'] * params['volume_multiplier'])
    ),
    "lose_everything": lambda row, params, short=False: (
        (row['rsi'] > 70 and row['macd_hist'] > 0 and row['close'] > row['upper_band'])
        if not short else
        (row['rsi'] < 30 and row['macd_hist'] < 0 and row['close'] < row['lower_band'])
    ),
    "volume_breakout": lambda row, params, short=False: (
        (row['close'] > row['upper_band'] and
         row['volume'] > row['volume_ma'] * params.get("volume_multiplier", 1.5))
        if not short else
        (row['close'] < row['lower_band'] and
         row['volume'] > row['volume_ma'] * params.get("volume_multiplier", 1.5))
    ),
    "macd_cross_rsi": lambda row, params, short=False: (
        (row['macd_hist'] > 0 and row['rsi'] > params.get("rsi_trigger", 50))
        if not short else
        (row['macd_hist'] < 0 and row['rsi'] < params.get("rsi_trigger", 50))
    ),
    "bbands_cross": lambda row, params, short=False: (
        (row['close'] < row['lower_band']) if not short else (row['close'] > row['upper_band'])
    ),
    "rsi_cross": lambda row, params, short=False: (
        (row['rsi'] > params.get("rsi_trigger", 50) and row['rsi'] < params.get("rsi_exit", 70))
        if not short else
        (row['rsi'] < params.get("rsi_trigger", 50) and row['rsi'] > params.get("rsi_exit", 30))
    ),
    "macd_cross": lambda row, params, short=False: (
        (row['macd'] > row['macd_signal'] and row['macd_hist'] > 0)
        if not short else
        (row['macd'] < row['macd_signal'] and row['macd_hist'] < 0)
    ),
    "moving_average_cross": lambda row, params, short=False: (
        (row['close'] > row['ma']) if not short else (row['close'] < row['ma'])
    ),
}
# === CONFIGURATION ===

CONFIGS = [
    {
        "name": f"{strategy.upper()}_L{lev}_SL{int(sl * 100)}",
        "symbol": "BTCUSDT",
        "interval": "1",
        "klines": 2000,
        "initial_balance": 1000,
        "leverage": lev,
        "stop_loss": sl,
        "trailing_stop": 0.2,
        "strategy": strategy,
        "strategy_params": {
            "rsi_buy": 30,
            "rsi_sell": 70,
            "macd_hist_threshold": 0.0,
            "volume_multiplier": 1.5,
            "rsi_trigger": 50,
            "rsi_exit": 70
        }
    }
    for strategy in STRATEGIES.keys()
    for lev in [2, 3]
    for sl in [0.02, 0.03]
]

# === LOAD HISTORICAL DATA ===
def load_data(symbol, interval, limit):
    try:
        conn = psycopg2.connect(
            host=db_config.DB_HOST,
            port=db_config.DB_PORT,
            database=db_config.DB_DATABASE,
            user=db_config.DB_USER,
            password=db_config.DB_PASSWORD
        )
        query = f"""
        SELECT start_time, open, close, high, low, volume,
               rsi, macd, macd_signal, macd_hist, ma,
               upper_band, lower_band
        FROM {db_config.DB_TRADING_SCHEMA}.kline_data
        WHERE symbol = %s AND interval = %s
        ORDER BY start_time ASC
        LIMIT %s;
        """
        df = pd.read_sql(query, conn, params=(symbol, interval, limit))
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['volume_ma'] = df['volume'].rolling(10).mean()
        return df.dropna()
    except Exception as e:
        print(f"DB Load Error: {e}")
        return pd.DataFrame()

# === BACKTEST CORE ===
def run_backtest(df, cfg):
    strategy_fn = STRATEGIES[cfg['strategy']]
    params = cfg['strategy_params']
    balance = cfg['initial_balance']
    position = 0.0
    entry_price = None
    trade_log = []

    for _, row in df.iterrows():
        price = row['close']
        time = row['start_time']

        if position > 0 and entry_price:
            stop_price = entry_price * (1 - cfg['stop_loss'])
            if price < stop_price:
                pnl = (price - entry_price) * position
                balance += pnl
                trade_log.append((time, "SELL", price, position, pnl))
                position = 0
                entry_price = None
                continue

        if position < 0 and entry_price:
            stop_price = entry_price * (1 + cfg['stop_loss'])
            if price > stop_price:
                pnl = (entry_price - price) * abs(position)
                balance += pnl
                trade_log.append((time, "COVER", price, position, pnl))
                position = 0
                entry_price = None
                continue

        if position == 0 and strategy_fn(row, params):
            position = (balance * cfg['leverage']) / price
            entry_price = price
            trade_log.append((time, "BUY", price, position))

        elif position == 0 and strategy_fn(row, params, short=True):
            position = -(balance * cfg['leverage']) / price
            entry_price = price
            trade_log.append((time, "SHORT", price, position))

    if position != 0:
        final_price = df.iloc[-1]['close']
        pnl = (final_price - entry_price) * position if position > 0 else (entry_price - final_price) * abs(position)
        balance += pnl
        trade_log.append((df.iloc[-1]['start_time'], "FINAL_EXIT", final_price, position, pnl))

    return balance, trade_log

# === REPORTING ===
def generate_report(cfg_name, initial_balance, final_balance, trades):
    closed_trades = [t for t in trades if len(t) == 5]
    profits = [t[-1] for t in closed_trades]
    wins = [p for p in profits if p > 0]
    losses = [p for p in profits if p <= 0]
    print(f"\n📊 STRATEGY: {cfg_name}")
    print("=======================")
    print(f"Initial Balance: ${initial_balance:.2f}")
    print(f"Final Balance:   ${final_balance:.2f}")
    print(f"Profit/Loss:     ${final_balance - initial_balance:.2f}")
    print(f"Total Trades:    {len(closed_trades)}")
    print(f"Wins:            {len(wins)} | Losses: {len(losses)}")
    print(f"Profit Factor:   {sum(wins)/abs(sum(losses)) if losses else float('inf'):.2f}")

# === FINAL PLOT FOR ALL STRATEGIES ===
def summary_bar_chart(results):
    names = [res['name'] for res in results]
    profits = [res['final'] - res['initial'] for res in results]
    plt.figure(figsize=(10, 5))
    plt.bar(names, profits, color='skyblue')
    plt.title("Profit Comparison Across Strategies")
    plt.ylabel("Total PnL")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()
def plot_trades(df, trades):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['start_time'], df['close'], label='Close Price')

    for trade in trades:
        time, action, price, *_ = trade
        color = 'green' if action in ['BUY', 'COVER'] else 'red'
        marker = '^' if action in ['BUY', 'SHORT'] else 'v'
        ax.plot(time, price, marker=marker, color=color, label=action)

    ax.set_title('Trade Entries and Exits')
    ax.set_xlabel('Time')
    ax.set_ylabel('Price')
    ax.legend()
    ax.grid(True)
    plt.show()

# === MAIN ===
if __name__ == "__main__":
    all_results = []
    for config in CONFIGS:
        df = load_data(config['symbol'], config['interval'], config['klines'])
        if df.empty:
            print("No data loaded.")
            continue
        final_balance, trades = run_backtest(df, config)
        generate_report(config['name'], config['initial_balance'], final_balance, trades)
        plot_trades(df, trades)

        all_results.append({"name": config['name'], "initial": config['initial_balance'], "final": final_balance})

    summary_bar_chart(all_results)

