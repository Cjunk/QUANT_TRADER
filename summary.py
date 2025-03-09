import psycopg2
import pandas as pd
import datetime
import config.config_db as db_config

# --- Function to load historical kline data from PostgreSQL ---
def load_historical_klines(symbol, interval,limit=200):
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
    ORDER BY start_time ASC
    LIMIT %s;
    """
    df = pd.read_sql(query, conn, params=(symbol, interval,limit))
    conn.close()
    return df

def compute_wilder_rsi_loop(prices, period=14):
    """
    Compute RSI using Wilder's smoothing method.
    prices: a list or array of closing prices.
    Returns a list of RSI values (with the first period elements as None).
    """
    if len(prices) < period + 1:
        return [None] * len(prices)
    
    rsi = [None] * len(prices)
    gains = []
    losses = []
    
    # Calculate initial averages using simple averages over the first period
    for i in range(1, period + 1):
        change = prices[i] - prices[i - 1]
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
    rsi[period] = 100 - (100 / (1 + rs))
    
    # Now, apply Wilder's smoothing for subsequent periods
    for i in range(period + 1, len(prices)):
        change = prices[i] - prices[i - 1]
        gain = max(change, 0)
        loss = abs(min(change, 0))
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
        rsi[i] = 100 - (100 / (1 + rs))
    return rsi

# --- Function to compute technical indicators ---
def compute_indicators(df):
    # Calculate moving averages
    df['MA7'] = df['close'].rolling(window=7).mean()
    df['MA14'] = df['close'].rolling(window=14).mean()
    df['MA200'] = df['close'].rolling(window=200).mean()
    
    # Calculate RSI (14 period)
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    window = 16
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    #rs = compute_wilder_rsi(df["close"].values,window)
    df['RSI'] = compute_wilder_rsi_loop(df["close"].values,window)
    #df['RSI'] = compute_rsi(df['close'], period=14)   
    # Calculate MACD:
    # 12-period EMA minus 26-period EMA, with a 9-period signal line.
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = ema12 - ema26
    df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['MACD_hist'] = df['MACD'] - df['MACD_signal']
    
    # Volume moving averages (7 and 14 period)
    df['Volume_MA7'] = df['volume'].rolling(window=7).mean()
    df['Volume_MA14'] = df['volume'].rolling(window=14).mean()
    
    return df

# --- Determine a basic trend summary ---
def trend_summary(df):
    # Use the latest candle: if close > MA200, say "Uptrend", else "Downtrend"
    latest_close = df.iloc[-1]['close']
    latest_ma200 = df.iloc[-1]['MA200']
    if pd.isna(latest_ma200):
        return "Insufficient data"
    return "Uptrend" if latest_close > latest_ma200 else "Downtrend"

# --- Print an in-depth summary ---
def print_summary(df):
    last_row = df.iloc[-1]
    print("\n--- Latest Candle Indicators ---")
    print(f"Time:        {last_row['start_time']}")
    print(f"Close:       {last_row['close']:.2f}")
    print(f"MA7:         {last_row['MA7']:.2f}")
    print(f"MA14:        {last_row['MA14']:.2f}")
    print(f"MA200:       {last_row['MA200']:.2f}")
    print(f"RSI:         {last_row['RSI']:.2f}")
    print(f"MACD:        {last_row['MACD']:.4f}")
    print(f"MACD Signal: {last_row['MACD_signal']:.4f}")
    print(f"MACD Hist:   {last_row['MACD_hist']:.4f}")
    print(f"Volume:      {last_row['volume']:.2f}")
    print(f"Volume MA7:  {last_row['Volume_MA7']:.2f}")
    print(f"Volume MA14: {last_row['Volume_MA14']:.2f}")
    print(f"Trend:       {trend_summary(df)}")
    
    print("\n--- Descriptive Statistics for Close Price ---")
    print(df['close'].describe())
    print("\n--- Descriptive Statistics for Volume ---")
    print(df['volume'].describe())

if __name__ == "__main__":
    SYMBOL = "BTCUSDT"
    INTERVAL = "5"  # 5-minute candles
    
    # Load data from PostgreSQL
    df = load_historical_klines(SYMBOL, INTERVAL)
    if df.empty:
        print("No data found!")
    else:
        # Ensure the timestamp column is parsed correctly
        if not pd.api.types.is_datetime64_any_dtype(df['start_time']):
            df['start_time'] = pd.to_datetime(df['start_time'])
        df = df.sort_values("start_time").reset_index(drop=True)
        
        # Compute indicators
        df = compute_indicators(df)
        
        # Print in-depth summary
        print_summary(df)
