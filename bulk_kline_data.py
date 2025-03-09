import requests
import datetime
import psycopg2
import psycopg2.extras
import json
import config.config_db as db_config
"""
  Bulk Kline Data Downloader.
  This will download from Bybit , via the API, historical kline data and store in the kline_data table in Postgresql database
  Make changes to the INTERVAL to capture required candles. Also change the start time to capture enough candles per interval. 

  Written by Jericho Sharman 2025
"""
# --- Configuration ---
SYMBOL = "BTCUSDT"
INTERVAL = "240"  # 5-minute candle
API_URL = "https://api.bybit.com/v5/market/kline"  # Adjust as needed
LIMIT = 200  # Maximum per API call

# --- Connect to PostgreSQL ---
conn = psycopg2.connect(
    host=db_config.DB_HOST,
    port=db_config.DB_PORT,
    database=db_config.DB_DATABASE,
    user=db_config.DB_USER,
    password=db_config.DB_PASSWORD
)
cursor = conn.cursor()

# --- Function to fetch historical klines ---
def fetch_historical_klines(symbol, interval, start_time, end_time, limit=LIMIT):
    """
    Fetch historical kline data between start_time and end_time.
    Timestamps in seconds. You may need to adjust based on API docs.
    """
    params = {
        "category": "spot",
        "symbol": symbol,
        "interval": interval,
        "start": int(start_time)*1000,  # assuming seconds
        "end": int(end_time)*1000,
        "limit": limit
    }
    response = requests.get(API_URL, params=params)
    data = response.json()
    print(data)
    if "result" in data and "list" in data["result"]:
        return data["result"]["list"]
    else:
        print("Error fetching data:", data)
        return []

# --- Function to bulk insert kline data ---
def insert_klines(data_list, symbol, interval):
    """
    data_list: list of candle lists.
    Each candle is assumed to have the format:
    [start, open, close, high, low, volume, turnover, confirm, timestamp]
    """
    # (Optional) print a sample candle for debugging:
    print("Sample candle:", data_list[-1])
    
    insert_sql = """
    INSERT INTO kline_data 
    (symbol, interval, start_time, open, close, high, low, volume, turnover, confirmed)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, interval, start_time) DO NOTHING
    """
    records = []
    print(data_list[0][0])
    for candle in data_list:
        # Convert start from ms to a datetime object:
        start_time = datetime.datetime.utcfromtimestamp(float(candle[0]) / 1000.0)
        record = (
            symbol,
            interval,
            start_time,
            float(candle[1]),  # open
            float(candle[2]),  # close
            float(candle[3]),  # high
            float(candle[4]),  # low
            float(candle[5]),  # volume
            float(candle[6]),  # turnover
            "True"
        )
        records.append(record)
    
    try:
        psycopg2.extras.execute_batch(cursor, insert_sql, records)
        conn.commit()
        print(f"Inserted {len(records)} rows into kline_data.")
    except Exception as e:
        conn.rollback()
        print("Error during bulk insert:", e)

# --- Example Workflow ---
if __name__ == "__main__":
    # Define your time range. For example, the past day:
    end_time = int(datetime.datetime.utcnow().timestamp())  # current epoch seconds
    start_time = end_time - 24*3600*40  # 24 hours ago

    all_data = []
    current_start = start_time
    while current_start < end_time:
        # Fetch one batch; the API might return fewer than LIMIT rows at the end.
        batch = fetch_historical_klines(SYMBOL, INTERVAL, current_start, end_time)
        if not batch:
            break
        all_data.extend(batch)
        # Assume the candles are ordered ascending – use the last candle's start as the new start.
        current_start = int(batch[-1][0])  # Using index 0 to get the start time
        print(f"Fetched {len(batch)} candles, next start_time: {current_start}")
    
    print(f"Total candles fetched: {len(all_data)}")
    
    if all_data:
        insert_klines(all_data, SYMBOL, INTERVAL)
    
    cursor.close()
    conn.close()
