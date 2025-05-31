"""
gap_utils.py

Contains gap-fix and Bybit klines logic for the PostgresDBBot.
This keeps db_core.py focused on DB/Redis logic and handlers.
"""
import time
import datetime
import pandas as pd
import requests
import config.config_db as db_config

def fix_data_gaps(db_bot, symbol, interval, lookback=5000):
    """
    Checks for missing klines in the DB and fills them using Bybit API.
    db_bot: instance of PostgresDBBot (provides .conn, .db_engine, .logger)
    symbol: str
    interval: str (e.g. '1', '5', '60', 'D')
    lookback: int (default 5000)
    """
    INTERVAL_MAP = getattr(db_bot, 'INTERVAL_MAP', {"1": 1, "5": 5, "60": 60, "D": 1440})
    cursor = db_bot.conn.cursor()
    try:
        db_bot.logger.info(f"Checking gaps in {symbol}-{interval}")
        interval_minutes = INTERVAL_MAP.get(interval)
        if interval_minutes is None:
            db_bot.logger.error(f"Unsupported interval: {interval}")
            return
        sql = f"""
            SELECT start_time FROM {db_config.DB_TRADING_SCHEMA}.kline_data
            WHERE symbol = %s AND interval = %s
            ORDER BY start_time ASC
        """
        df = pd.read_sql(sql, db_bot.db_engine, params=(symbol, interval))
        if df.empty:
            db_bot.logger.warning(f"No data in DB for {symbol}-{interval}, skipping gap check.")
            return
        df['start_time'] = pd.to_datetime(df['start_time'], utc=True)
        df.set_index('start_time', inplace=True)
        df.index = df.index.floor(f"{interval_minutes}min")
        now_utc = pd.Timestamp.now(tz='UTC').floor(f"{interval_minutes}min") - pd.Timedelta(minutes=interval_minutes)
        expected_times = pd.date_range(start=df.index.min(), end=now_utc, freq=f'{interval_minutes}min')
        existing_times = set(df.index)
        missing_times = [t for t in expected_times if t not in existing_times]
        if not missing_times:
            db_bot.logger.info(f"No gaps for {symbol}-{interval}")
            return
        gap_groups = pd.Series(missing_times).groupby(
            (pd.Series(missing_times).diff() != pd.Timedelta(minutes=interval_minutes)).cumsum()
        )
        for _, gap in gap_groups:
            start_gap = gap.min()
            end_gap = gap.max()
            db_bot.logger.info(f"Gap detected: {start_gap} to {end_gap} ({symbol}-{interval})")
            klines = fetch_bybit_klines(symbol, interval, interval_minutes, start_gap, end_gap + pd.Timedelta(minutes=interval_minutes))
            expected = int(((end_gap + pd.Timedelta(minutes=interval_minutes)) - start_gap).total_seconds() // 60 // interval_minutes)
            if len(klines) > expected:
                db_bot.logger.warning(f"Bybit returned {len(klines)} candles for a range that should have only {expected} — slicing to expected")
                klines = klines[:expected]
            if klines:
                insert_missing_klines(db_bot, symbol, interval, klines)
                db_bot.logger.info(f"Inserted {len(klines)} missing klines for gap {start_gap} - {end_gap}")
            else:
                db_bot.logger.warning(f"No klines returned for gap {start_gap} - {end_gap}")
    except Exception as e:
        db_bot.logger.error(f"Gap-fix error: {e}")
    finally:
        cursor.close()

def fetch_bybit_klines(symbol, interval, interval_minutes, start_time, end_time, category='linear'):
    klines = []
    start_ts = int(start_time.timestamp() * 1000)
    end_ts = int(end_time.timestamp() * 1000)
    while start_ts <= end_ts:
        params = {
            'symbol': symbol,
            'interval': interval,
            'start': start_ts,
            'limit': 1000,
            'category': category
        }
        resp = requests.get("https://api.bybit.com/v5/market/kline", params=params).json()
        if resp['retCode'] != 0 or not resp['result']['list']:
            break
        batch = list(reversed(resp['result']['list']))
        klines.extend(batch)
        last_ts = int(batch[-1][0])
        if last_ts <= start_ts:
            break
        start_ts = last_ts + interval_minutes * 60000
        time.sleep(0.2)
    return klines

def insert_missing_klines(db_bot, symbol, interval, klines):
    cursor = db_bot.conn.cursor()
    insert_sql = f"""
        INSERT INTO {db_config.DB_TRADING_SCHEMA}.kline_data 
        (symbol, interval, start_time, open, high, low, close, volume, turnover, confirmed)
        VALUES (%s, %s, to_timestamp(%s / 1000.0), %s, %s, %s, %s, %s, %s, TRUE)
        ON CONFLICT (symbol, interval, start_time) DO NOTHING
    """
    try:
        values = []
        for k in klines:
            try:
                pd.to_datetime(int(k[0]), unit='ms', utc=True)
            except Exception as e:
                db_bot.logger.error(f"Time conversion failed for {k[0]}: {e}")
                continue
            values.append((
                symbol,
                interval,
                int(k[0]),
                float(k[1]),
                float(k[2]),
                float(k[3]),
                float(k[4]),
                float(k[5]),
                float(k[6])
            ))
        cursor.executemany(insert_sql, values)
        db_bot.conn.commit()
        db_bot.logger.info(f"✅ Actually inserted {cursor.rowcount} klines for {symbol}-{interval}")
    except Exception as e:
        db_bot.conn.rollback()
        db_bot.logger.error(f"Inserting klines failed: {e}")
    finally:
        cursor.close()
