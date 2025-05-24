"""
GlobalIndicators Utility
Author: Jericho

Purpose:
--------
- Provides computation of technical indicators for trading bots (RSI, MACD, Bollinger Bands, MA, volume metrics, RVOL, etc.).
- Used by PreprocessorBot and other analytics modules to enrich kline data.

Indicators & Variables:
-----------------------
- RSI_PERIOD: Window for Relative Strength Index (RSI) calculation (default: 14)
- EMA12_SPAN, EMA26_SPAN: Spans for MACD calculation (default: 12, 26)
- MACD_SIGNAL_SPAN: Span for MACD signal line (default: 9)
- MA_WINDOW: Window for moving average and Bollinger Bands (default: 20)
- VOLUME_MA_WINDOW: Window for volume moving average and RVOL (default: 20)

Key Methods:
------------
- compute_indicators(df): Adds all supported indicators to the DataFrame.
- __compute_wilder_rsi_loop(prices, period): Internal helper for RSI.

Usage:
------
- Instantiate once per bot: GlobalIndicators()
- Call compute_indicators(df) on a DataFrame with 'close' and 'volume' columns.

"""
import pandas as pd
from utils.logger import setup_logger

class GlobalIndicators:
    # ==== Configurable Constants (no hardcoded values) ====
    RSI_PERIOD = 6
    EMA12_SPAN = 12
    EMA26_SPAN = 26
    MACD_SIGNAL_SPAN = 9
    MA_WINDOW = 10
    VOLUME_MA_WINDOW = 20

    def __init__(self, log_filename="GlobalIndicators.log"):
        self.logger = setup_logger(log_filename)

    def __compute_wilder_rsi_loop(self, prices, period=None):
        period = period or self.RSI_PERIOD
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
        if avg_loss == 0:
            rsi[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[period] = round(100 - (100 / (1 + rs)), 6)

        for i in range(period + 1, len(prices)):
            change = prices[i] - prices[i - 1]
            gain = max(change, 0)
            loss = abs(min(change, 0))
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
            if avg_loss == 0:
                rsi[i] = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi[i] = round(100 - (100 / (1 + rs)), 6)

        return rsi

    def compute_indicators(self, df):
        # Validate input
        if 'close' not in df.columns or df['close'].isnull().all():
            self.logger.warning("No 'close' data available.")
            return df

        # Convert columns to numeric
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df.dropna(subset=['close'])

        # RSI
        if len(df) >= self.RSI_PERIOD:
            df['RSI'] = self.__compute_wilder_rsi_loop(df['close'].tolist(), period=self.RSI_PERIOD)
        else:
            df['RSI'] = [None] * len(df)

        # MACD
        df['EMA12'] = df['close'].ewm(span=self.EMA12_SPAN, adjust=False).mean()
        df['EMA26'] = df['close'].ewm(span=self.EMA26_SPAN, adjust=False).mean()
        df['MACD'] = df['EMA12'] - df['EMA26']
        df['MACD_Signal'] = df['MACD'].ewm(span=self.MACD_SIGNAL_SPAN, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

        # Bollinger Bands & MA
        df['MA'] = df['close'].rolling(window=self.MA_WINDOW, min_periods=1).mean()
        df['STD'] = df['close'].rolling(window=self.MA_WINDOW, min_periods=1).std()
        df['UpperBand'] = df['MA'] + 2 * df['STD']
        df['LowerBand'] = df['MA'] - 2 * df['STD']

        # Volume metrics
        df['Volume_MA'] = df['volume'].rolling(window=self.VOLUME_MA_WINDOW, min_periods=1).mean()
        df['Volume_Change'] = df['volume'].pct_change().fillna(0)
        df['Volume_Slope'] = df['volume'].diff().fillna(0)

        # Relative Volume (RVOL)
        df['RVOL'] = df['volume'] / df['Volume_MA']

        # Clean up and fill bands
        df[['UpperBand', 'LowerBand']] = df[['UpperBand', 'LowerBand']].bfill().ffill()

        df.drop(columns=['EMA12', 'EMA26', 'STD'], inplace=True)

        return df

