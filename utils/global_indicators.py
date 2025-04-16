"""
  The global indicators file:
  To ensure all bots are using the same algorithms to determine indicators. 
"""
import pandas as pd
from utils.logger import setup_logger

class GlobalIndicators:
    def __init__(self, log_filename="GlobalIndicators.log"):
        self.test = 5
        self.logger = setup_logger(log_filename)
    def __compute_wilder_rsi_loop(self, prices, period=14):
        """
        Compute the Relative Strength Index (RSI) using Wilder's smoothing method.
        Returns a list of RSI values.
        """
        if len(prices) < period + 1:
            return [None] * len(prices)

        rsi = [None] * len(prices)
        gains = []
        losses = []

        # Initial averages
        for i in range(1, period + 1):
            change = prices[i] - prices[i - 1]
            gains.append(max(change, 0))
            losses.append(abs(min(change, 0)))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
        rsi[period] = 100 - (100 / (1 + rs))

        # Apply Wilder's smoothing
        for i in range(period + 1, len(prices)):
            change = prices[i] - prices[i - 1]
            gain = max(change, 0)
            loss = abs(min(change, 0))
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period
            rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
            rsi[i] = 100 - (100 / (1 + rs))

        return rsi

    def compute_indicators(self, df):
        """
        Given a DataFrame with a 'close' column, compute technical indicators:
        - RSI (using a 14-period Wilder smoothing)
        - MACD, MACD Signal, and MACD Histogram (using spans of 12, 26, and 9)
        - A 20-period Moving Average (MA)
        - Bollinger Bands (Upper and Lower) calculated as MA ± 2 * STD
        Returns the DataFrame with new columns added.
        """
        #self.logger.debug(f"Computing indicators for symbol: {df['symbol'].iloc[0]} with {len(df)} rows")
        #self.logger.debug(f"Data for {df['symbol'].iloc[0]}:\n{df.head()}")  # Log the first few rows of data

        # Check for minimum data length for RSI calculation (14 periods)
        if len(df) < 14:
            #self.logger.warning(f"Not enough data for RSI calculation for {df['symbol'].iloc[0]} (less than 14 rows)")
            df['RSI'] = [None] * len(df)  # Return None for RSI if there's not enough data
            return df

        # Check if there are any NaN or infinite values in the 'close' column
        df['close'] = df['close'].replace([float('inf'), -float('inf')], float('nan'))  # Replace infinite values with NaN
        df = df.dropna(subset=['close'])  # Drop rows with NaN in the 'close' column

        # Debug: Log if there are still NaN values in 'close'
        if df['close'].isna().any():
            self.logger.warning(f"NaN values detected in 'close' column for symbol: {df['symbol'].iloc[0]}")

        # Compute RSI only if there are enough rows
        if len(df) >= 14:
            #self.logger.debug(f"Computing RSI for {df['symbol'].iloc[0]}")
            df['RSI'] = self.__compute_wilder_rsi_loop(df['close'].tolist(), period=14)
            #self.logger.debug(f"Computed RSI for {df['symbol'].iloc[0]}: {df['RSI'].head()}")

        # If there are NaN values in RSI after computation, log a warning
        if df['RSI'].isna().any():
            self.logger.warning(f"Some RSI values are NaN for symbol: {df['symbol'].iloc[0]}")

        # Compute MACD and its signal line
        #self.logger.debug(f"Computing MACD for {df['symbol'].iloc[0]}")
        df['EMA12'] = df['close'].ewm(span=12, adjust=False).mean()
        df['EMA26'] = df['close'].ewm(span=26, adjust=False).mean()
        df['MACD'] = df['EMA12'] - df['EMA26']
        df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

        # Debug: Log MACD components
        #self.logger.debug(f"MACD for {df['symbol'].iloc[0]}: {df['MACD'].head()}")
        #self.logger.debug(f"MACD_Signal for {df['symbol'].iloc[0]}: {df['MACD_Signal'].head()}")
        #self.logger.debug(f"MACD_Hist for {df['symbol'].iloc[0]}: {df['MACD_Hist'].head()}")

        # Compute Moving Average and Bollinger Bands (20-period window)
        #self.logger.debug(f"Computing Bollinger Bands for {df['symbol'].iloc[0]}")
        df['MA'] = df['close'].rolling(window=20).mean()
        df['STD'] = df['close'].rolling(window=20).std()
        df['UpperBand'] = df['MA'] + 2 * df['STD']
        df['LowerBand'] = df['MA'] - 2 * df['STD']

        # Handle cases where rolling mean or std might be NaN due to insufficient data
        df['UpperBand'] = df['UpperBand'].bfill()
        df['LowerBand'] = df['LowerBand'].bfill()

        # Debug: Log Bollinger Band calculations
        #self.logger.debug(f"MA for {df['symbol'].iloc[0]}: {df['MA'].head()}")
        #self.logger.debug(f"UpperBand for {df['symbol'].iloc[0]}: {df['UpperBand'].head()}")
        #self.logger.debug(f"LowerBand for {df['symbol'].iloc[0]}: {df['LowerBand'].head()}")

        # Optionally, drop intermediate columns if you don't need them
        df.drop(columns=['EMA12', 'EMA26', 'STD'], inplace=True)

        # Debug: Log the final computed indicators
        #self.logger.debug(f"Final computed indicators for {df['symbol'].iloc[0]}: {df[['RSI', 'MACD', 'MACD_Signal', 'MACD_Hist', 'MA', 'UpperBand', 'LowerBand']].head()}")

        #self.logger.info(f"Computed indicators for {df['symbol'].iloc[0]} with {len(df)} rows")

        return df


