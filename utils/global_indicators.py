"""
  The global indicators file:
  To ensure all bots are using the same algorithms to determine indicators. 
"""
import pandas as pd


class GlobalIndicators:
    def __init__(self, log_filename="GlobalIndicators.log"):
        self.test = 5

    def __compute_wilder_rsi_loop(self,prices, period=14):
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
    def compute_indicators(self,df):
      """
      Given a DataFrame with a 'close' column, compute technical indicators:
      - RSI (using a 14-period Wilder smoothing)
      - MACD, MACD Signal, and MACD Histogram (using spans of 12, 26, and 9)
      - A 20-period Moving Average (MA)
      - Bollinger Bands (Upper and Lower) calculated as MA ± 2 * STD
      Returns the DataFrame with new columns added.
      """
      # Compute RSI. Convert the close column to a list.
      df['RSI'] = self.__compute_wilder_rsi_loop(df['close'].tolist(), period=14)
      
      # Compute MACD and its signal line
      df['EMA12'] = df['close'].ewm(span=12, adjust=False).mean()
      df['EMA26'] = df['close'].ewm(span=26, adjust=False).mean()
      df['MACD'] = df['EMA12'] - df['EMA26']
      df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
      df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
      
      # Compute Moving Average and Bollinger Bands (20-period window)
      df['MA'] = df['close'].rolling(window=20).mean()
      df['STD'] = df['close'].rolling(window=20).std()
      df['UpperBand'] = df['MA'] + 2 * df['STD']
      df['LowerBand'] = df['MA'] - 2 * df['STD']
      
      # Optionally, drop the intermediate columns if you don't need them:
      df.drop(columns=['EMA12', 'EMA26', 'STD'], inplace=True)
    
      return df