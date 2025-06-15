from collections import deque
from datetime import datetime, timedelta

# === Debug Mode ===
DEBUG = False

class TradeWindow:
    """
    Maintains a rolling window of trades and provides volume and price analysis over specified time intervals.

    Key features:
    - Keeps only recent trades within a max time window.
    - Computes VWAP, CVD, and volume splits over custom intervals.
    - Provides trend bias by comparing early vs. late trade activity in the interval.
    """

    def __init__(self, max_duration_secs=900):
        """
        Initializes the trade window with a maximum rolling duration (default 900 seconds / 15 minutes).

        Args:
            max_duration_secs (int): Maximum time in seconds to retain trade data.
        """
        self.trades = deque()  # Holds individual trades in time order
        self.max_duration = timedelta(seconds=max_duration_secs)
        if DEBUG:
            print("[TradeWindow] Initialized with max duration:", self.max_duration)

    def add_trade(self, price, volume, side, timestamp):
        """
        Adds a new trade to the rolling window and trims old trades.

        Args:
            price (float): Trade price.
            volume (float): Trade volume.
            side (str): 'Buy' or 'Sell'.
            timestamp (datetime): Time the trade occurred.
        """
        self.trades.append({
            "price": price,
            "volume": volume,
            "side": side,
            "timestamp": timestamp
        })
        if DEBUG:
            print(f"[TradeWindow] Added trade @ {timestamp} | {side} {volume} @ {price}")
        self._trim_old(timestamp)

    def _trim_old(self, now):
        """
        Removes trades older than the allowed max duration.

        Args:
            now (datetime): Current reference time (typically timestamp of the latest trade).
        """
        cutoff = now - self.max_duration
        removed = 0
        while self.trades and self.trades[0]['timestamp'] < cutoff:
            self.trades.popleft()
            removed += 1
        if DEBUG and removed:
            print(f"[TradeWindow] Trimmed {removed} old trades")

    def compute_stats(self, interval_secs):
        """
        Computes VWAP, CVD, and volume distribution over a recent interval.

        Args:
            interval_secs (int): The number of seconds to look back for trade data.

        Returns:
            dict: {
                'VWAP': float,
                'CVD': float,
                'Buy Volume': float,
                'Sell Volume': float,
                'Total Volume': float
            }
        """
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=interval_secs)
        relevant = [t for t in self.trades if t["timestamp"] >= cutoff]

        buys = sells = total_volume = total_value = 0
        for t in relevant:
            v, p = t["volume"], t["price"]
            if t["side"] == "Buy":
                buys += v
            else:
                sells += v
            total_volume += v
            total_value += p * v

        vwap = total_value / total_volume if total_volume else 0
        cvd = buys - sells

        if DEBUG:
            print(f"[TradeWindow] compute_stats({interval_secs}s) | VWAP={vwap:.2f}, CVD={cvd:.2f}, Buy={buys}, Sell={sells}")

        return {
            "VWAP": vwap,
            "CVD": cvd,
            "Buy Volume": buys,
            "Sell Volume": sells,
            "Total Volume": total_volume
        }

    def get_trend_inputs(self, interval_secs):
        """
        Extracts VWAP and CVD values from early and late portions of the interval to estimate trend direction.

        Args:
            interval_secs (int): Timeframe to analyze (e.g., 60 for 1-minute trend estimation).

        Returns:
            Tuple[float, float, float, float]: vwap_start, vwap_end, cvd_start, cvd_end
                or (None, None, None, None) if not enough data.
        """
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=interval_secs)
        relevant = [t for t in self.trades if t["timestamp"] >= cutoff]

        if len(relevant) < 2:
            if DEBUG:
                print("[TradeWindow] Not enough data for trend computation")
            return None, None, None, None

        third = len(relevant) // 3
        start_trades = relevant[:third]
        end_trades = relevant[-third:]

        def compute(trades):
            buys = sells = volume = value = 0
            for t in trades:
                v, p = t["volume"], t["price"]
                if t["side"] == "Buy":
                    buys += v
                else:
                    sells += v
                volume += v
                value += v * p
            vwap = value / volume if volume else 0
            cvd = buys - sells
            return vwap, cvd

        vwap_start, cvd_start = compute(start_trades)
        vwap_end, cvd_end = compute(end_trades)

        if DEBUG:
            print(f"[TradeWindow] Trend Inputs for {interval_secs}s | VWAP: {vwap_start:.2f} → {vwap_end:.2f}, CVD: {cvd_start:.2f} → {cvd_end:.2f}")

        return vwap_start, vwap_end, cvd_start, cvd_end
