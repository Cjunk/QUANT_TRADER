from collections import deque
from enum import Enum

class TradeSignal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"

class TradeState(Enum):
    NEUTRAL = "NEUTRAL"
    READY_LONG = "READY_LONG"
    READY_SHORT = "READY_SHORT"
    LONG = "LONG"
    SHORT = "SHORT"

class TradeDecisionEngine:
    def __init__(self, confirmation_count=3, volume_threshold=1.3):
        self.state = TradeState.NEUTRAL
        self.bias_history = deque(maxlen=confirmation_count)
        self.confirmation_count = confirmation_count
        self.volume_threshold = volume_threshold
        self.entry_price = None
        self.last_signal = {}
    def process_signal(self, stats):
        bias = stats.get("Bias", "❓")
        buy_vol = stats.get("Buy Volume", 0.0)
        sell_vol = stats.get("Sell Volume", 0.0)
        vwap = stats.get("VWAP", 0.0)

        # 1. Track bias history
        if bias in ["📈 Bullish", "📉 Bearish"]:
            self.bias_history.append(bias)

        # 2. Handle entry states
        if self.state == TradeState.NEUTRAL:
            if list(self.bias_history).count("📈 Bullish") >= self.confirmation_count:
                self.state = TradeState.READY_LONG
            elif list(self.bias_history).count("📉 Bearish") >= self.confirmation_count:
                self.state = TradeState.READY_SHORT

        # 3. Confirm and enter
        if self.state == TradeState.READY_LONG and buy_vol > (sell_vol * self.volume_threshold):
            self.state = TradeState.LONG
            self.entry_price = vwap
            return TradeSignal.BUY

        if self.state == TradeState.READY_SHORT and sell_vol > (buy_vol * self.volume_threshold):
            self.state = TradeState.SHORT
            self.entry_price = vwap
            return TradeSignal.SELL

        # 4. Manage position exits (simple version)
        if self.state == TradeState.LONG and list(self.bias_history).count("📉 Bearish") >= self.confirmation_count:
            self.state = TradeState.NEUTRAL
            return TradeSignal.SELL

        if self.state == TradeState.SHORT and list(self.bias_history).count("📈 Bullish") >= self.confirmation_count:
            self.state = TradeState.NEUTRAL
            return TradeSignal.BUY

        return TradeSignal.HOLD
    def update_and_decide(self, symbol, market, bias, vwap, cvd, buy_vol, sell_vol):
        key = (symbol, market)
        last = self.last_signal.get(key)

        # Basic rules: avoid duplicate signals unless conditions change
        signal = "HOLD"
        if bias == "📈 Bullish" and (last != "LONG"):
            signal = "LONG"
        elif bias == "📉 Bearish" and (last != "SHORT"):
            signal = "SHORT"
        elif bias.startswith("⚠️ Divergence") and (last != "HEDGE"):
            signal = "HEDGE"
        else:
            signal = "HOLD"

        self.last_signal[key] = signal
        return signal