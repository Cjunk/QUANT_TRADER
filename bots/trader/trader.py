import os, sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
import json
import time
from utils.redis_handler import RedisHandler
from utils.logger import setup_logger
from bybit_controller import BybitController
import config.config_redis as config_redis

class StealthTrader:
    """
    StealthTrader listens for trade signals on a Redis channel(config_redis.TRADE_SIGNAL_PREFIX:market:symbol), aggregates multi-interval bias signals,
    and executes trades when a consistent signal is detected. It uses a weighted scoring system for
    different intervals and only acts when a signal is confirmed for a set number of consecutive checks.

    Attributes:
        symbol (str): Trading symbol, e.g., "BTCUSDT".
        market (str): Market type, e.g., "linear" (for USDT perpetuals).
        channel (str): Redis channel to listen for trade signals.
        bot_id (int): Unique identifier for the bot instance.
        logger: Logger instance for logging events and errors.
        redis: RedisHandler instance for Redis communication.
        pubsub: Redis pubsub object for subscribing to channels.
        last_decision (str): Last trade decision ("LONG", "SHORT", "HOLD").
        last_decision_time (float): Timestamp of the last decision.
        required_consistency (int): Number of consecutive signals required before acting.
        consistent_count (int): Counter for consecutive consistent signals.
        decision_interval (int): Minimum seconds between trade decisions.
        trigger_score_long (float): Score threshold to trigger a LONG trade.
        trigger_score_short (float): Score threshold to trigger a SHORT trade.
        executor: TradeExecutor instance for placing orders.
    """

    def __init__(self, symbol="BTCUSDT", market="linear", bot_id=1001, required_consistency=1,
                 consistent_count=0, decision_interval=30, trigger_score=3, letherage_max=10):
        self.symbol = symbol  # Trading symbol
        self.market = market  # Market type (e.g., "linear")
        self.channel = f"{config_redis.TRADE_SIGNAL_PREFIX}:{self.market}:{self.symbol}"  # Redis channel for signals
        self.bot_id = bot_id  # Unique bot identifier
        self.leverage_max = letherage_max
        self.logger = setup_logger("StealthTrader.log")  # Logger for this bot
        self.redis = RedisHandler(config_redis, self.logger, service_name="StealthTrader")  # Redis handler
        self.redis.connect()
        self.pubsub = self.redis.client.pubsub()  # Redis pubsub for listening
        self.pubsub.subscribe(self.channel)  # Subscribe to trade signal channel

        self.last_decision = None  # Last trade decision ("LONG", "SHORT", "HOLD")
        self.last_decision_time = 0  # Timestamp of last trade decision
        self.required_consistency = required_consistency  # Signals needed before acting
        self.consistent_count = consistent_count  # Counter for consecutive signals
        self.decision_interval = decision_interval  # Minimum seconds between trades

        self.trigger_score_long = trigger_score  # Score threshold for LONG
        self.trigger_score_short = -trigger_score  # Score threshold for SHORT

        self.executor = BybitController(self.logger)  # Trade executor for placing orders

        self.account_snapshot()  # Log initial account balance

        # Request subscription for trade data (for WebSocket bots)
        subscription_request = {
            "action": "add",
            "owner": f"stealth_trader_{self.bot_id}",
            "market": self.market,
            "symbols": [self.symbol],
            "topics": ["publicTrade"]
        }
        self.redis.client.lpush("spot_coin_subscriptions", json.dumps(subscription_request))

        print("\n📱 StealthTrader is Live")
        print(f"▶ Symbol: {self.symbol}")
        print(f"▶ Market: {self.market}")
        print(f"▶ Trigger Score: LONG ≥ {self.trigger_score_long} | SHORT ≤ {self.trigger_score_short}")
        print(f"▶ Required Consistency: {self.required_consistency}")
        print(f"▶ Decision Interval: {self.decision_interval} seconds\n")

    def account_snapshot(self):
        """
        Logs the initial USDT account balance using the TradeExecutor.
        """
        try:
            balance = self.executor.get_available_usdt()
            self.logger.info("🏦 Initial USDT Balance: %.2f", balance)
        except Exception as e:
            self.logger.error("❌ Failed to fetch initial account snapshot: %s", e)

    def listen(self):
        """
        Main loop: listens for trade signals, aggregates interval biases, and places trades
        when a consistent signal is detected for the required number of checks.
        """
        weights = {"60s": 1, "300s": 2, "600s": 3}  # Interval weights for scoring
        try:
            while True:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message is None or message["type"] != "message":
                    continue

                data = json.loads(message["data"])
                stats = data.get("stats", {})
                price = data.get("price")
                timestamp = data.get("timestamp")

                total_score = 0
                bias_breakdown = []

                # Aggregate interval biases and calculate total score
                for interval, interval_stats in stats.items():
                    if not interval_stats.get("has_enough_data"):
                        continue

                    bias = interval_stats.get("Bias")
                    weight = weights.get(interval, 1)

                    if bias == "BUY":
                        total_score += weight
                    elif bias == "SELL":
                        total_score -= weight
                    elif "Divergence" in bias:
                        if "Price Up" in bias and "CVD Down" in bias:
                            total_score -= 0.5 * weight
                        elif "Price Down" in bias and "CVD Up" in bias:
                            total_score += 0.5 * weight

                    bias_breakdown.append(f"{interval}:{bias}")

                print(f"[{timestamp}] {self.symbol} Price: {price} | Biases: {', '.join(bias_breakdown)} | Score: {total_score:.1f}")

                # Decision logic based on score thresholds
                if total_score >= self.trigger_score_long:
                    new_decision = "LONG"
                elif total_score <= self.trigger_score_short:
                    new_decision = "SHORT"
                else:
                    new_decision = "HOLD"

                now = time.time()
                consistent = new_decision == self.last_decision

                # Track consistency of signals
                if consistent:
                    self.consistent_count += 1
                    if self.consistent_count == self.required_consistency:
                        print(f"✅ Consistent signal for {new_decision} confirmed at {timestamp} (score={total_score:.1f})")
                else:
                    self.consistent_count = 1
                    self.last_decision = new_decision

                # Place trade if signal is consistent and interval has passed
                if self.consistent_count >= self.required_consistency and new_decision in ["LONG", "SHORT"]:
                    if now - self.last_decision_time > self.decision_interval:
                        self.last_decision_time = now
                        side = "short" if new_decision == "LONG" else "long"
                        self.executor.place_limit_order(self.symbol, side)

        except KeyboardInterrupt:
            self.logger.info("\n[StealthTrader] 🛑 Stopped.")

if __name__ == "__main__":
    trader = StealthTrader()
    trader.listen()

