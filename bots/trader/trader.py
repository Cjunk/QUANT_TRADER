import json
import sys
import os
import time

DEBUG = True

# Add project root to sys.path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.redis_handler import RedisHandler
from utils.logger import setup_logger

# Import your own config file for this bot
import config.trader_config as trader_config
import config.config_redis as config_redis
# Send subscription request to WebSocket bot

class MockTrader:
    def __init__(self, symbol="BTCUSDT", market="spot", bot_id=1001):
        self.symbol = symbol
        self.market = market
        self.channel = f"{config_redis.TRADE_SIGNAL_PREFIX}:{self.market}:{self.symbol}"
        self.bot_id = bot_id
        self.logger = setup_logger("MockTrader.log")
        self.logger.setLevel("DEBUG" if DEBUG else "INFO")
        self.redis = RedisHandler(config_redis, self.logger, service_name="MockTrader", debug=DEBUG)
        self.redis.connect()
        self.logger.info(f"[MockTrader] ✅ Connected to Redis, subscribing to '{self.channel}' (bot_id={self.bot_id})")
        self.pubsub = self.redis.client.pubsub()
        self.pubsub.subscribe(self.channel)

        self.last_decision = None
        self.last_decision_time = 0
        self.required_consistency = 2
        self.consistent_count = 0
        self.decision_interval = 30  # Minimum seconds between decisions
        subscription_request = {
            "action": "add",
            "owner": f"mock_trader_{self.bot_id}",
            "market": self.market,
            "symbols": [self.symbol],
            "topics": ["publicTrade"]
        }
        self.redis.client.lpush("spot_coin_subscriptions", json.dumps(subscription_request))
        self.logger.info(f"[MockTrader] 📡 Requested trade subscription for {self.symbol} on {self.market}")

    def listen(self):
        weights = {"300s": 2, "600s": 3}
        try:
            while True:
                message = self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
                if message is None or message["type"] != "message":
                    continue

                data = json.loads(message["data"])
                symbol = data.get("symbol")
                market = data.get("market")
                price = data.get("price")
                timestamp = data.get("timestamp")
                stats = data.get("stats", {})

                total_score = 0
                bias_breakdown = []

                for interval, interval_stats in stats.items():
                    if not interval_stats.get("has_enough_data"):
                        self.logger.info(
                            f"[Trader][{self.bot_id}] {symbol}-{interval}  | Price = {price} | ⏳ {interval_stats.get('message')} @ {timestamp}"
                        )
                        continue

                    bias = interval_stats.get("Bias")
                    weight = weights.get(interval, 1)

                    if bias == "BUY":
                        total_score += weight
                    elif bias == "SELL":
                        total_score -= weight
                    elif "Divergence" in bias:
                        if "Price Up" in bias and "CVD Down" in bias:
                            total_score -= 0.5 * weight  # bearish
                        elif "Price Down" in bias and "CVD Up" in bias:
                            total_score += 0.5 * weight  # bullish

                    bias_breakdown.append(f"{interval}:{bias}")

                if total_score >= 5:
                    new_decision = "MEGA LONG"
                    decision_emoji = "🚀🚀"
                elif total_score >= 3:
                    new_decision = "EXTRA LONG"
                    decision_emoji = "🟢🟢"
                elif total_score >= 1:
                    new_decision = "GO LONG"
                    decision_emoji = "🟢"
                elif total_score <= -5:
                    new_decision = "MEGA SHORT"
                    decision_emoji = "💣💣"
                elif total_score <= -3:
                    new_decision = "EXTRA SHORT"
                    decision_emoji = "🔴🔴"
                elif total_score <= -1:
                    new_decision = "GO SHORT"
                    decision_emoji = "🔴"
                else:
                    new_decision = "HOLD"
                    decision_emoji = "🟡"

                now = time.time()
                consistent = new_decision == self.last_decision

                if consistent:
                    self.consistent_count += 1
                else:
                    self.consistent_count = 1
                    self.last_decision = new_decision

                if self.consistent_count >= self.required_consistency and (now - self.last_decision_time > self.decision_interval):
                    self.last_decision_time = now
                else:
                    new_decision = "HOLD"
                    decision_emoji = "🟡"

                log_line = (
                    f"[Trader][{self.bot_id}] {symbol}-{market} | | Price = {price} | Biases: {', '.join(bias_breakdown)} "
                    f"=> Score: {total_score:.1f} → {decision_emoji} {new_decision} @ {timestamp}"
                )
                self.logger.info(log_line)
        except KeyboardInterrupt:
            self.logger.info("\n[MockTrader] 🛑 Stopped.")


if __name__ == "__main__":
    trader = MockTrader()
    trader.listen()


