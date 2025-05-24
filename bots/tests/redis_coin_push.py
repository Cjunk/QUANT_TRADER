import redis 
import json
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
r.delete("linear_subscriptions")
r.delete("spot_subscriptions")

#Clearing the most recent subscriptions
r.delete("most_recent_subscriptions:linear")
r.delete("most_recent_subscriptions:spot")
r.delete("spot_coin_subscriptions", "linear_coin_subscriptions", "most_recent_subscriptions:spot", "most_recent_subscriptions:linear")

#Listing the number of 'memebers' in a variable
r.smembers("spot_coin_subscriptions")
r.smembers("linear_coin_subscriptions")

#linear test
r.lpush("linear_coin_subscriptions", json.dumps({"action": "set", "owner":"me","market":"linear","symbols": ["BTCUSDT"], "topics": ["kline.1", "kline.5","kline.60","kline.D","orderbook.200", "trade"]}))
r.lpush("spot_coin_subscriptions", json.dumps({"action": "set", "owner":"me","market":"linear","symbols": ["BTCUSDT"], "topics": ["kline.1", "kline.5","kline.60","kline.D","orderbook.200", "trade"]}))
r.lpush("linear_coin_subscriptions", json.dumps({
    "action": "set",
    "symbols": [],
    "topics": [],
    "market": "spot"
}))


# SUBSCRIPTIONS
r.lpush("spot_coin_subscriptions", json.dumps({
    "action": "set",
    "symbols": [],
    "market": "spot",
    "topics": [],
    "owner": "test_owner"
}))
coins = ["XRPUSDT", "LTCUSDT", "DOTUSDT", "BNBUSDT", "AVAXUSDT", "ATOMUSDT"]
for coin in coins:


r.lpush("spot_coin_subscriptions", json.dumps({
    "action": "set",
    "symbols": ["BTCUSDT"],
    "topics": ["kline.1", "kline.5"],
    "market": "spot",
    "owner": "test_owner"
}))

r.lpush("spot_coin_subscriptions", json.dumps({
    "action": "set",
    "symbols": [],
    "topics": ["trade"],
    "market": "spot",
    "owner": "test_owner"
}))
