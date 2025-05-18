import redis 
import json
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
r.delete("linear_subscriptions")
r.delete("spot_subscriptions")

#Clearing the most recent subscriptions
r.delete("most_recent_subscriptions:linear")
r.delete("most_recent_subscriptions:spot")
#Listing the number of 'memebers' in a variable
r.smembers("spot_coin_subscriptions")
r.smembers("linear_coin_subscriptions")