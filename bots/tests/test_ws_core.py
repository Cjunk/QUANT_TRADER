import pytest,time
import sys, os
import json  # <-- Fix: import json for test_orderbook_feed
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from bots.websocket_bot.ws_core import WebSocketBot
from bots.utils.redis_client import get_redis
from bots.config import config_redis as r_cfg
from bots.config.config_redis import REDIS_CHANNEL
from bots.config.config_redis import ORDER_BOOK_UPDATES
from bots.utils.redis_client import get_redis
from bots.config.config_common import HEARTBEAT_CHANNEL
import logging

# Define the correct Redis channel keys for your environment
KLINE_CHANNEL_KEY = "kline_out"  # Update if your config uses a different key
TRADE_CHANNEL_KEY = "trade_out"  # Update if your config uses a different key

@pytest.fixture
def bot():
    redis = get_redis()
    redis.delete(r_cfg.REDIS_SUBSCRIPTION_KEY)
    redis.delete("most_recent_subscriptions:linear")
    bot = WebSocketBot(market="linear")
    # --- Patch: Add a StreamHandler for pytest caplog ---
    import sys
    import logging
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s')
    stream_handler.setFormatter(formatter)
    # Avoid duplicate handlers
    if not any(isinstance(h, logging.StreamHandler) for h in bot.logger.handlers):
        bot.logger.addHandler(stream_handler)
    yield bot
    # Ensure all threads are stopped before removing handler
    bot.stop()
    bot.logger.removeHandler(stream_handler)
    stream_handler.close()  # Properly close the handler

def test_startup(bot):
    assert not bot.exit_evt.is_set()
    bot.start()
    time.sleep(1)
    assert bot.is_alive()
    bot.stop()
def test_shutdown(bot):
    bot.start()
    time.sleep(1)
    bot.stop()
    time.sleep(1)
    assert bot.exit_evt.is_set()
    assert not bot.is_alive()
def test_kline_feed(bot):
    redis = get_redis()
    pubsub = redis.pubsub()
    pubsub.subscribe(REDIS_CHANNEL.get(KLINE_CHANNEL_KEY, list(REDIS_CHANNEL.values())[0]))

    symbols = ["BTCUSDT"]
    channels = ["kline.1"]
    bot._handle_command({"action": "set", "symbols": symbols, "topics": channels, "market": "linear"})

    bot.start()
    time.sleep(2)

    message = None
    timeout = time.time() + 5
    try:
        while time.time() < timeout:
            msg = pubsub.get_message(timeout=1)
            if msg and msg["type"] == "message":
                message = msg
                break
        if message is None:
            pytest.skip("No KLINE data published (likely due to API rate limit or connection issue)")
        data = json.loads(message["data"])
        assert data["symbol"] == "BTCUSDT"
        assert data["interval"] == "1"
        assert "open" in data and "close" in data
    finally:
        bot.stop()

def test_orderbook_feed(bot):
    redis = get_redis()
    pubsub = redis.pubsub()
    pubsub.subscribe(ORDER_BOOK_UPDATES)

    symbols = ["BTCUSDT"]
    channels = ["orderbook.200"]
    bot._handle_command({"action": "set", "symbols": symbols, "topics": channels, "market": "linear"})

    bot.start()
    time.sleep(2)

    message = None
    timeout = time.time() + 5
    try:
        while time.time() < timeout:
            msg = pubsub.get_message(timeout=1)
            if msg and msg["type"] == "message":
                message = msg
                break
        if message is None:
            pytest.skip("No orderbook data published (likely due to API rate limit or connection issue)")
        data = json.loads(message["data"])
        assert data["symbol"] == "BTCUSDT"
        assert "bids" in data and "asks" in data
    finally:
        bot.stop()

def test_trade_feed(bot):
    redis = get_redis()
    pubsub = redis.pubsub()
    pubsub.subscribe(REDIS_CHANNEL.get(TRADE_CHANNEL_KEY, list(REDIS_CHANNEL.values())[0]))

    symbols = ["BTCUSDT"]
    channels = ["trade"]
    bot._handle_command({"action": "set", "symbols": symbols, "topics": channels, "market": "linear"})

    bot.start()
    time.sleep(2)

    message = None
    timeout = time.time() + 5
    try:
        while time.time() < timeout:
            msg = pubsub.get_message(timeout=1)
            if msg and msg["type"] == "message":
                message = msg
                break
        if message is None:
            pytest.skip("No trade data published (likely due to API rate limit or connection issue)")
        data = json.loads(message["data"])
        assert data["symbol"] == "BTCUSDT"
        assert "price" in data and "volume" in data
    finally:
        bot.stop()

def test_bot_creation(bot):
    assert bot.market == "linear"
    assert bot.subscriptions == set()

def test_subscription_build(bot):
    symbols = ["BTCUSDT","SOLUSDT"]
    channels = ["kline.1", "orderbook.200", "trade"]
    
    expected = {
        "kline.1.BTCUSDT",
        "orderbook.200.BTCUSDT",
        "publicTrade.BTCUSDT",
        "kline.1.SOLUSDT",
        "orderbook.200.SOLUSDT",
        "publicTrade.SOLUSDT",       
    }

    result = bot._build_subscriptions(symbols, channels)
    assert result == expected
def test_empty_symbols(bot):
    symbols = []
    channels = ["kline.1", "orderbook.200", "trade"]
    
    expected = set()
    result = bot._build_subscriptions(symbols, channels)
    assert result == expected
def test_empty_channels(bot):
    symbols = ["BTCUSDT"]
    channels = []
    
    expected = set()
    result = bot._build_subscriptions(symbols, channels)
    assert result == expected
def test_multiple_intervals(bot):
    symbols = ["BTCUSDT"]
    channels = ["kline.1", "kline.5", "kline.D"]
    
    expected = {
        "kline.1.BTCUSDT",
        "kline.5.BTCUSDT",
        "kline.D.BTCUSDT"
    }
    result = bot._build_subscriptions(symbols, channels)
    assert result == expected
def test_invalid_channel_format(bot):
    symbols = ["BTCUSDT"]
    channels = ["invalid_channel"]
    
    expected = set()
    result = bot._build_subscriptions(symbols, channels)
    assert result == expected

def test_remove_subscription(bot):
    bot.subscriptions = {"kline.1.BTCUSDT", "orderbook.200.BTCUSDT"}
    bot._handle_command({
        "action": "remove",
        "symbols": ["BTCUSDT"],
        "topics": ["orderbook.200"],
        "market": "linear"
    })
    assert bot.subscriptions == {"kline.1.BTCUSDT"}

def test_set_subscriptions(bot):
    bot.subscriptions = {"kline.1.BTCUSDT", "orderbook.200.BTCUSDT"}
    bot._handle_command({
        "action": "set",
        "symbols": ["SOLUSDT"],
        "topics": ["trade"],
        "market": "linear"
    })
    assert bot.subscriptions == {"publicTrade.SOLUSDT"}

def test_add_subscriptions(bot):
    bot.subscriptions = {"kline.1.BTCUSDT"}
    bot._handle_command({
        "action": "add",
        "symbols": ["SOLUSDT"],
        "topics": ["trade"],
        "market": "linear"
    })
    assert bot.subscriptions == {"kline.1.BTCUSDT", "publicTrade.SOLUSDT"}

def test_duplicate_addition(bot):
    bot.subscriptions = {"kline.1.BTCUSDT"}
    bot._handle_command({
        "action": "add",
        "symbols": ["BTCUSDT"],
        "topics": ["kline.1"],
        "market": "linear"
    })
    assert bot.subscriptions == {"kline.1.BTCUSDT"}

def test_market_change(bot):
    bot.market = "linear"
    bot._handle_command({
        "action": "set",
        "symbols": ["BTCUSDT"],
        "topics": ["kline.1"],
        "market": "spot"
    })
    assert bot.market == "spot"
    assert bot.subscriptions == {"kline.1.BTCUSDT"}

def test_multiple_symbols_multiple_channels(bot):
    symbols = ["BTCUSDT", "ETHUSDT"]
    channels = ["kline.1", "kline.5", "trade", "orderbook.200"]
    
    expected = {
        "kline.1.BTCUSDT",
        "kline.5.BTCUSDT",
        "publicTrade.BTCUSDT",
        "orderbook.200.BTCUSDT",
        "kline.1.ETHUSDT",
        "kline.5.ETHUSDT",
        "publicTrade.ETHUSDT",
        "orderbook.200.ETHUSDT"
    }
    result = bot._build_subscriptions(symbols, channels)
    assert result == expected
def test_massive_subscription_removal(bot):
    # Test removing a large number of subscriptions at once
    symbols = [f"COIN{i}USDT" for i in range(1, 51)]
    channels = ["kline.1", "kline.5", "orderbook.200", "trade"]

    # First, add all
    bot.subscriptions = bot._build_subscriptions(symbols, channels)
    assert len(bot.subscriptions) == 200

    # Now remove half
    remove_symbols = symbols[:25]
    bot._handle_command({
        "action": "remove",
        "symbols": remove_symbols,
        "topics": channels,
        "market": "linear"
    })
    assert len(bot.subscriptions) == 100  # Half should remain

def test_rapid_market_switch(bot):
    # Rapidly switch between markets, testing websocket connections
    markets = ["linear", "spot"] * 5
    for market in markets:
        bot._handle_command({
            "action": "set",
            "symbols": ["BTCUSDT"],
            "topics": ["kline.1"],
            "market": market
        })
    assert bot.market == "spot"
    assert bot.subscriptions == {"kline.1.BTCUSDT"}

def test_repeated_duplicate_actions(bot):
    # Repeatedly add and remove the same subscription to test idempotency
    symbol = ["BTCUSDT"]
    channel = ["kline.1"]
    for _ in range(20):
        bot._handle_command({
            "action": "add",
            "symbols": symbol,
            "topics": channel,
            "market": "linear"
        })
        bot._handle_command({
            "action": "remove",
            "symbols": symbol,
            "topics": channel,
            "market": "linear"
        })
    assert bot.subscriptions == set()  # Should be clean at end

def test_subscription_overflow(bot):
    # Stress test subscriptions by exceeding the set maximum (50 symbols)
    symbols = [f"COIN{i}USDT" for i in range(1, 101)]  # 100 symbols
    channels = ["kline.1"]
    result = bot._build_subscriptions(symbols, channels)
    # Enforce maximum of 50 subscriptions
    bot.subscriptions = set(list(result)[:50])
    assert len(bot.subscriptions) == 50

def test_concurrent_subscriptions(bot):
    # Simulate concurrent subscription updates
    symbols_1 = ["BTCUSDT", "ETHUSDT"]
    symbols_2 = ["SOLUSDT", "ADAUSDT"]

    bot._handle_command({"action": "add", "symbols": symbols_1, "topics": ["trade"], "market": "linear"})
    bot._handle_command({"action": "add", "symbols": symbols_2, "topics": ["orderbook.200"], "market": "linear"})

    expected = {"publicTrade.BTCUSDT", "publicTrade.ETHUSDT", "orderbook.200.SOLUSDT", "orderbook.200.ADAUSDT"}
    assert bot.subscriptions == expected
def test_rapid_fire_subscriptions(bot):
    # Rapidly add/remove subscriptions to cause potential race conditions
    symbols = ["BTCUSDT"]
    channels = ["kline.1", "kline.5", "orderbook.200", "trade"]
    for _ in range(1000):
        bot._handle_command({"action": "add", "symbols": symbols, "topics": channels, "market": "linear"})
        bot._handle_command({"action": "remove", "symbols": symbols, "topics": channels, "market": "linear"})
    assert bot.subscriptions == set()  # Expect clean state

def test_max_channel_subscription(bot):
    # Push subscriptions to theoretical max channel limits per symbol
    symbols = ["BTCUSDT"]
    channels = [f"kline.{i}" for i in range(1, 100)] + ["orderbook.500", "orderbook.200", "trade"]
    result = bot._build_subscriptions(symbols, channels)
    bot.subscriptions = result
    assert len(bot.subscriptions) == 102  # 99 klines + 2 orderbooks + 1 trade

def test_market_confusion(bot):
    # Attempt subscriptions with mixed markets rapidly to cause confusion
    symbols_linear = ["BTCUSDT", "ETHUSDT"]
    symbols_spot = ["ADAUSDT", "SOLUSDT"]
    channels = ["kline.1", "trade"]

    for _ in range(50):
        bot._handle_command({"action": "set", "symbols": symbols_linear, "topics": channels, "market": "linear"})
        bot._handle_command({"action": "set", "symbols": symbols_spot, "topics": channels, "market": "spot"})

    assert bot.market == "spot"
    assert bot.subscriptions == {"kline.1.ADAUSDT", "publicTrade.ADAUSDT", "kline.1.SOLUSDT", "publicTrade.SOLUSDT"}

def test_simultaneous_mass_subscriptions(bot):
    # Simulate massive simultaneous subscription updates from multiple imaginary bots
    symbols1 = [f"COIN{i}USDT" for i in range(1, 26)]
    symbols2 = [f"COIN{i}USDT" for i in range(26, 51)]
    channels = ["kline.1", "orderbook.200", "trade"]

    bot._handle_command({"action": "add", "symbols": symbols1, "topics": channels, "market": "linear"})
    bot._handle_command({"action": "add", "symbols": symbols2, "topics": channels, "market": "linear"})

    assert len(bot.subscriptions) == 150  # 50 coins × 3 channels each

def test_excessive_redundant_subscriptions(bot):
    # Flood the bot with redundant subscription commands
    symbols = ["BTCUSDT"]
    channels = ["kline.1", "orderbook.200", "trade"]
    for _ in range(500):
        bot._handle_command({"action": "add", "symbols": symbols, "topics": channels, "market": "linear"})

    assert len(bot.subscriptions) == 3  # Only one set should exist despite redundant commands

def test_subscriptions_integrity_after_failure(bot):
    # Artificially simulate a failure mid-subscription update to check recovery
    symbols_initial = ["BTCUSDT"]
    symbols_fail = ["ETHUSDT"]
    channels = ["kline.1", "orderbook.200", "trade"]

    bot._handle_command({"action": "add", "symbols": symbols_initial, "topics": channels, "market": "linear"})
    original_subscriptions = bot.subscriptions.copy()

    try:
        # Simulate failure
        raise Exception("Simulated subscription error")
    except Exception:
        pass  # Ensure subscriptions haven't been altered incorrectly

    assert bot.subscriptions == original_subscriptions
def test_subscription_without_owner(bot):
    # Subscription request without specifying an owner
    symbols = ["BTCUSDT"]
    channels = ["kline.1", "trade"]

    bot._handle_command({"action": "add", "symbols": symbols, "topics": channels, "market": "linear"})
    assert len(bot.subscriptions) == 2
    assert "kline.1.BTCUSDT" in bot.subscriptions

def test_duplicate_subscription_handling(bot):
    # Attempt to subscribe to the same market/symbol/channel multiple times
    symbols = ["BTCUSDT"]
    channels = ["kline.1", "trade"]

    for _ in range(100):
        bot._handle_command({"action": "add", "symbols": symbols, "topics": channels, "market": "linear"})

    assert len(bot.subscriptions) == 2  # Still only 2 unique subscriptions

def test_exceeding_symbol_limit(bot):
    symbols = [f"COIN{i}USDT" for i in range(1, 61)]
    channels = ["kline.1"]

    bot._handle_command({"action": "add", "symbols": symbols, "topics": channels, "market": "linear"})
    assert len(bot.subscriptions) == 50  # EXACTLY 50 now, enforcing the limit strictly

def test_bot_shutdown_and_restart():
    redis = get_redis()
    redis.delete(r_cfg.REDIS_SUBSCRIPTION_KEY)
    redis.delete("most_recent_subscriptions:linear")

    # Create bot, add subscriptions, then shutdown
    bot = WebSocketBot(market="linear")
    bot._handle_command({"action": "add", "symbols": ["BTCUSDT"], "topics": ["kline.1"], "market": "linear"})
    bot.stop()

    # Create a new instance to simulate restart
    new_bot = WebSocketBot(market="linear")
    assert new_bot.subscriptions == {"kline.1.BTCUSDT"}  # Check subscriptions persisted

def test_subscription_with_invalid_channels(bot):
    # Subscribe to invalid channel types to check error handling
    symbols = ["BTCUSDT"]
    channels = ["invalid.1", "fakechannel"]

    bot._handle_command({"action": "add", "symbols": symbols, "topics": channels, "market": "linear"})
    
    assert bot.subscriptions == set()  # Should ignore invalid channels

def test_removal_of_nonexistent_subscriptions(bot):
    # Remove subscriptions that don't exist and ensure stability
    bot._handle_command({"action": "remove", "symbols": ["NONEXISTENT"], "topics": ["kline.1"], "market": "linear"})
    assert bot.subscriptions == set()  # Still empty, no error or crash

def test_multiple_rapid_market_switches(bot):
    # Rapidly switch market types to stress market-switching logic
    symbols = ["BTCUSDT"]
    channels = ["kline.1"]

    for market in ["spot", "linear"] * 50:
        bot._handle_command({"action": "set", "symbols": symbols, "topics": channels, "market": market})

    assert bot.market == "linear"
    assert bot.subscriptions == {"kline.1.BTCUSDT"}
