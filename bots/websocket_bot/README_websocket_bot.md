# 📡 WebSocket Bot – Real-Time Market Data Collector

## Overview

This bot subscribes to **Bybit's public WebSocket API** and ingests real-time market data:
- Trades
- Candlesticks (klines)
- Order Book snapshots and deltas

It publishes clean, structured data into **Redis**, where downstream bots (preprocessor, database, analytics) can consume it. Designed for **modular quant systems** and scalable pipelines.

---

## 📁 Project Structure

```bash
bots/
├── websocket/              # ⬅ Runner folder
│   └── runner.py           # Launches the WebSocketBot
├── websocket_bot/          # Core logic
│   ├── core.py             # Main bot logic
│   ├── utils.py            # Shared utilities (logging, webhook, parsing)
│   └── __init__.py
config/
├── config_ws.py            # WebSocket settings (URL, keys, etc.)
├── config_redis.py         # Redis settings and channels
```

---

## 🚀 Runtime Behavior

### On Startup
- Logs a `started` message via Discord webhook
- Subscribes to Redis pubsub channels for:
  - Coin feed
  - Live updates
- Sends an authenticated **heartbeat every X seconds**
- Requests the current coin list from the PostgreSQL bot (PB)

### Subscriptions
- Connects to Bybit via WebSocket
- Subscribes in batches to:
  - Trades (e.g., `publicTrade.BTCUSDT`)
  - Klines (1m, 5m, 1h, daily)
  - Order books (depth 200)

### Message Handling
- Parses each message by topic:
  - `publicTrade` → recent trades
  - `kline.X.symbol` → confirmed candle
  - `orderbook.200.symbol` → snapshot or delta
- Sends parsed data to Redis

### Heartbeat
- Sent every `HEARTBEAT_INTERVAL` seconds
- Used for presence tracking and bot liveness
- Format includes:
  - `bot_name`
  - `auth_token`
  - `timestamp`

---

## 🛠 Configuration

Set in `config/config_ws.py` and `config/config_redis.py`.

### `config_ws.py`
| Variable             | Description                                |
|----------------------|--------------------------------------------|
| `BOT_NAME`           | Unique bot name identifier                 |
| `BOT_AUTH_TOKEN`     | Used for bot authentication                |
| `SPOT_WEBSOCKET_URL` | Bybit WebSocket endpoint                   |
| `LOG_FILENAME`       | Log file path                              |
| `LOG_LEVEL`          | Logging verbosity (e.g., `INFO`, `DEBUG`)  |
| `HEARTBEAT_INTERVAL` | Seconds between each heartbeat             |
| `SUBSCRIPTIONS`      | Template for WebSocket channel generation  |
| `WEBHOOK`            | Discord webhook for alerts                 |
| `WEBSOCKET_USERID`   | Discord user ID for pings                  |

---

### `config_redis.py`
| Variable                 | Description                                      |
|--------------------------|--------------------------------------------------|
| `REDIS_HOST`             | Redis server IP                                 |
| `REDIS_PORT`             | Redis server port                               |
| `REDIS_DB`               | Redis DB number (0–15)                          |
| `COIN_CHANNEL`           | Channel for manual coin updates                 |
| `COIN_FEED_AUTO`         | Channel for automated/smart bot updates         |
| `SERVICE_STATUS_CHANNEL` | Deprecated (heartbeat used instead)            |
| `HEARTBEAT_CHANNEL`      | Channel for heartbeat pings                     |
| `RESYNC_CHANNEL`         | Where to request a new coin list from PB bot   |

---

## 🔁 Redis Pub/Sub Channels (Used)

- `COIN_CHANNEL` → listens for dev updates to coin list
- `COIN_FEED_AUTO` → listens for smart bot updates
- `RESYNC_CHANNEL` → sends startup coin list requests
- `HEARTBEAT_CHANNEL` → sends heartbeat every X seconds

---

## 📤 Redis Keys (Published)

- `trades:{symbol}` → list of recent trades (LPUSH, capped to 1000)
- `latest_trade:{symbol}` → most recent price (SET)
- `orderbook:{symbol}:bids` → Redis sorted set (ZADD by price)
- `orderbook:{symbol}:asks` → Redis sorted set (ZADD by price)

---

## 🧪 Development

### To Run:
```bash
cd bots/websocket
python runner.py
```

### Dev Notes:
- Must run inside virtual environment (`venv`)
- Will fail if `BOT_AUTH_TOKEN` or `SPOT_WEBSOCKET_URL` is missing
- Reconnection handled via `WebSocketApp.run_forever(ping_interval=30)`
- Trade/orderbook parsing to be implemented in later stages

---

## 🧼 Logging

Logfile is controlled via `LOG_FILENAME` and `LOG_LEVEL`.  
Rotating file logging or console output can be added later.

---

## 🧱 Design Principles

- Redis for real-time IPC (scalable, resilient)
- Webhook for external visibility (DevOps or Discord alerts)
- Heartbeats for passive health monitoring (avoids forced registration)
- Decoupled from DB logic (dedicated DB bot handles storage)
- Code is modular and ready for test mocking

---

## 📌 TODOs

- [ ] Implement trade message parsing
- [ ] Implement kline message parsing
- [ ] Implement order book snapshot + delta logic
- [ ] Add retry/backoff logic on WS error
- [ ] Publish metrics to Redis (subscriptions, reconnects)
- [ ] Add memory/cpu usage to heartbeat payload

---

## 👨‍💻 Maintainer Notes

- This bot **must always run in the background** before any strategy or analytics bots.
- If restarted, it will re-request the coin list and reconnect automatically.
- Bot must be aware of downtime recovery (coinlist sync retries are built-in).

---