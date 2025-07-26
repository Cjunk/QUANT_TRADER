# 📡 WebSocket Bot

## 📝 Overview

Handles real-time WebSocket connections to Bybit for three distinct markets:

* **Spot**
* **Linear (Futures)**
* **Macro** (BTC/ETH focus)

Publishes live trade, candlestick (kline), and order book delta data into Redis. Each market runs in its own thread.

---

## 🚀 Entry Point

**File:** `main.py`

Launches:

* `WebSocketBot("spot")`
* `WebSocketBot("linear")`
* `WebSocketBot("macro")`

---

## 🧵 Redis Channels Used

### 📥 Subscription Channels

| Channel Name                       | Purpose                                |
| ---------------------------------- | -------------------------------------- |
| `SPOT_SUBSCRIPTION_CHANNEL`        | Receives symbol/topic updates for spot |
| `LINEAR_SUBSCRIPTION_CHANNEL`      | Receives updates for linear (futures)  |
| `DERIVATIVES_SUBSCRIPTION_CHANNEL` | Receives derivitives stuff         |

### 📡 Sync & Status Channels

| Redis Key                                   | Description                           |
| ------------------------------------------- | ------------------------------------- |
| `REDIS_SUBSCRIPTION_KEY:<market>`           | Saves current subscriptions           |
| `SERVICE_STATUS_CHANNEL`                    | Publishes start/stop/heartbeat events |
| `DB_REQUEST_SUBSCRIPTIONS`                  | Requests active symbols from DB bot   |
| `DB_SAVE_SUBSCRIPTIONS`                     | Publishes active symbols to DB bot    |
| `REDIS_CHANNEL["<market>.orderbook_delta"]` | Order book delta publishing           |

---

## 🧪 Redis Command Format

### ➕ Add Coins

```json
{
  "action": "add",
  "market": "spot",
  "symbols": ["BTCUSDT"],
  "topics": ["trade", "orderbook.50"]
}
```

### ❌ Remove Coins

```json
{
  "action": "remove",
  "market": "linear",
  "symbols": ["ETHUSDT"],
  "topics": ["trade"]
}
```

### 🔄 Replace Entire Coin List

```json
{
  "action": "set",
  "market": "macro",
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "topics": ["kline.1", "publicTrade"]
}
```

---

## 🛠 How to Run

```bash
python main.py
```

* Make sure Redis is running
* Uses `.env` for Redis and Discord credentials

---

## 🔍 Dev Notes

* Each bot is named as `BOT_NAME:<market>`
* Heartbeats auto-publish status every 30s
* Redis stores subscriptions for restart recovery
* Order book deltas routed to `orderbook_delta:<market>`

---

## ✅ Use Case

This README is meant to guide **development and testing**:

* Shows expected Redis structure
* Documents channels and formats
* Prepares for integration with strategy or analytics layers

---
