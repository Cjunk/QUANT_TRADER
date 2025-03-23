# 🧿 Bybit Coin Monitor Bot

## 📌 Purpose

A background service that continuously monitors Bybit for newly listed **Spot** and **USDT Perpetual (Linear)** trading pairs.  
It keeps an internal copy of all known symbols and identifies **newly listed tokens** by comparing with the live exchange feed.  
Discovered coins are published via Redis and optionally logged into the database for historical tracking and downstream processing.

---

## 🛠️ Tech Stack

| Component        | Technology                      |
|------------------|----------------------------------|
| Language         | Python 3.10+                    |
| Exchange Access  | [ccxt](https://github.com/ccxt/ccxt) (Bybit wrapper) |
| Redis            | Pub/Sub via `redis-py`          |
| Database         | PostgreSQL (restricted access role) |
| Auth             | Token-based bot authentication  |
| Logging          | `logging` + rotating log files  |
| Template Base    | Standardized Bot Template (PID, heartbeat, lifecycle) |

---

## 🔁 Operational Flow

1. **On Startup:**
   - Connect to database.
   - Load all known Spot & Linear trading pairs into memory.
   - If table is empty (first run), initialize without triggering “new” coin logic.

2. **At Interval (e.g., every 5 minutes):**
   - Fetch current list of **Spot** and **USDT Perp** tickers from Bybit via `ccxt`.
   - Compare against in-memory set.
   - If a symbol is not recognized:
     - Add it to a "new_discoveries" buffer (in-memory).
     - Publish discovery to Redis channel.
     - Mark for batch-insert into DB on next write cycle.

3. **Batch Sync (e.g., every 10–15 minutes):**
   - Insert all new discovered coins into the `known_coins` table.

4. **Heartbeat:**
   - Publishes a regular health check to a Redis heartbeat channel every 30 seconds.

---

## 🧬 Redis Channels

| Channel Name              | Payload Description                   |
|---------------------------|----------------------------------------|
| `NEW_COIN_DISCOVERED`     | Publishes each new coin found `{ "symbol": "XYZ/USDT", "market_type": "spot" }` |
| `HEARTBEAT_CHANNEL`       | `{ "bot_name": "coin_monitor", "heartbeat": true, "time": timestamp }` |

---

## 🗃️ Database Schema (Suggested)

### Table: `known_coins`

| Field           | Type        | Description                         |
|----------------|-------------|-------------------------------------|
| `id`           | SERIAL PK   | Unique row ID                       |
| `symbol`       | TEXT        | Symbol in `XYZ/USDT` format         |
| `market_type`  | TEXT        | `spot` or `linear`                  |
| `discovered_at`| TIMESTAMP   | Time the symbol was discovered      |

- Add an index on `(symbol, market_type)` to speed up lookups.

---

## 🔐 Security & Permissions

- Bot uses a dedicated DB role (`coin_monitor_user`) with:
  - ✅ `SELECT`, `INSERT` rights on `known_coins`
  - ❌ No access to other tables or schemas

---

## 🔮 Future Enhancements

- Add support for:
  - ✅ Inverse contracts
  - ✅ Option markets
- Detect delisted or disabled pairs
- Integrate external alerting (e.g., Discord, Email)
- Save coin metadata (e.g., quote/ base, leverage settings, risk limits)
- Trigger webhook or Redis signal to notify strategy bots of eligible new assets
- Integrate with coin analytics bot for immediate analysis upon discovery
- Web dashboard visibility (via your PHP dashboard project)

---

## 🚀 Summary

The Coin Monitor Bot acts as the **entry point into the trading ecosystem**, ensuring all newly listed tokens are **immediately recognized and evaluated**.  
By keeping the symbol dataset accurate and live, it lays the foundation for early opportunity detection, rapid backtesting, and strategy assignment.
