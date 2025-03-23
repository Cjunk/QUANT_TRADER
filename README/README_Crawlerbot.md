# 🤖 Crawler Bot Vision — Quant Platform Component

## 🧠 Purpose:
The Crawler Bot scans the Bybit exchange for promising trading opportunities by cycling through multiple coins, analyzing technicals, and flagging strong setups for further evaluation.

---

## 🧩 Core Responsibilities:

- ✅ Retrieve a complete list of all Bybit tradeable symbols
- 🔽 Rank coins based on:
  - 24h Volume
  - Open Interest (OI)
  - Volatility (e.g. ATR or % range)
- 🚫 Filter out top N coins (e.g., top 10) to avoid high-volume institutional noise
- 🔁 Loop through remaining coins one by one (or via threading)

---

## 🔍 Per-Coin Analysis:

For each eligible coin:

1. 📦 **Download Candle Data**
   - 5-minute candles
   - 1-hour candles

2. 📈 **Perform Analysis**
   - RSI, MACD, Moving Averages
   - Bollinger Bands, Volume Spikes
   - Trend strength and direction
   - Historical breakout zones
   - (Optional later: sentiment from Reddit, Twitter, etc.)

3. 🧼 **Clean up**
   - Drop or archive temporary candle data after processing

4. 🏁 **Decision**
   - If the coin meets criteria, flag it as `✅ FLAGGED`
   - Store in `flagged_coins` table with timestamp + reasons

---

## 🗃️ Database Schema Suggestions:

- **Table:** `flagged_coins`
  - `id`, `symbol`, `flag_reason`, `timestamp`, `confidence_score`, `status`

- **Table:** `crawler_logs`
  - Every coin's outcome (flagged or not), metrics, and analysis time

---

## ⚙️ Configuration:

- ⏱️ Run every X minutes/hours (configurable)
- 🔧 Volume threshold, score thresholds, etc. stored in config
- 🧵 Multithreading optional (depends on performance impact)

---

## 🧠 Future Integrations:

- 📡 Send flagged coin alerts to `manager_bot` via Redis
- 🧠 Save full per-coin snapshots for ML training
- 📈 Use flags to generate passive weekly strategy reports

---

**Status:** *Not yet implemented*  
**Author:** Jericho Sharman  
**Target Host:** New Analysis Machine  
**Phase:** Post-supervisor rollout  
