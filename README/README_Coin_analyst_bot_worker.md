# 🧠 Coin Analyst Bot — Vision & Architecture

## 🔹 Purpose
An autonomous research bot that evaluates coins, identifies opportunities, and generates reports for other bots in the system.

---

## ✅ Phase 1: Lite Scanning & Filtering

**Goal:**  
Cycle through all coins, apply filters, and generate a refined candidate list.

**Actions:**
- Pull coin lists.
- For each coin:
  - Fetch 24h volume and market cap (if available) from multiple exchanges using `ccxt`.
- Apply filters:
  - Exclude top 20 coins by volume or cap.
  - Remove illiquid or microcap tokens.
  - Retain only mid-tier coins with potential.

**Output:**
- Cleaned list of coin symbols.
- Publish list to:  
  `REDIS_CHANNEL_COIN_CANDIDATES`

---

## 🔎 Phase 2: Deep Analysis (On Demand)

**Triggered when a coin is sent for review.**

**Actions:**
- Pull historical Kline data:
  - Timeframes: `1h`, `15m`, `5m`
  - From multiple exchanges (if needed)
- Analyze:
  - Volatility, price movement
  - Volume surges, trend direction
  - Range vs breakout
- Create structured report with:
  - Coin name, trend, indicators
  - Confidence score
  - Flags: "low volume", "potential breakout", etc.

**Optional (Later):**
- Pull social sentiment data
- Add on-chain metrics (gas usage, holders, etc.)
- Alert system to re-check coins on certain triggers

**Publish to:**
- `REDIS_CHANNEL_COIN_REPORTS`

---

## 🗂 Storage

Store results in:
- `coin_analysis_reports` table  
With fields:
- `coin_symbol`
- `exchange`
- `time_analyzed`
- `indicators_json`
- `confidence_score`
- `tags`

---

## 🔁 Phase 3: Strategy Discovery Bot (Future)

**Triggered by promising coin reports.**

**Actions:**
- Pull extended historical data.
- Backtest using various strategies:
  - VWAP
  - EMA crossover
  - Scalping logic
  - Reversion models
- Log outcomes for strategy matching

---

## 📡 Redis Channels

| Channel                     | Purpose                         |
|----------------------------|----------------------------------|
| `COIN_ANALYSIS_QUEUE`      | Receive coins to analyze         |
| `COIN_CANDIDATES_PUB`      | Output from lite scanner         |
| `COIN_REPORTS_PUB`         | Final analyzed coin reports      |

---

## 🔐 Security Notes

- Uses read-only API keys (no trading access)
- Requires token-based auth to write to Redis or DB

---

## 🛠 Future Enhancements

- Parallel processing via threads or async
- Real-time sentiment scraping
- Dynamic alert system + dashboard visibility
