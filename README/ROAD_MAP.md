# 📈 Quant Trading Platform — Phase 2 Roadmap

This document outlines the key next steps in the evolution of Jericho's institutional-grade quant trading platform.

---

## ✅ 1. Finalize Bot Infrastructure

- [ ] **Trade Bot Template** - Build a clean, base `TradeBot` class with plug-in strategies.
  - [ ] Base class handles init, DB, Redis, and lifecycle.
  - [ ] Inherit for strategy-specific logic (VWAPBot, ScalperBot).

- [ ] **Strategy Slotting** - Insert strategies like VWAP, EMA cross, mean reversion, etc.
  - [ ] VWAP-based entry signals
  - [ ] EMA crossover logic
  - [ ] RSI filters and thresholds

- [ ] **Historical Replay / Backtesting** - Use stored kline data to simulate trades.
  - [ ] Read from `kline_data` table
  - [ ] Simulate candles in batches
  - [ ] Log hypothetical orders

- [ ] **Order Execution Mocking** - Log simulated trades (paper trade mode) before live trading.
  - [ ] Store trades in a separate table (`simulated_trades`)
  - [ ] Track virtual PnL

- [ ] **Redis Trade Signal Queue** - Publish trade signals to a Redis channel for future use.
  - [ ] `TRADE_SIGNALS_CHANNEL` with JSON payloads
  - [ ] Include symbol, action, confidence score

---

## ✅ 2. Risk & Position Management

- [ ] **Bot Budget Allocation System** - Assign and track available capital per bot (stored in DB).
  - [ ] Table: `bot_funds` with bot_id, allocation, used, etc.

- [ ] **Risk Profiles per Coin or Bot** - Classify as `low`, `normal`, `high`, `do_not_trade`.
  - [ ] Stored in `current_coins` or `bot_profiles`

- [ ] **PnL Simulation Module** - Track unrealized/realized gains per bot over time.
  - [ ] Use separate table `bot_pnl`
  - [ ] Use mock trade entries from paper mode

- [ ] **Dynamic Bot Disabling** - Auto-disable bots exceeding loss/error thresholds.
  - [ ] Monitor drawdowns in `bot_pnl`
  - [ ] Disable via update in `bots` table

---

## ✅ 3. Admin Tools / Dashboard

- [ ] **PHP or JavaScript Web Dashboard** - Display active bots, trade logs, coin tracking, PnL, etc.
  - [ ] PHP backend or Node.js
  - [ ] Table views + summary stats

- [ ] **Live Charts Integration** - Pull historical kline + trades from DB to chart candles & signals.
  - [ ] Use Chart.js or TradingView widgets

- [ ] **Login Auth System** - Protect web interface with admin-only access.
  - [ ] Basic login + hashed password file
  - [ ] Session-based control

---

## ✅ 4. Data Layer Enhancements

- [ ] **Data Archival & Cleanup** - Archive old candles, orderbooks, trades.
  - [ ] Move old rows to `*_archive` tables

- [ ] **Indicator Caching** - Store precomputed indicators to reduce CPU load.
  - [ ] Add columns to `kline_data`
  - [ ] Recompute only on insert/update

- [ ] **Action Logging System** - Log bot events, trades, updates, and anomalies.
  - [ ] New `system_logs` table

---

## ✅ 5. Deployment & Scaling

- [ ] **Move PostgreSQL to Secondary PC** - Keep database separate from live bots.
  - [ ] Update DB_HOST in config

- [ ] **Secure DB & Redis Access** - Restrict connections to trusted IPs or VPN-only.
  - [ ] Use firewall and `pg_hba.conf`

- [ ] **(Optional) Dockerize Services** - If needed for deployment, use Docker for modular isolation.
  - [ ] Create Dockerfiles for each service
  - [ ] Use `docker-compose` for orchestration

---

## 🔚 Future Ideas

- [ ] Cloud alerts or email triggers for critical system events
- [ ] Real capital trading + API integration (Bybit live API)
- [ ] Strategy optimizer & performance analyzer

---

**Built by:** Jericho Sharman  
**Year:** 2025

