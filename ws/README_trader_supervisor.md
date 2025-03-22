# 🧠 Supervisor Bot — Quant Trading Platform

The **Supervisor Bot** manages a team of trader bots.  
It does **not** trade itself — instead, it **controls**, **monitors**, and **orchestrates** bot behavior.

---

## 📌 Core Responsibilities

- ✅ Register itself on startup (with auth token)
- ✅ Maintain a **heartbeat** in Redis for liveness monitoring
- ✅ Launch and manage a maximum number of **trader bots**
- ✅ Assign strategies and risk profiles per bot
- ✅ Listen to Redis channels for:
  - New coin assignments
  - Trader bot updates
  - Commands from a potential Manager Bot

---

## 🔁 Supervisor Lifecycle

1. 🔐 Authenticates and registers with the DB Bot  
2. ❤️ Sends regular heartbeats to `HEARTBEAT_CHANNEL`  
3. 📡 Subscribes to `SUPERVISOR_COMMAND_CHANNEL`  
4. ⚙️ Executes commands like:
   - `start_bot`
   - `stop_bot`
   - `resync_strategy`  
5. 🚀 Starts trader bots via subprocesses or threads  
6. 👀 Monitors active bots and logs feedback  

---

## 🧱 Requirements

- 🔑 Auth token stored in config
- 📖 Must be defined in the `bot_roles` table (e.g., `supervisor`)
- 📦 PostgreSQL permissions to read/write bot-related tables
- 🔄 Redis Subscriptions:
  - `SERVICE_STATUS_CHANNEL`
  - `HEARTBEAT_CHANNEL`
  - `SUPERVISOR_COMMAND_CHANNEL`

---

## 🔮 Future Ideas

- 🤝 Communicate with a higher-level **Manager Bot**
- 💰 Enforce budget limits from a `bot_funds` table
- ♻️ Restart bots if they crash
- 📊 Publish summary reports on bot performance

---

🧑‍💻 **Author:** Jericho Sharman  
📆 **Year:** 2025
