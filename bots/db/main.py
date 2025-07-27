# --- main.py ---
import sys, os, signal
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from db_core import PostgresDBBot
from fastapi import FastAPI
import uvicorn
from threading import Thread

# FastAPI app
app = FastAPI()

# Instantiate bot
bot = PostgresDBBot("DB_BOT.log")

# Start bot in background thread
@app.on_event("startup")
def start_bot():
    Thread(target=bot.run, daemon=True).start()

# Simple API endpoint
@app.get("/status")
def get_status():
    return {
        "bot_name": bot.status["bot_name"],
        "running": bot.running,
        "pid": bot.status["metadata"].get("pid"),
        "strategy": bot.status["metadata"].get("strategy"),
    }
@app.get("/subscriptions/linear")
def get_linear_subscriptions():
    return bot.get_websocket_subscriptions("linear")

@app.get("/subscriptions/spot")
def get_spot_subscriptions():
    return bot.get_websocket_subscriptions("spot")
@app.get("/subscriptions/protected")
def get_protected_subs():
    query = """
        SELECT DISTINCT symbol, market
        FROM trading.websocket_subscriptions
        WHERE owner = 'protected'
    """
    results = bot.query(query)
    return [{"symbol": row["symbol"], "market": row["market"]} for row in results]

# Launch FastAPI server
if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    uvicorn.run(app, host="0.0.0.0", port=8001)


