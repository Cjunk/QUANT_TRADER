from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from db_core import PostgresDBBot

app = FastAPI()
db_bot_instance = PostgresDBBot()

@app.on_event("startup")
def startup_event():
    import threading
    threading.Thread(target=db_bot_instance.run, daemon=True).start()

@app.get("/status")
def get_status():
    return {
        "bot_name": db_bot_instance.status["bot_name"],
        "running": db_bot_instance.running,
        "pid": db_bot_instance.status["metadata"].get("pid"),
        "strategy": db_bot_instance.status["metadata"].get("strategy"),
    }

@app.get("/subscriptions")
def get_subscriptions(market: str = Query(..., description="Market type, e.g. 'spot' or 'linear'")):
    subs = db_bot_instance.get_websocket_subscriptions(market)
    return {"market": market, "subscriptions": subs}

@app.post("/subscriptions")
def set_subscriptions(
    market: str,
    symbols: list[str],
    topics: list[str],
    owner: str
):
    db_bot_instance.set_websocket_subscriptions(market, symbols, topics, owner)
    return JSONResponse({"result": "subscriptions updated"})

@app.post("/stop")
def stop_bot():
    db_bot_instance.stop()
    return {"result": "bot stopped"}
