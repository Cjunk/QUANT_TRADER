import datetime, json, logging, time
from collections import defaultdict
from bots.websocket_bot.config_websocket_bot import REDIS_CHANNEL, ORDER_BOOK_DEPTH
from bots.config import config_redis as r_cfg
from bots.utils.logger import setup_logger

# Global trackers
_orderbooks          = defaultdict(lambda: {"bids": [], "asks": []})
_last_seq            = defaultdict(int)
_last_snapshot       = defaultdict(lambda: time.time())
_last_published_time = defaultdict(lambda: 0)

SNAPSHOT_REFRESH = 360           # seconds
ORDERBOOK_AGG_INTERVAL = 60      # seconds

def _extract_update_seq(msg: dict, symbol: str) -> int:
    try:
        return int(msg["data"]["u"])
    except (KeyError, TypeError, ValueError):
        logger = logging.getLogger("seq")
        logger.warning("⚠️ using ts as seq for %s – adjust extractor!", symbol)
        return int(msg.get("ts", 0))

def _handle_snapshot(sym: str, msg: dict):
    book = _orderbooks[sym]
    ob = msg["data"]
    book["bids"] = ob["b"]
    book["asks"] = ob["a"]
    _last_seq[sym] = _extract_update_seq(msg, sym)
    _last_snapshot[sym] = time.time()

def _apply_delta(sym: str, msg: dict):
    book = _orderbooks[sym]
    bids, asks = book["bids"], book["asks"]

    for p, q in msg["data"]["b"]:
        if float(q) == 0:
            bids[:] = [lvl for lvl in bids if lvl[0] != p]
        else:
            for i, lvl in enumerate(bids):
                if lvl[0] == p:
                    bids[i] = [p, q]; break
                if float(p) > float(lvl[0]):
                    bids.insert(i, [p, q]); break
            else:
                bids.append([p, q])

    for p, q in msg["data"]["a"]:
        if float(q) == 0:
            asks[:] = [lvl for lvl in asks if lvl[0] != p]
        else:
            for i, lvl in enumerate(asks):
                if lvl[0] == p:
                    asks[i] = [p, q]; break
                if float(p) < float(lvl[0]):
                    asks.insert(i, [p, q]); break
            else:
                asks.append([p, q])

class MessageRouter:
    def __init__(self, redis_client, market):
        self.redis = redis_client
        self.market = market  # 'linear' or 'spot'
        self.logger = setup_logger("router.py", logging.DEBUG)

    def trade(self, data):
        trade = data["data"][-1]
        trade_data = {
            "symbol": trade["s"],
            "price": float(trade["p"]),
            "volume": float(trade["v"]),
            "side": trade["S"],
            "trade_time": datetime.datetime.utcfromtimestamp(trade["T"]/1000).isoformat()
        }
        channel = REDIS_CHANNEL[f"{self.market}.trade_out"]
        self.redis.publish(channel, json.dumps(trade_data))


    def kline(self, msg: dict):
        try:
            p_topic = msg["topic"].split(".")
            interval, sym = p_topic[1], p_topic[2]
            c = msg["data"][0]
            if not c.get("confirm"):
                return
            out = {
                "symbol": sym,
                "interval": interval,
                "start_time": datetime.datetime.utcfromtimestamp(c["start"] / 1000).isoformat(),
                "open": c["open"],
                "close": c["close"],
                "high": c["high"],
                "low": c["low"],
                "volume": c["volume"],
                "turnover": c["turnover"],
                "confirmed": True
            }
            channel = REDIS_CHANNEL[f"{self.market}.kline_out"]
            self.redis.publish(REDIS_CHANNEL[f"{self.market}.kline_out"], json.dumps(out))
            self.logger.debug(f"KLINE → {sym} {interval}")
        except Exception as exc:
            self.logger.error(f"kline() parse error: {exc}  RAW={msg}")

    def orderbook(self, raw: dict):
        try:
            sym = raw["data"]["s"]
            typ = raw["type"]
            seq = _extract_update_seq(raw, sym)

            # Handle snapshot
            if typ == "snapshot":
                _handle_snapshot(sym, raw)
                self.logger.debug("📸 SNAPSHOT %s seq=%s", sym, seq)
                _last_seq[sym] = seq

            # Handle delta
            elif typ == "delta":
                if _last_seq[sym] == 0:
                    self.logger.debug("🔄 Δ before snapshot for %s – requesting snapshot", sym)
                    return

                if seq > _last_seq[sym] + 1:
                    self.logger.warning("⚠️ SEQ GAP %s last=%s new=%s – requesting resync", sym, _last_seq[sym], seq)
                    return

                _apply_delta(sym, raw)
                _last_seq[sym] = seq
                _last_snapshot[sym] = time.time()

            # Force snapshot refresh periodically
            if time.time() - _last_snapshot[sym] > SNAPSHOT_REFRESH:
                self.logger.debug("🔃 Forcing snapshot refresh for %s", sym)
                _last_snapshot[sym] = time.time()

            # Aggregated publishing interval
            now = time.time()
            if now - _last_published_time[sym] >= ORDERBOOK_AGG_INTERVAL:
                bids = _orderbooks[sym]["bids"][:ORDER_BOOK_DEPTH]
                asks = _orderbooks[sym]["asks"][:ORDER_BOOK_DEPTH]

                aggregated_payload = {
                    "symbol": sym,
                    "best_bid": bids[0] if bids else None,
                    "best_ask": asks[0] if asks else None,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": datetime.datetime.utcnow().isoformat()
                }

                self.redis.publish(REDIS_CHANNEL[f"{self.market}.orderbook_out"], json.dumps(aggregated_payload))
                _last_published_time[sym] = now
                self.logger.debug("✅ Aggregated orderbook published for %s", sym)

        except Exception as exc:
            snippet = str(raw)[:120]
            self.logger.error("OB-parse error: %s raw:%s…", exc, snippet)




