import datetime, json, logging,time
from collections import defaultdict
from websocket_bot.config_websocket_bot import REDIS_CHANNEL, ORDER_BOOK_DEPTH
from config import config_redis as r_cfg
from utils.logger import setup_logger          # <- your file
_orderbooks      = defaultdict(lambda: {"bids": [], "asks": []})
_last_seq        = defaultdict(int)
_last_snapshot   = defaultdict(float)
SNAPSHOT_REFRESH = 360          # seconds
def _extract_update_seq(msg: dict, symbol: str) -> int:
    """
    Bybit sends:
      • snapshot:  {'type':'snapshot', 'data':{..., 'u':123456}}
      • delta   :  {'type':'delta',    'data':{..., 'u':123457}}
    'u' is the LAST update-id applied to that payload.
    """
    try:
        return int(msg["data"]["u"])              # works for both spot & linear
    except (KeyError, TypeError, ValueError):
        # Fallback to old behaviour (timestamp) but WARN once
        logger = logging.getLogger("seq")
        logger.warning("⚠️  using ts as seq for %s – adjust extractor!", symbol)
        return int(msg.get("ts", 0))
def _handle_snapshot(sym: str, msg: dict):
    book = _orderbooks[sym]
    ob   = msg["data"]
    book["bids"] = ob["b"]            # already price-DESC order
    book["asks"] = ob["a"]            # already price-ASC  order
    _last_seq[sym]      = _extract_update_seq(msg, sym)
    _last_snapshot[sym] = time.time()
def _apply_delta(sym: str, msg: dict):
    book = _orderbooks[sym]
    bids, asks = book["bids"], book["asks"]

    for p, q in msg["data"]["b"]:                          # ------ bids
        if float(q) == 0:
            bids[:] = [lvl for lvl in bids if lvl[0] != p]
        else:
            # replace if exists, else insert in sorted position
            for i, lvl in enumerate(bids):
                if lvl[0] == p:
                    bids[i] = [p, q]; break
                if float(p) > float(lvl[0]):                # higher price → earlier
                    bids.insert(i, [p, q]); break
            else:
                bids.append([p, q])

    for p, q in msg["data"]["a"]:                          # ------ asks
        if float(q) == 0:
            asks[:] = [lvl for lvl in asks if lvl[0] != p]
        else:
            for i, lvl in enumerate(asks):
                if lvl[0] == p:
                    asks[i] = [p, q]; break
                if float(p) < float(lvl[0]):                # lower price → earlier
                    asks.insert(i, [p, q]); break
            else:
                asks.append([p, q])

class MessageRouter:
    """
    Stateless helpers that publish processed items to Redis.
    """
    def __init__(self, redis_client):
        self.redis = redis_client
        self.logger = setup_logger("router.py", logging.DEBUG)

    # one example – the other processors stay identical
    def trade(self, data):
        trade = data["data"][-1]
        trade_data = {
            "symbol"     : trade["s"],
            "price"      : float(trade["p"]),
            "volume"     : float(trade["v"]),
            "side"       : trade["S"],
            "trade_time" : datetime.datetime.utcfromtimestamp(trade["T"]/1000).isoformat()
        }
        self.redis.publish(REDIS_CHANNEL["trade_out"], json.dumps(trade_data))
    def kline(self, msg: dict):
            try:
                p_topic      = msg["topic"].split(".")           # kline.<intvl>.<sym>
                interval, sym = p_topic[1], p_topic[2]

                c = msg["data"][0]
                if not c.get("confirm"):
                    return                                       # skip in-flight bar

                out = {
                    "symbol"    : sym,
                    "interval"  : interval,
                    "start_time": datetime.datetime.utcfromtimestamp(
                                    c["start"] / 1000).isoformat(),
                    "open"      : c["open"],
                    "close"     : c["close"],
                    "high"      : c["high"],
                    "low"       : c["low"],
                    "volume"    : c["volume"],
                    "turnover"  : c["turnover"],
                    "confirmed" : True
                }
                self.redis.publish(REDIS_CHANNEL["kline_out"], json.dumps(out))
                self.logger.debug(f"KLINE → {sym} {interval}")
            except Exception as exc:
                self.logger.error(f"kline() parse error: {exc}  RAW={msg}")

    # ------------------------------------------------------------ ORDERBOOK
    def orderbook(self, raw: dict):
        """
        Normalises Bybit order-book flow and publishes a lean
        {symbol,bids,asks} packet expected by the DB bot.
        Implements:
          • snapshot overwrite
          • delta apply with sequence-gap detection
          • periodic forced re-snapshot
        """
        try:
            sym   = raw["data"]["s"]
            typ   = raw["type"]               # 'snapshot' or 'delta'
            seq   = _extract_update_seq(raw, sym)

            # ---------- 1) snapshot ---------------------------------
            if typ == "snapshot":
                _handle_snapshot(sym, raw)
                self.logger.debug("SNAPSHOT %s seq=%s", sym, seq)
                _last_seq[sym] = seq          #  <-- you need this line

            # ---------- 2) delta ------------------------------------
            elif typ == "delta":
                # first time? -> ignore delta & fetch snapshot
                if _last_seq[sym] == 0:
                    self.logger.debug("Δ before snapshot for %s – requesting snapshot", sym)
                    return

                # gap detection
                if seq > _last_seq[sym] + 1:
                    self.logger.warning("SEQ GAP %s  last=%s  new=%s – requesting resync",
                                        sym, _last_seq[sym], seq)
                    # Tell the WS-core to call rest-endpoint for fresh snapshot
                    return

                _apply_delta(sym, raw)
                _last_seq[sym] = seq
                _last_snapshot[sym] = time.time()     # ← reset timer on every good delta

            # ---------- 3) periodic resync --------------------------
            if time.time() - _last_snapshot[sym] > SNAPSHOT_REFRESH:
                self.logger.debug("Forcing snapshot refresh for %s", sym)
                # Let ws_core fetch a fresh snapshot (depends on your design)
                _last_snapshot[sym] = time.time()

            # ---------- 4) publish lean book ------------------------
            payload = {
                "symbol": sym,
                "bids" : _orderbooks[sym]["bids"][:50],   # trim if you like
                "asks" : _orderbooks[sym]["asks"][:50]
            }
            self.redis.publish(r_cfg.ORDER_BOOK_UPDATES, json.dumps(payload))

        except Exception as exc:
            snippet = str(raw)[:120]
            self.logger.error("OB-parse error: %s  raw:%s…", exc, snippet)

    # ------------------------------------------------------------ HELPERS



