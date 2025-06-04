import datetime, json, logging, time
from collections import defaultdict

from config import config_redis as r_cfg
from config_websocket_bot import ORDER_BOOK_DEPTH
from utils.logger import setup_logger

# ==== Jericho: Configurable Constants ====
SNAPSHOT_REFRESH = 360           # seconds
ORDERBOOK_AGG_INTERVAL = 60      # seconds

class MessageRouter:
    """
    Jericho: Professional, robust, and clear message router for WebSocket trading bots.
    Handles trade, kline, and orderbook messages, with all state and logic encapsulated.
    """
    def __init__(self, redis_client, market):
        # ==== Jericho: Core State ====
        log_level = logging.DEBUG if getattr(r_cfg, "LOG_LEVEL", "INFO").upper() == "DEBUG" else logging.INFO
        self.redis = redis_client
        self.market = market  # 'linear' or 'spot'
        # Log file per market, not .py file
        self.logger = setup_logger(f"router_{market}.log", log_level)
        self._orderbooks = defaultdict(lambda: {"bids": [], "asks": []})
        self._last_seq = defaultdict(int)
        self._last_snapshot = defaultdict(lambda: time.time())
        self._last_published_time = defaultdict(lambda: 0)

    # ==== Jericho: Trade Message Handler ====
    def trade(self, data):
        try:
            trade = data["data"][-1]
            trade_data = {
                "symbol": trade["s"],
                "price": float(trade["p"]),
                "volume": float(trade["v"]),
                "side": trade["S"],
                "trade_time": datetime.datetime.utcfromtimestamp(trade["T"]/1000).isoformat()
            }
            channel = r_cfg.REDIS_CHANNEL[f"{self.market}.trade_out"]
            #self.redis.publish(channel, json.dumps(trade_data))
            #self.logger.debug(f"Published trade: {trade_data}")
        except Exception as exc:
            self.logger.error(f"trade() parse error: {exc}  RAW={data}")

    # ==== Jericho: Kline Message Handler ====
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
            channel = r_cfg.REDIS_CHANNEL[f"{self.market}.kline_out"]
            self.redis.publish(channel, json.dumps(out))
            self.logger.info(f"KLINE {interval} → {sym} {interval}")
            self.logger.debug(f"Published kline: {out}")
        except Exception as exc:
            self.logger.error(f"kline() parse error: {exc}  RAW={msg}")

    # ==== Jericho: Orderbook Message Handler ====
    def orderbook(self, raw: dict):
        try:
            sym = raw["data"]["s"]
            typ = raw["type"]
            seq = self._extract_update_seq(raw, sym)

            if typ == "snapshot":
                self._handle_snapshot(sym, raw)
                self.logger.debug("📸 SNAPSHOT %s seq=%s", sym, seq)
                self._last_seq[sym] = seq
            elif typ == "delta":
                if self._last_seq[sym] == 0:
                    self.logger.debug("🔄 Δ before snapshot for %s – requesting snapshot", sym)
                    return
                if seq > self._last_seq[sym] + 1:
                    self.logger.warning("⚠️ SEQ GAP %s last=%s new=%s – requesting resync", sym, self._last_seq[sym], seq)
                    return
                self._apply_delta(sym, raw)
                self._last_seq[sym] = seq
                self._last_snapshot[sym] = time.time()

            # Periodic snapshot refresh
            if time.time() - self._last_snapshot[sym] > SNAPSHOT_REFRESH:
                self.logger.debug("🔃 Forcing snapshot refresh for %s", sym)
                self._last_snapshot[sym] = time.time()

            # Aggregated publishing interval
            now = time.time()
            if now - self._last_published_time[sym] >= ORDERBOOK_AGG_INTERVAL:
                bids = self._orderbooks[sym]["bids"][:ORDER_BOOK_DEPTH]
                asks = self._orderbooks[sym]["asks"][:ORDER_BOOK_DEPTH]
                aggregated_payload = {
                    "symbol": sym,
                    "best_bid": bids[0] if bids else None,
                    "best_ask": asks[0] if asks else None,
                    "bids": bids,
                    "asks": asks,
                    "timestamp": datetime.datetime.utcnow().isoformat()
                }
                self.redis.publish(r_cfg.REDIS_CHANNEL[f"{self.market}.orderbook_out"], json.dumps(aggregated_payload))
                self._last_published_time[sym] = now
                #self.logger.debug(f"Published orderbook: {aggregated_payload}")
        except Exception as exc:
            snippet = str(raw)[:120]
            self.logger.error("OB-parse error: %s raw:%s…", exc, snippet)

    # ==== Jericho: Sequence Reset ====
    def reset_seq(self, symbol):
        self._last_seq[symbol] = 0
        self.logger.debug("🔄 Reset sequence for symbol: %s", symbol)

    # ==== Jericho: Internal Helpers ====
    def _extract_update_seq(self, msg: dict, symbol: str) -> int:
        try:
            return int(msg["data"]["u"])
        except (KeyError, TypeError, ValueError):
            logger = logging.getLogger("seq")
            logger.warning("⚠️ using ts as seq for %s – adjust extractor!", symbol)
            return int(msg.get("ts", 0))

    def _handle_snapshot(self, sym: str, msg: dict):
        book = self._orderbooks[sym]
        ob = msg["data"]
        book["bids"] = ob["b"]
        book["asks"] = ob["a"]
        self._last_seq[sym] = self._extract_update_seq(msg, sym)
        self._last_snapshot[sym] = time.time()

    def _apply_delta(self, sym: str, msg: dict):
        book = self._orderbooks[sym]
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



