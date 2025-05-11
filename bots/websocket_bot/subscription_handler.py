# subscription_handler.py
import json, threading, time, logging
from utils.redis_client import get_redis
from utils.logger       import setup_logger

# … imports …
class SubscriptionHandler(threading.Thread):
    LIST_KEY = "current_coins"

    def __init__(self, redis_conn, out_q, log_level=logging.INFO):
        super().__init__(daemon=True)
        self.redis   = redis_conn
        self.out_q   = out_q
        self.logger  = setup_logger("subscription_handler", log_level)

    # ---------------------------------------------------------------- run
    def run(self):
        self.logger.info(f"Listening on Redis list '{self.LIST_KEY}' …")
        while True:
            _key, raw = self.redis.blpop(self.LIST_KEY)          # blocks
            self.logger.debug(f"[blpop] RAW:{raw[:80]}  list_len={self.redis.llen(self.LIST_KEY)}")

            try:
                cmd = json.loads(raw)
                self._normalise(cmd)
                self.out_q.put(cmd)
                self.logger.debug("put() into out_q ✅")
            except Exception as exc:
                self.logger.error(f"Bad command: {exc}  RAW:{raw}")

    # ---------------------------------------------------------------- helpers
    def _normalise(self, cmd: dict):
        """
        Accept {'action':'add'/'remove'/'set', 'market':'spot', 'symbols':[…]}.
        Fills in defaults so ws_core always gets full keys.
        """
        cmd.setdefault("action",  "add")      # default = add
        cmd.setdefault("market",  "spot")
        # allow single-string shorthand
        if isinstance(cmd.get("symbols"), str):
            cmd["symbols"] = [cmd["symbols"]]
