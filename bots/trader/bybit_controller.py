import os
import math
import decimal
from pybit.unified_trading import HTTP
from config_trade_executor import (
    API_KEY, API_SECRET, LEVERAGE, STOP_LOSS_PERCENT,
    TAKE_PROFIT_PERCENT, LIMIT_OFFSET_PERCENT, SYMBOL_QTY_INCREMENT, ALLOCATION_PERCENT
)



class BybitController:
    def __init__(self, logger):
        self.logger = logger
        self.session = HTTP(api_key=API_KEY, api_secret=API_SECRET)

        self.logger.info("🔌 Connected to Bybit")
        self.logger.info("▶ LEVERAGE: %dx", LEVERAGE)
        self.logger.info("▶ Allocation: %.2f%%", ALLOCATION_PERCENT * 100)
        self.logger.info("▶ SL: %.2f%% | TP: %.2f%% | Offset: %.2f%%",
                         STOP_LOSS_PERCENT, TAKE_PROFIT_PERCENT, LIMIT_OFFSET_PERCENT)
        self.logger.info("▶ Symbol Increments: %s", SYMBOL_QTY_INCREMENT)

    def get_balance(self, asset="USDT"):
        try:
            res = self.session.get_wallet_balance(accountType="UNIFIED")
            for coin in res["result"]["list"][0]["coin"]:
                if coin["coin"] == asset:
                    balance = float(coin.get("availableToWithdraw") or coin["walletBalance"])
                    self.logger.info("💰 %s Balance: %.2f", asset, balance)
                    return balance
        except Exception as e:
            self.logger.error("❌ Balance fetch failed: %s", e)
        return 0.0

    def get_orderbook(self, symbol):
        try:
            book = self.session.get_orderbook(category="linear", symbol=symbol)
            best_bid = float(book["result"]["b"][0][0])
            best_ask = float(book["result"]["a"][0][0])
            self.logger.info("📘 Orderbook - Best Bid: %.2f | Best Ask: %.2f", best_bid, best_ask)
            return best_bid, best_ask
        except Exception as e:
            self.logger.error("❌ Failed to fetch orderbook for %s: %s", symbol, e)
            return None, None

    def set_leverage(self, symbol, leverage=LEVERAGE):
        try:
            self.session.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage)
            )
            self.logger.info("✅ Leverage set for %s to %sx", symbol, leverage)
        except Exception as e:
            if "leverage not modified" in str(e):
                self.logger.debug("ℹ️ Leverage already set for %s", symbol)
            else:
                self.logger.error("⚠️ Leverage setup failed: %s", e)
    def get_open_positions(self, symbol):
        """
        Returns a tuple: (positions_list, has_open_position)
        - positions_list: list of open positions for the symbol
        - has_open_position: True if any position size > 0, else False
        """
        try:
            res = self.session.get_positions(category="linear", symbol=symbol)
            positions = res.get("result", {}).get("list", [])
            has_open = any(float(pos.get("size", 0)) > 0 for pos in positions)
            if not positions:
                self.logger.info("📭 No open positions for %s", symbol)
            else:
                self.logger.info("📊 Open positions for %s:", symbol)
                for pos in positions:
                    self.logger.info("▶ Size: %s | Entry: %s | PnL: %s | Value: %s", 
                                     pos["size"], pos["avgPrice"], pos["unrealisedPnl"], pos["positionValue"])
            return positions, has_open
        except Exception as e:
            self.logger.error("❌ Failed to fetch open positions for %s: %s", symbol, e)
            return [], False

    def has_open_position(self, symbol):
        try:
            positions = self.session.get_positions(category="linear", symbol=symbol)
            for p in positions.get("result", {}).get("list", []):
                if float(p.get("size", 0)) > 0:
                    return True
        except Exception as e:
            self.logger.error("❌ Failed to check open position for %s: %s", symbol, e)
        return False

    def round_qty(self, symbol, qty):
        increment = SYMBOL_QTY_INCREMENT.get(symbol, 0.001)
        rounded_qty = math.floor(qty / increment) * increment
        decimal_places = abs(decimal.Decimal(str(increment)).as_tuple().exponent)
        return round(rounded_qty, decimal_places)

    def format_qty(self, symbol, qty):
        increment = SYMBOL_QTY_INCREMENT.get(symbol, 0.001)
        decimal_places = abs(decimal.Decimal(str(increment)).as_tuple().exponent)
        return f"{qty:.{decimal_places}f}"
    def get_available_usdt(self):
        try:
            res = self.session.get_wallet_balance(accountType="UNIFIED")
            for coin in res["result"]["list"][0]["coin"]:
                if coin["coin"] == "USDT":
                    balance = float(coin.get("availableToWithdraw") or coin["walletBalance"])
                    self.logger.info("💰 Available USDT: %.2f", balance)
                    return balance
        except Exception as e:
            self.logger.error("❌ Balance fetch failed: %s", e)
        return 0.0
    def place_limit_order(self, symbol, side):
        if self.has_open_position(symbol):
            self.logger.info("⚠️ %s already has an open position. Skipping.", symbol)
            return

        try:
            best_bid, best_ask = self.get_orderbook(symbol)
            base_price = best_bid if side == "long" else best_ask
            offset_price = base_price * (1 - LIMIT_OFFSET_PERCENT / 100) if side == "long" else base_price * (1 + LIMIT_OFFSET_PERCENT / 100)
            limit_price = round(offset_price, 4)

            max_deviation = 0.1  # %
            if abs(limit_price - base_price) / base_price > (max_deviation / 100):
                limit_price = round(base_price, 4)
                self.logger.info("⚠️ Offset capped to prevent excess slippage. New limit price: %.4f", limit_price)

            available = self.get_balance()
            allocation = available * ALLOCATION_PERCENT
            raw_qty = (allocation * LEVERAGE) / limit_price
            qty = self.round_qty(symbol, raw_qty)

            notional = qty * limit_price
            min_notional = 100
            min_qty = max(0.001, 100 / limit_price)

            while qty * limit_price < min_notional and qty > min_qty:
                qty -= SYMBOL_QTY_INCREMENT[symbol]
                qty = self.round_qty(symbol, qty)

            if qty < min_qty:
                self.logger.warning("⚠️ Qty %.4f below minimum %.4f for %s. Skipping.", qty, min_qty, symbol)
                return

            self.logger.info("🧮 Final Qty: %.10f | Notional: %.2f", qty, qty * limit_price)

            sl = limit_price * (1 - STOP_LOSS_PERCENT / 100) if side == "long" else limit_price * (1 + STOP_LOSS_PERCENT / 100)
            tp = limit_price * (1 + TAKE_PROFIT_PERCENT / 100) if side == "long" else limit_price * (1 - TAKE_PROFIT_PERCENT / 100)

            self.set_leverage(symbol)
            formatted_qty = self.format_qty(symbol, qty)

            resp = self.session.place_order(
                category="linear",
                symbol=symbol,
                side="Buy" if side == "long" else "Sell",
                orderType="Limit",
                qty=f"{qty:.3f}",
                price=f"{limit_price:.4f}",
                timeInForce="PostOnly",
                stopLoss=f"{sl:.4f}",
                takeProfit=f"{tp:.4f}"
            )

            self.logger.info("📡 Bybit Response: %s", resp)
            self.logger.info("🚀 %s LIMIT %s placed | Qty: %s | Entry: %.2f | SL: %.2f | TP: %.2f",
                             symbol, side.upper(), formatted_qty, limit_price, sl, tp)

        except Exception as e:
            self.logger.error("❌ Order failed for %s: %s", symbol, e)
