from datetime import datetime

import quickfix as qf

from .sqlite import Database
from .order import Order
from .orderlist import OrderList
from .validation import SymbolValidate

db = Database("database.db")

class Market:
    def __init__(self) -> None:
        self.Asks = OrderList(side=qf.Side_SELL)
        self.Bids = OrderList(side=qf.Side_BUY)
        db.create_table_pending_order()
        db.create_table_order_history()

    def insert(self, order: Order):
        order.insertTime = datetime.now()
        if order.side == qf.Side_SELL:
            self.Asks.insert(order)
            db.insert_order_pending_order(order)
        if order.side == qf.Side_BUY:
            self.Bids.insert(order)
            db.insert_order_pending_order(order)

    def cancel(self, clOrdId: str, side: str) -> Order:
        if side == qf.Side_SELL:
            order = self.Asks.pop(clOrdId)
            if not order:
                order = Order(db.select_order_pending_order(clOrdId))

        if side == qf.Side_BUY:
            order = self.Bids.pop(clOrdId)
            if not order:
                order = Order(db.select_order_pending_order(clOrdId))

        if order:
            order.cancel()
            db.delete_order_pending_order(order.clOrdID)
            db.insert_order_history(order, "Canceled")
            return order

    def match(self) -> list[Order]:
        matched = []

        while len(self.Asks) > 0 and len(self.Bids) > 0:
            best_ask = self.Asks[0]
            best_bid = self.Bids[0]
            if best_ask.price > best_bid.price:
                break

            # Exec Vol = min bestBid/bestAsk openQuantity
            exec_volume = min(best_bid.openQuantity, best_ask.openQuantity)
            # Exec Price = maker order price
            exec_price = min(best_ask, best_bid, key=lambda order: order.insertTime).price

            best_ask.execute(exec_price, exec_volume)
            matched.append(best_ask)
            SymbolValidate.update_symbol(best_ask.symbol, best_ask)
            if best_ask.is_closed:
                self.Asks.remove(best_ask)
                db.delete_order_pending_order(best_ask.clOrdID)
                db.insert_order_history(best_ask, "Filled")
            else:
                db.update_order_pending_order(best_ask)
                db.insert_order_history(best_ask, "Partially filled")
            
            best_bid.execute(exec_price, exec_volume)
            matched.append(best_bid)
            SymbolValidate.update_symbol(best_bid.symbol, best_bid)
            if best_bid.is_closed:
                self.Bids.remove(best_bid)
                db.delete_order_pending_order(best_bid.clOrdID)
                db.insert_order_history(best_bid, "Filled")
            else:
                db.update_order_pending_order(best_bid)
                db.insert_order_history(best_bid, "Partially filled")

        return matched
    
    def find(self, clOrdId: str, side: str) -> Order:
        if side == qf.Side_SELL:
            order = self.Asks.pop(clOrdId)
        if side == qf.Side_BUY:
            order = self.Bids.pop(clOrdId)
        return order
