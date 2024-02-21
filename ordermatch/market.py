from datetime import datetime

import quickfix as qf

from .order import Order
from .orderlist import OrderList


class Market:
    def __init__(self) -> None:
        self.Asks = OrderList(side=qf.Side_SELL)
        self.Bids = OrderList(side=qf.Side_BUY)

    def insert(self, order: Order):
        order.insertTime = datetime.now()
        if order.side == qf.Side_SELL:
            self.Asks.insert(order)
        if order.side == qf.Side_BUY:
            self.Bids.insert(order)

    def cancel(self, clOrdId: str, side: str) -> Order:
        if side == qf.Side_SELL:
            order = self.Asks.pop(clOrdId)
        if side == qf.Side_BUY:
            order = self.Bids.pop(clOrdId)
        print(order)
        if order:
            order.cancel()
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
            if best_ask.is_closed:
                self.Asks.remove(best_ask)

            best_bid.execute(exec_price, exec_volume)
            matched.append(best_bid)
            if best_bid.is_closed:
                self.Bids.remove(best_bid)

        return matched
