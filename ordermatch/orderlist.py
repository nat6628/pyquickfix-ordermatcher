from collections import UserList
from typing import Optional, Union

import quickfix as qf

from .order import Order


class OrderList(UserList[Order]):
    _index_map: dict[str, int]

    def __init__(self, initlist=None, side: str = None):
        super().__init__(initlist)
        self._side = side
        self._index_map = {}

    def sort(self):
        """Sort orderList by price-time priority"""

        if self._side == qf.Side_BUY:
            # Best bid with the lowest price
            super().sort(key=lambda o: (o.price, o.insertTime))

        if self._side == qf.Side_SELL:
            # Best ask with the highest price
            super().sort(key=lambda o: (-o.price, o.insertTime))

    def insert(self, item: Order) -> None:
        inserted = False

        for order in self:
            if (self._side == qf.Side_BUY and item.price < order.price) or (
                self._side == qf.Side_SELL and item.price > order.price
            ):
                super().insert(self.index(order), item)
                inserted = True
                break

        if not inserted:
            # In  case empty order list or worst price
            self.append(item)

        self._update_index_map()

    def remove(self, item: Union[Order, str]):
        if isinstance(item, Order):
            super().remove(item)

        if isinstance(item, str):
            order_index = self._get_order_index(item)
            super().remove(self[order_index])

        self._update_index_map()

    def pop(self, i: Union[str, int] = -1) -> Optional[Order]:
        if isinstance(i, str):  # clOrdID
            order_index = self._get_order_index(i)
            if order_index is None:
                return
            order = super().pop(order_index)

        if isinstance(i, int):
            order = super().pop(i)

        self._update_index_map()
        return order

    def _get_order_index(self, clOrdID: str):
        return self._index_map.get(clOrdID)

    def _update_index_map(self):
        self._index_map = {order.clOrdID: self.index(order) for order in self.data}
