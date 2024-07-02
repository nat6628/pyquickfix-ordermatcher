import threading
from dataclasses import asdict, dataclass
from typing import List

_lock = threading.RLock()


@dataclass
class Order:
    type: str
    order_id: str
    symbol_code: str
    order_price: float
    side: str
    qty: int

    def to_csv(self) -> str:
        csv_attrs = [self.type, self.symbol_code, self.order_price, self.side, self.qty]
        return ",".join(csv_attrs)

    @classmethod
    def from_csv(cls, order_id: int, order_str: str) -> "Order":
        type, symbol_code, order_price, side, qty = order_str.split(",")
        return Order(type, str(order_id), symbol_code, float(order_price), side, int(qty))

@dataclass
class CancelOrder:
    type: str
    order_id: str
    symbol_code: str
    side: str


    def to_csv(self) -> str:
        csv_attrs = [self.type, self.order_id, self.symbol_code, self.side]
        return ",".join(csv_attrs)
    
    @classmethod
    def from_csv(cls, order_str: str) -> "CancelOrder":
        type, order_id, symbol_code, side = order_str.split(",")
        return CancelOrder(type, str(order_id), symbol_code, side.strip())
    
@dataclass
class ModifyOrder:
    type: str
    orgin_order_id: str
    symbol_code: str
    order_price: float
    side: str
    qty: int
    order_id: str

    def to_csv(self) -> str:
        csv_attrs = [self.type, self.orgin_order_id, self.symbol_code, self.order_price, self.side, self.qty]
        return ",".join(csv_attrs)

    @classmethod
    def from_csv(cls, order_id: int, order_str: str) -> "ModifyOrder":
        type, orgin_order_id, symbol_code, order_price, side, qty = order_str.split(",")
        return ModifyOrder(type, str(orgin_order_id), symbol_code, float(order_price), side, int(qty), str(order_id).strip())