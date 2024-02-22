import decimal
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Order:
    clOrdID: str
    symbol: str
    senderCompID: str
    targetCompID: str
    side: str
    ordType: str
    price: decimal.Decimal
    quantity: decimal.Decimal
    executedQuantity = decimal.Decimal(0)
    avgPx = decimal.Decimal(0)
    insertTime: datetime = None
    lastExecutedQuantity = decimal.Decimal(0)
    lastExecutedPrice = decimal.Decimal(0)
    _open_quantity: decimal.Decimal = None

    def __str__(self) -> str:
        return (
            f"Order[clOrdID:{self.clOrdID} symbol:{self.symbol} side:{self.side} "
            f"ordType:{self.ordType} price:{self.price} quantity:{self.quantity}]"
        )

    def execute(self, price: decimal.Decimal, quantity: decimal.Decimal):
        executed_value = self.avgPx * self.executedQuantity
        self.executedQuantity += quantity
        self.lastExecutedPrice = price
        self.lastExecutedQuantity = quantity
        self.avgPx = (executed_value + price * quantity) / (self.executedQuantity)

    def cancel(self):
        self._open_quantity = decimal.Decimal(0)

    @property
    def openQuantity(self):
        if self._open_quantity is None:
            return self.quantity - self.executedQuantity
        return self._open_quantity

    @property
    def is_closed(self) -> bool:
        return self.openQuantity.is_zero()
