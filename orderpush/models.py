import threading
from dataclasses import asdict, dataclass
from typing import List

_lock = threading.RLock()


@dataclass
class Order:
    order_id: str
    symbol_code: str
    order_price: float
    side: str
    qty: int


@dataclass
class Band:
    price: float
    volume: int

    def __str__(self) -> str:
        return str(self.price) + "-" + str(self.volume)


@dataclass
class OHLC:
    open: float
    high: float
    low: float
    close: float


@dataclass
class Quote:
    symbol: str
    bids: List[Band]
    asks: List[Band]
    ohlc: OHLC
    trade_price: float
    trade_size: float
    total_trade_volume: float

    @classmethod
    def create_blank(cls, symbol: str):
        return cls(symbol, [], [], OHLC(0, 0, 0, 0), 0, 0, 0)

    def update(self, updating_quote: "Quote"):
        with _lock:
            for key, value in asdict(updating_quote).items():
                if not value:
                    continue
                elif key == "ohlc":
                    if not any(value.values()):
                        continue
                    setattr(self, key, OHLC(**value))
                elif key == "trade_size":
                    setattr(self, "total_trade_volume", self.total_trade_volume + value)
                elif key in ("bids", "asks"):
                    setattr(self, key, [Band(v.get("price"), int(v.get("volume"))) for v in value if v.get("price")])
                else:
                    setattr(self, key, value)

    def __str__(self) -> str:
        return (
            f"symbol={self.symbol}, last_price={self.trade_price}, "
            f"total_trade_volume={self.total_trade_volume}, "
            f"ohlc=[{':'.join(str(price) for price in asdict(self.ohlc).values())}], "
            f"bids=[{'|'.join(str(band) for band in self.bids)}], "
            f"asks=[{'|'.join(str(band) for band in self.asks)}]"
        )


@dataclass
class Symbol:
    symbol_code: str
    last_price: float
