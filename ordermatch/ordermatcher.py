from .market import Market
from .order import Order
from .validation import SymbolValidate


class OrderMatcher:
    markets: dict[str, "Market"]

    def __init__(self) -> None:
        self.markets = {}
        self.symbol_validate = SymbolValidate()

    def insert(self, order: Order):
        market = self.markets.get(order.symbol)
        if not market:
            market = Market()
            self.markets[order.symbol] = market
        if self.symbol_validate.validate(order):
            market.insert(order)

    def cancel(self, clOrdId: str, symbol: str, side: str):
        market = self.markets.get(symbol)
        if not market:
            return
        return market.cancel(clOrdId, side)

    def match(self, symbol: str) -> list[Order]:
        market = self.markets.get(symbol)
        if not market:
            return []
        return market.match()
    
    def find(self, clOrdId: str, symbol: str, side: str):
        market = self.markets.get(symbol)
        if not market:
            return
        return market.find(clOrdId, side)
