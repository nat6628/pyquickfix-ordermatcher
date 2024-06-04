import decimal
from dataclasses import dataclass

@dataclass
class Symbol:
    symbol: str
    close: decimal.Decimal
    limitUp: decimal.Decimal
    limitDown: decimal.Decimal
    open = decimal.Decimal(0)
    high = decimal.Decimal(0)
    low = decimal.Decimal(0)
    lastPrice = decimal.Decimal(0)
    volume = decimal.Decimal(0)
    
    @classmethod
    def mapping(cls, d:dict ):
        return cls(**d)
    
    def __init__(self, symbol: str, close: decimal.Decimal, limitUp: decimal.Decimal, limitDown: decimal.Decimal, open = decimal.Decimal(0), high = decimal.Decimal(0), low = decimal.Decimal(0), lastPrice = decimal.Decimal(0), volume = decimal.Decimal(0)):
        self.symbol = symbol
        self.close = close
        self.limitUp = limitUp
        self.limitDown = limitDown
        self.open = open
        self.high = high
        self.low = low
        self.lastPrice = lastPrice
        self.volume = volume
        
    

        


    
    