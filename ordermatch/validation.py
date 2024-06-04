import csv
import decimal
from .sqlite_symbol import SymbolDatabase
from .order import Order
from .symbol import Symbol


db = SymbolDatabase('database.db')
class SymbolValidate: 
    def __init__(self):
        db.create_table_symbol()
        SymbolValidate.insert_symbol(self)

    def find_symbol(self, order: Order):
        return Order(db.find_symbol(order.symbol))
    
    def update_symbol(self, symbol: Symbol, order: Order):
        symbol.limitUp = order.price * decimal.Decimal(1.1)
        symbol.limitDown = order.price * decimal.Decimal(0.9)
        symbol.volume += order.quantity
        if symbol.open == decimal.Decimal(0):
            symbol.open = order.price
        if symbol.high < order.price:
            symbol.high = order.price
        if symbol.low > order.price:
            symbol.low = order.price
        
        db.update_symbol(symbol)
    
    def insert_symbol(self):
        with open('symbol.csv', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                symbol_data = {
                    'symbol': row['symbol'],
                    'close': decimal.Decimal(row['close']),
                    'limitUp': decimal.Decimal(row['limitUp']),
                    'limitDown': decimal.Decimal(row['limitDown'])
                }
                symbol = Symbol.mapping(symbol_data)
                if db.find_symbol(symbol.symbol) is None:
                    db.insert_symbol(symbol)
        
    def validate(self, order: Order):
        symbol = Symbol.mapping(db.find_symbol(order.symbol))
        if order.price > symbol.limitUp or order.price < symbol.limitDown:
            return False