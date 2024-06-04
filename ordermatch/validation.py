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
    
    def update_symbol(self, symbol: str, order: Order):
        symbol_data_sql = db.find_symbol(symbol)
        symbol_data = {
            'symbol': symbol,
            'close': decimal.Decimal(symbol_data_sql[1]),
            'limitUp': decimal.Decimal(symbol_data_sql[2]),
            'limitDown': decimal.Decimal(symbol_data_sql[3]),
            'open': decimal.Decimal(symbol_data_sql[4]),
            'high': decimal.Decimal(symbol_data_sql[5]),
            'low': decimal.Decimal(symbol_data_sql[6]),
            'lastPrice': decimal.Decimal(symbol_data_sql[7]),
            'volume': decimal.Decimal(symbol_data_sql[8])
        }
        symbol_mapping = Symbol.mapping(symbol_data)
        symbol_mapping.limitUp = order.price * decimal.Decimal(1.1)
        symbol_mapping.limitDown = order.price * decimal.Decimal(0.9)
        symbol_mapping.lastPrice = order.price
        symbol_mapping.volume += order.quantity
        if symbol_mapping.open == decimal.Decimal(0):
            symbol_mapping.open = order.price
        if symbol_mapping.high < order.price:
            symbol_mapping.high = order.price
        if (symbol_mapping.low > order.price or symbol_mapping.low == decimal.Decimal(0)):
            symbol_mapping.low = order.price
        
        db.update_symbol(symbol_mapping)
    
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
        symbol_data_sql = db.find_symbol(order.symbol)
        if symbol_data_sql is not None:
            symbol, close, limitUp, limitDown, open, high, low, lastPrice, volume = symbol_data_sql
            symbol_data = {
                'symbol': symbol,
                'close': decimal.Decimal(close),
                'limitUp': decimal.Decimal(limitUp),
                'limitDown': decimal.Decimal(limitDown),
                'open': decimal.Decimal(open),
                'high': decimal.Decimal(high),
                'low': decimal.Decimal(low),
                'lastPrice': decimal.Decimal(lastPrice),
                'volume': decimal.Decimal(volume)
            }
            symbol = Symbol.mapping(symbol_data)
            if order.price > symbol.limitUp or order.price < symbol.limitDown:
                return False
            else:
                return True