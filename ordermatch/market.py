from datetime import datetime

import quickfix as qf
import decimal

from .sqlite import Database
from .order import Order
from .orderlist import OrderList
from .validation import SymbolValidate

db = Database("database.db")

class Market:
    def __init__(self) -> None:
        self.Asks = OrderList(side=qf.Side_SELL)
        self.Bids = OrderList(side=qf.Side_BUY)
        self.symbol_validate = SymbolValidate()
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
                order_data_sql = db.select_order_pending_order(clOrdId)
                order_data = {
                    'clOrdID': order_data_sql[0],
                    'symbol': order_data_sql[1],
                    'senderCompID': order_data_sql[2],
                    'targetCompID': order_data_sql[3],
                    'side': Order.side_mapping(order_data_sql[4]),
                    'ordType': order_data_sql[5],
                    'price': decimal.Decimal(order_data_sql[6]),
                    'quantity': decimal.Decimal(order_data_sql[7]),
                    'executedQuantity': decimal.Decimal(order_data_sql[8]),
                    'lastExecutedQuantity': decimal.Decimal(order_data_sql[9]),
                    'lastExecutedPrice': decimal.Decimal(order_data_sql[10]),
                    '_open_quantity': decimal.Decimal(order_data_sql[11]),
                    'insertTime': datetime.strptime(order_data_sql[12], '%Y-%m-%d %H:%M:%S.%f')
                }
                order = Order.mapping(order_data)

        if side == qf.Side_BUY:
            order = self.Bids.pop(clOrdId)
            if not order:
                order_data_sql = db.select_order_pending_order(clOrdId)
                order_data = {
                    'clOrdID': order_data_sql[0],
                    'symbol': order_data_sql[1],
                    'senderCompID': order_data_sql[2],
                    'targetCompID': order_data_sql[3],
                    'side': Order.side_mapping(order_data_sql[4]),
                    'ordType': order_data_sql[5],
                    'price': decimal.Decimal(order_data_sql[6]),
                    'quantity': decimal.Decimal(order_data_sql[7]),
                    'executedQuantity': decimal.Decimal(order_data_sql[8]),
                    'lastExecutedQuantity': decimal.Decimal(order_data_sql[9]),
                    'lastExecutedPrice': decimal.Decimal(order_data_sql[10]),
                    '_open_quantity': decimal.Decimal(order_data_sql[11]),
                    'insertTime': datetime.strptime(order_data_sql[12], '%Y-%m-%d %H:%M:%S.%f')
                }
                order = Order.mapping(order_data)

        if order:
            order.cancel()
            db.delete_order_pending_order(order.clOrdID)
            db.insert_order_history(order, "Canceled")
            return order

    def match(self) -> list[Order]:
        matched = []

        while len(self.Asks) > 0 and len(self.Bids) > 0:
            self.Asks = sorted(self.Asks, key=lambda x: x.price)
            best_ask = self.Asks[0]

            self.Bids = sorted(self.Bids, key=lambda x: x.price, reverse=True)
            best_bid = self.Bids[0]
            print("best_ask", best_ask)
            print("best_bid", best_bid)
            if best_ask.price > best_bid.price:
                break

            # Exec Vol = min bestBid/bestAsk openQuantity
            exec_volume = min(best_bid.openQuantity, best_ask.openQuantity)
            # Exec Price = maker order price
            exec_price = min(best_ask, best_bid, key=lambda order: order.insertTime).price

            best_ask.execute(exec_price, exec_volume)
            matched.append(best_ask)
            self.symbol_validate.update_symbol(best_ask.symbol, best_ask)
            if best_ask.is_closed:
                self.Asks.remove(best_ask)
                db.delete_order_pending_order(best_ask.clOrdID)
                db.insert_order_history(best_ask, "Filled")
            else:
                db.update_order_pending_order(best_ask)
                db.insert_order_history(best_ask, "Partially filled")
            
            best_bid.execute(exec_price, exec_volume)
            matched.append(best_bid)
            self.symbol_validate.update_symbol(best_bid.symbol, best_bid)
            if best_bid.is_closed:
                self.Bids.remove(best_bid)
                db.delete_order_pending_order(best_bid.clOrdID)
                db.insert_order_history(best_bid, "Filled")
            else:
                db.update_order_pending_order(best_bid)
                db.insert_order_history(best_bid, "Partially filled")

        return matched
    
    def find(self, clOrdId: str) -> Order:
        order = self.Asks.pop(clOrdId)
        if not order:
            order = self.Bids.pop(clOrdId)
        return order
