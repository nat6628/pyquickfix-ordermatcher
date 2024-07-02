import os
import time
import threading

import quickfix as qf
import quickfix42 as qf42

from .models import Order, CancelOrder, ModifyOrder
from .utils import now

__SOH__ = chr(1)
MDREQID = "TRADE_SIMULATION"
MARKETDEPTH_FULLBOOK = 0
SLEEP_TIME = 0.01


class FixApplication(qf.Application):
    def __init__(self, node_id: int):
        super().__init__()

        self.gen_execID = node_id

    def onCreate(self, sessionID):
        print("onCreate : Session (%s)" % sessionID.toString())
        return

    def onLogon(self, sessionID):
        self.sessionID = sessionID
        print("Successful Logon to session '%s'." % sessionID.toString())
        return

    def onLogout(self, sessionID):
        # print("Session (%s) logout !" % sessionID.toString())
        return

    def toAdmin(self, message, sessionID):
        # msg = message.toString().replace(__SOH__, "|")
        # print("(Admin) S >> %s" % msg)
        return

    def fromAdmin(self, message, sessionID):
        # msg = message.toString().replace(__SOH__, "|")
        # print("(Admin) R << %s" % msg)
        return

    def toApp(self, message, sessionID):
        # msg = message.toString().replace(__SOH__, "|")
        # print("(App) S >> %s" % msg)
        return

    def fromApp(self, message, sessionID):
        # msg = message.toString().replace(__SOH__, "|")
        # print("(App) R << %s" % msg)
        self.onMessage(message, sessionID)
        return

    def sendNewOrderSingle(self, order: Order):
        if self.sessionID.getBeginString().getValue() == qf.BeginString_FIX42:
            message = qf42.NewOrderSingle()

            message.setField(qf.ClOrdID(order.order_id))
            message.setField(qf.Side(order.side))
            message.setField(qf.Symbol(order.symbol_code))
            message.setField(qf.Price(order.order_price))
            message.setField(qf.OrderQty(order.qty))
            message.setField(qf.OrdType(qf.OrdType_LIMIT))
            message.setField(
                qf.HandlInst(
                    qf.HandlInst_AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION
                )
            )
            message.setField(qf.TimeInForce(qf.TimeInForce_GOOD_TILL_CANCEL))

            trstime = qf.TransactTime()
            trstime.setString(now())
            message.setField(trstime)

            qf.Session.sendToTarget(message, self.sessionID)

    def sendCancelOrder(self, cancel_order: CancelOrder):
        if self.sessionID.getBeginString().getValue() == qf.BeginString_FIX42:
            message = qf42.OrderCancelRequest()

            message.setField(qf.ClOrdID(cancel_order.order_id))
            message.setField(qf.OrigClOrdID(cancel_order.order_id))
            message.setField(qf.Symbol(cancel_order.symbol_code))
            message.setField(qf.Side(cancel_order.side))

            trstime = qf.TransactTime()
            trstime.setString(now())
            message.setField(trstime)

            qf.Session.sendToTarget(message, self.sessionID)
    def sendCancelReplaceOrder(self, modify_order: ModifyOrder):
        if self.sessionID.getBeginString().getValue() == qf.BeginString_FIX42:
            message = qf42.OrderCancelReplaceRequest()

            message.setField(qf.ClOrdID(modify_order.order_id))
            message.setField(qf.OrigClOrdID(modify_order.orgin_order_id))
            message.setField(qf.Symbol(modify_order.symbol_code))
            message.setField(qf.Side(modify_order.side))
            message.setField(qf.Price(modify_order.order_price))
            message.setField(qf.OrderQty(modify_order.qty))
            message.setField(qf.OrdType(qf.OrdType_LIMIT))
            message.setField(
                qf.HandlInst(
                    qf.HandlInst_AUTOMATED_EXECUTION_ORDER_PRIVATE_NO_BROKER_INTERVENTION
                )
            )
            message.setField(qf.TimeInForce(qf.TimeInForce_GOOD_TILL_CANCEL))

            trstime = qf.TransactTime()
            trstime.setString(now())
            message.setField(trstime)

            qf.Session.sendToTarget(message, self.sessionID)
    def run(self):
        """Keep mainThread running"""

        clOrderId = 1

        with open("order.csv", "r") as file:
            file.seek(0, os.SEEK_END)

            while True:
                line = file.readline()
                if not line:
                    time.sleep(SLEEP_TIME)
                    continue
                parts = line.split(",")
                order_type = parts[0]
                
                if order_type == "NEW":
                    order = Order.from_csv(clOrderId, line)
                    self.sendNewOrderSingle(order)
                elif order_type == "CANCEL":
                    cancel_order = CancelOrder.from_csv(line)
                    print(cancel_order)
                    self.sendCancelOrder(cancel_order)
                elif order_type == "MODIFY":
                    modify_order = ModifyOrder.from_csv(clOrderId, line)
                    print(modify_order)
                    self.sendCancelReplaceOrder(modify_order)
                
                clOrderId += 1