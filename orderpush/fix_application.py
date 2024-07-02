import os
import time

import quickfix as qf
import quickfix42 as qf42

from .models import Order
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
                order = Order.from_csv(clOrderId, line)
                print(order)
                clOrderId += 1
