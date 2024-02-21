import decimal
import logging
import time

import quickfix as qf
from quickfix42 import NewOrderSingle, OrderCancelRequest

from .order import Order
from .ordermatcher import OrderMatcher
from .router import MessageRouter
from .utils import gen_execID, get_field_value, get_float, log_message

_logger = logging.getLogger(__name__)


class Application(qf.Application):
    def __init__(self):
        super().__init__()

        self.order_matcher = OrderMatcher()
        self.gen_execID = gen_execID()

        # Setup message router
        self.router = MessageRouter()
        self.router.add_route(NewOrderSingle, self.onNewOrderSingle)
        self.router.add_route(OrderCancelRequest, self.onOrderCancelRequest)

    def onCreate(self, sessionID):
        _logger.debug("onCreate : Session (%s)" % sessionID.toString())

    def onLogon(self, sessionID):
        _logger.debug("Successful Logon to session '%s'." % sessionID.toString())

    def onLogout(self, sessionID):
        _logger.debug("Session (%s) logout !" % sessionID.toString())

    def toAdmin(self, message, sessionID):
        _logger.debug("Message ToAdmin: %s" % log_message(message))

    def fromAdmin(self, message, sessionID):
        _logger.debug("Message FromAdmin: %s" % log_message(message))

    def toApp(self, message, sessionID):
        _logger.info("Message ToApp: %s" % log_message(message))

    def fromApp(self, message: qf.Message, sessionID: qf.SessionID):
        _logger.info("Message FromApp: %s" % log_message(message))
        self.router.route(message, sessionID)

    def onNewOrderSingle(self, message: qf.Message, sessionID: qf.SessionID):
        header = message.getHeader()
        senderCompID = get_field_value(header, qf.SenderCompID())
        targetCompID = get_field_value(header, qf.TargetCompID())

        clOrdID = get_field_value(message, qf.ClOrdID())
        symbol = get_field_value(message, qf.Symbol())
        side = get_field_value(message, qf.Side())
        ordType = get_field_value(message, qf.OrdType())
        price = get_field_value(message, qf.Price())
        orderQty = get_field_value(message, qf.OrderQty())

        order = Order(
            clOrdID=clOrdID,
            symbol=symbol,
            side=side,
            ordType=ordType,
            price=decimal.Decimal(str(price)),
            quantity=decimal.Decimal(str(orderQty)),
            senderCompID=senderCompID,
            targetCompID=targetCompID,
        )

        # Insert order
        self.order_matcher.insert(order)
        self.execution_report(order, qf.OrdStatus_NEW, sessionID)

        # Match order
        matched = self.order_matcher.match(order.symbol)
        if not matched:
            return

        # Fill matched order(s)
        for order in matched:
            updated_status = qf.OrdStatus_FILLED
            if not order.is_closed:
                updated_status = qf.OrdStatus_PARTIALLY_FILLED
            self.execution_report(order, updated_status, sessionID)

    def onOrderCancelRequest(self, message: qf.Message, sessionID: qf.SessionID):
        origClOrdID = get_field_value(message, qf.OrigClOrdID())
        symbol = get_field_value(message, qf.Symbol())
        side = get_field_value(message, qf.Side())

        order = self.order_matcher.cancel(origClOrdID, symbol, side)
        if order:
            self.execution_report(order, qf.OrdStatus_CANCELED, sessionID)

    def execution_report(self, order: Order, ordStatus: str, sessionID: qf.SessionID):
        execReport = qf.Message()

        header = execReport.getHeader()
        header.setField(sessionID.getBeginString())
        header.setField(qf.MsgType(qf.MsgType_ExecutionReport))
        header.setField(qf.TargetCompID(order.senderCompID))
        header.setField(qf.SenderCompID(order.targetCompID))

        execReport.setField(qf.OrderID(order.clOrdID))
        execReport.setField(qf.ExecID(self.gen_execID()))
        execReport.setField(qf.ExecTransType(qf.ExecTransType_NEW))
        execReport.setField(qf.ExecType(ordStatus))
        execReport.setField(qf.OrdStatus(ordStatus))
        execReport.setField(qf.Symbol(order.symbol))
        execReport.setField(qf.Side(order.side))
        execReport.setField(qf.LeavesQty(get_float(order.openQuantity)))
        execReport.setField(qf.CumQty(get_float(order.executedQuantity)))
        execReport.setField(qf.AvgPx(get_float(order.avgPx)))
        execReport.setField(qf.OrderQty(get_float(order.quantity)))
        execReport.setField(qf.ClOrdID(order.clOrdID))

        if ordStatus in (qf.OrdStatus_FILLED, qf.OrdStatus_PARTIALLY_FILLED):
            execReport.setField(qf.LastShares(get_float(order.lastExecutedQuantity)))
            execReport.setField(qf.LastPx(get_float(order.lastExecutedPrice)))

        qf.Session.sendToTarget(execReport, sessionID)

    def run(self):
        """Keep mainThread running"""
        while True:
            time.sleep(2)
