import decimal
import logging
import time

import quickfix as qf
from quickfix42 import NewOrderSingle, OrderCancelRequest, OrderCancelReplaceRequest

from .order import Order
from .ordermatcher import OrderMatcher
from .router import MessageRouter
from .utils import gen_execID, get_field_value, get_float, log_message

_logger = logging.getLogger(__name__)


class Application(qf.Application):
    def __init__(self, node_id: int):
        super().__init__()

        self.order_matcher = OrderMatcher()
        self.gen_execID = gen_execID(node_id)

        # Setup message router
        self.router = MessageRouter()
        self.router.add_route(NewOrderSingle, self.onNewOrderSingle)
        self.router.add_route(OrderCancelRequest, self.onOrderCancelRequest)
        self.router.add_route(OrderCancelReplaceRequest, self.onOrderCancelReplaceRequest)

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
        else:
            reject_message = qf.Message()
            reject_message.getHeader().setField(qf.MsgType(qf.MsgType_OrderCancelReject))
            reject_message.setField(qf.OrigClOrdID(origClOrdID))
            reject_message.setField(qf.ClOrdID(origClOrdID))
            reject_message.setField(qf.OrdStatus(qf.OrdStatus_REJECTED))
            reject_message.setField(qf.Text("Order not found"))
            qf.Session.sendToTarget(reject_message, sessionID)
            
    def onOrderCancelReplaceRequest(self, message: qf.Message, sessionID: qf.SessionID):
        origClOrdID = get_field_value(message, qf.OrigClOrdID())
        clOrdID = get_field_value(message, qf.ClOrdID())
        symbol = get_field_value(message, qf.Symbol())
        side = get_field_value(message, qf.Side())
        price = get_field_value(message, qf.Price())
        orderQty = get_field_value(message, qf.OrderQty())
        ordType = get_field_value(message, qf.OrdType())

        original_order = self.order_matcher.find(origClOrdID, symbol, side)
        
        if not original_order:
        # Order not found then reject
            reject_message = qf.Message()
            reject_message.getHeader().setField(qf.MsgType(qf.MsgType_OrderCancelReject))
            reject_message.setField(qf.OrigClOrdID(origClOrdID))
            reject_message.setField(qf.ClOrdID(clOrdID))
            reject_message.setField(qf.OrdStatus(qf.OrdStatus_REJECTED))
            reject_message.setField(qf.Text("Order not found"))
            qf.Session.sendToTarget(reject_message, sessionID)
            return

        self.order_matcher.cancel(origClOrdID, symbol, side)
        self.execution_report(original_order, qf.OrdStatus_CANCELED, sessionID)

        # Create a new order with the updated details
        new_order = Order(
            clOrdID=clOrdID,
            symbol=symbol,
            side=side,
            ordType=ordType,
            price=decimal.Decimal(str(price)),
            quantity=decimal.Decimal(str(orderQty)),
            senderCompID=original_order.senderCompID,
            targetCompID=original_order.targetCompID,
        )

        # Insert the new order and send a report
        self.order_matcher.insert(new_order)
        self.execution_report(new_order, qf.OrdStatus_NEW, sessionID)

        # Try to match the new order
        matched = self.order_matcher.match(new_order.symbol)
        if matched:
            for new_order in matched:
                updated_status = qf.OrdStatus_FILLED if new_order.is_closed else qf.OrdStatus_PARTIALLY_FILLED
                self.execution_report(new_order, updated_status, sessionID)

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
