from typing import Callable, Type

import quickfix as qf

from .utils import get_field_value


class MessageRouter:
    routes: dict[tuple[str, str], Callable] = {}

    def __init__(self) -> None:
        return

    def add_route(self, messageClass: Type[qf.Message], handler: Callable):
        message = messageClass()
        routeKey = self._get_routeKey(message)

        self.routes[routeKey] = handler

    def route(self, message: qf.Message, sessionID: qf.SessionID) -> Callable:
        routeKey = self._get_routeKey(message)
        handler = self.routes.get(routeKey)

        if not handler:
            raise qf.UnsupportedMessageType()
        return handler(message, sessionID)

    @classmethod
    def _get_routeKey(cls, message: qf.Message) -> tuple[str, str]:
        header: qf.Header = message.getHeader()
        beginString = get_field_value(header, qf.BeginString())
        msgType = get_field_value(header, qf.MsgType())
        return beginString, msgType
