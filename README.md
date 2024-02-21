# Python QuickFIX Order matcher

A simple matching engine with FIX acceptor written in Python

* Accept any canonical `NewOrderSingle` message for an instrument, with the instrument symbol consisting of an arbitrary string
* Accept any canonical `OrderCancelRequest` message for any order resting in the book
* Sends `ExecutionReport` messages when orders are matched, either partially or in full, or cancelled

## Features

* Support FIX 4.2
* Price-time priority matching for LIMIT orders
