import sqlite3
from .order import Order

class Database:
    def __init__(self, db_name):
        self.db_name = db_name

    def execute(self, sql, params=()):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()
        cursor.execute(sql, params)
        connection.commit()
        connection.close()

    def fetchone(self, sql, params=()):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()
        cursor.execute(sql, params)
        row = cursor.fetchone()
        connection.close()
        return row
    
    def fetchall(self, sql, params=()):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        connection.close()
        return rows
    
    def create_table_pending_order(self):
        sql = """
        CREATE TABLE IF NOT EXISTS pending_order (
            clOrdID TEXT,
            symbol TEXT,
            senderCompID TEXT,
            targetCompID TEXT,
            side TEXT,
            ordType TEXT,
            price REAL,
            quantity REAL,
            executedQuantity REAL DEFAULT 0,
            lastExecutedQuantity REAL DEFAULT 0,
            lastExecutedPrice REAL DEFAULT 0,
            openQuantity REAL DEFAULT 0,
            insertTime DATETIME 
        );
        """
        self.execute(sql)

    def insert_order_pending_order(self, order: Order):
        sql = """
        INSERT INTO pending_order (
            clOrdID, symbol, senderCompID, targetCompID, side, ordType, price, quantity, executedQuantity, lastExecutedQuantity, lastExecutedPrice, openQuantity, insertTime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        params = (
            order.clOrdID, 
            order.symbol, 
            order.senderCompID, 
            order.targetCompID, 
            Order.side_mapping(order.side), 
            order.ordType, 
            round(float(order.price), 2), 
            round(float(order.quantity), 2), 
            round(float(order.executedQuantity), 2),
            round(float(order.lastExecutedQuantity), 2),
            round(float(order.lastExecutedPrice), 2),
            round(float(order.openQuantity), 2),
            order.insertTime
        )
        self.execute(sql, params)

    def update_order_pending_order(self, order: Order):
        sql = """
        UPDATE pending_order SET 
            executedQuantity = ?, 
            lastExecutedQuantity = ?, 
            lastExecutedPrice = ?, 
            openQuantity = ?
        WHERE clOrdID = ?;
        """
        params = (
            round(float(order.executedQuantity), 2),
            round(float(order.lastExecutedQuantity), 2),
            round(float(order.lastExecutedPrice), 2),
            round(float(order.openQuantity), 2),
            order.clOrdID
        )
        self.execute(sql, params)

    def select_order_pending_order(self, clOrdID):
        sql = "SELECT * FROM pending_order WHERE clOrdID = ?;"
        row = self.fetchone(sql, (clOrdID,))
        return row

    def delete_order_pending_order(self, clOrdID):
        sql = "DELETE FROM pending_order WHERE clOrdID = ?;"
        self.execute(sql, (clOrdID,))

    def create_table_order_history(self):
        sql = """
        CREATE TABLE IF NOT EXISTS order_history (
            clOrdID TEXT,
            symbol TEXT,
            senderCompID TEXT,
            targetCompID TEXT,
            side TEXT,
            ordType TEXT,
            status TEXT,
            price REAL,
            quantity REAL,
            executedQuantity REAL DEFAULT 0,
            lastExecutedQuantity REAL DEFAULT 0,
            lastExecutedPrice REAL DEFAULT 0,
            openQuantity REAL DEFAULT 0,
            insertTime DATETIME 
        );
        """
        self.execute(sql)
    
    def insert_order_history(self, order: Order, status):
        sql = """
        INSERT INTO order_history (
            clOrdID, symbol, senderCompID, targetCompID, side, ordType, status, price, quantity, executedQuantity, lastExecutedQuantity, lastExecutedPrice, openQuantity, insertTime
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        params = (
            order.clOrdID, 
            order.symbol, 
            order.senderCompID, 
            order.targetCompID, 
            Order.side_mapping(order.side), 
            order.ordType, 
            status,
            round(float(order.price), 2), 
            round(float(order.quantity), 2), 
            round(float(order.executedQuantity), 2),
            round(float(order.lastExecutedQuantity), 2),
            round(float(order.lastExecutedPrice), 2),
            round(float(order.openQuantity), 2),
            order.insertTime
        )
        self.execute(sql, params)

    def close(self):
        self.connection.close()