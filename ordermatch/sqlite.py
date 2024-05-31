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

    def fetchone(self):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()
        row = cursor.fetchone()
        connection.close()
        return row
    
    def fetchall(self):
        connection = sqlite3.connect(self.db_name)
        cursor = connection.cursor()
        rows = cursor.fetchall()
        connection.close()
        return rows
    
    def create_table_pending_order(self):
        sql = """
        CREATE TABLE IF NOT EXISTS pending_order (
            clOrdID TEXT PRIMARY KEY,
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
            float(order.price), 
            float(order.quantity), 
            float(order.executedQuantity),
            float(order.lastExecutedQuantity),
            float(order.lastExecutedPrice),
            float(order.openQuantity),
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
            float(order.executedQuantity),
            float(order.lastExecutedQuantity),
            float(order.lastExecutedPrice),
            float(order.openQuantity),
            order.clOrdID
        )
        self.execute(sql, params)

    def select_order_pending_order(self, clOrdID):
        sql = "SELECT * FROM pending_order WHERE clOrdID = ?;"
        self.execute(sql, (clOrdID,))
        row = self.fetchone()
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
            float(order.price), 
            float(order.quantity), 
            float(order.executedQuantity),
            float(order.lastExecutedQuantity),
            float(order.lastExecutedPrice),
            float(order.openQuantity),
            order.insertTime
        )
        self.execute(sql, params)

    def close(self):
        self.connection.close()