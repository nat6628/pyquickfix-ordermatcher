import sqlite3
from .symbol import Symbol

class SymbolDatabase:
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
    def create_table_symbol(self):
        sql = """
        CREATE TABLE IF NOT EXISTS symbol (
            symbol TEXT PRIMARY KEY,
            close REAL,
            limitUp REAL,
            limitDown REAL,
            open REAL DEFAULT 0,
            high REAL DEFAULT 0,
            low REAL DEFAULT 0,
            lastPrice REAL DEFAULT 0,
            volume REAL DEFAULT 0
        );
        """
        self.execute(sql)
    
    def find_symbol(self, symbol):
        sql = """SELECT * FROM symbol WHERE symbol = ?;"""
        row = self.fetchone(sql, (symbol,))
        return row
    
    def update_symbol(self, symbol: Symbol):
        sql = """UPDATE symbol SET 
                    limitUp = ?, 
                    limitDown = ?, 
                    open = ?, 
                    high = ?, 
                    low = ?, 
                    lastPrice = ?, 
                    volume = ? 
                WHERE symbol = ?;"""
        params = (
            round(float(symbol.limitUp), 2),
            round(float(symbol.limitDown), 2),
            round(float(symbol.open), 2),
            round(float(symbol.high), 2),
            round(float(symbol.low), 2),
            round(float(symbol.lastPrice), 2),
            round(float(symbol.volume), 2),
            symbol.symbol
        )
        self.execute(sql, params)
        
    def insert_symbol(self, symbol: Symbol):
        sql = """INSERT INTO symbol (
                    symbol, close, limitUp, limitDown
                ) VALUES (?, ?, ?, ?);"""
        params = (
            symbol.symbol,
            round(float(symbol.close), 2),
            round(float(symbol.limitUp), 2),
            round(float(symbol.limitDown), 2)
        )
        self.execute(sql, params)
    