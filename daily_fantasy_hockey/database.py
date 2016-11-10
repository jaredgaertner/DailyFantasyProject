import sqlite3

class database(object):
    _sqlite_file = None
    _conn = None
    _cursor = None

    def __init__(self):
        self._sqlite_file = '../resources/sql/daily_fantasy_hockey_db.sqlite'

        # Make connection to an SQLite database file
        self._conn = sqlite3.connect(self._sqlite_file)

        # Allow rows to be reference by column name
        self._conn.row_factory = self.dict_factory

        self._cursor = self._conn.cursor()

        # Need to turn on foreign keys, not on by default
        self._cursor.execute('''PRAGMA foreign_keys = ON''')
        self._conn.commit()

    def query(self, query, params = None):
        if params == None:
            self._cursor.execute(query)
            self._conn.commit()
            return self._cursor
        else:
            self._cursor.execute(query, params)
            self._conn.commit()
            return self._cursor

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def get_last_row_id(self):
        return self._cursor.lastrowid

    def rollback(self):
        return self._conn.rollback()

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.commit()
        self._conn.close()

    def dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d