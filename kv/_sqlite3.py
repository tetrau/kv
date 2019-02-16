import sqlite3


class SQLite3Adaptor:
    @staticmethod
    def _type_name(t):
        m = {str: "TEXT", int: "INTEGER", bytes: "BLOB", float: "REAL"}
        return m[t]

    def _cursor(self):
        return self._connection.cursor()

    def _table_name(self):
        m = {str: "s", int: "i", bytes: "b", float: "f"}
        return "{}_{}{}".format(self.table,
                                m[self.key_type],
                                m[self.value_type])

    def __init__(self, database, table, key_type=str, value_type=str, index=True):
        accepted_type = (str, int, bytes, float)
        if key_type not in accepted_type:
            raise ValueError("not accepted key type {}".format(key_type))
        if value_type not in accepted_type:
            raise ValueError("not accepted value type {}".format(value_type))
        self._connection = sqlite3.connect(database)
        self.key_type = key_type
        self.value_type = value_type
        self.table = table
        cursor = self._cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS {} (k {} UNIQUE, v {});"
                       .format(self._table_name(),
                               self._type_name(self.key_type),
                               self._type_name(self.value_type)))
        if index:
            cursor.execute("CREATE INDEX IF NOT EXISTS __index_on_{} "
                           "ON {} (k);".format(self._table_name(),
                                               self._table_name()))
        self._connection.commit()

    def get(self, k):
        cursor = self._cursor()
        cursor.execute("SELECT v FROM {} WHERE k = ?".format(self._table_name()),
                       (k,))
        row = cursor.fetchone()
        if row is not None:
            return row[0]
        else:
            return None

    def put(self, k, v):
        cursor = self._cursor()
        cursor.execute("INSERT OR REPLACE INTO {} (k, v) "
                       "VALUES (?, ?)".format(self._table_name()),
                       (k, v))
        self._connection.commit()

    def delete(self, k):
        cursor = self._cursor()
        cursor.execute("DELETE FROM {} WHERE k = ?".format(self._table_name()),
                       (k,))
        self._connection.commit()

    def contains(self, k):
        cursor = self._cursor()
        cursor.execute("SELECT count(*) FROM {} WHERE k = ?".format(self._table_name()),
                       (k,))
        row = cursor.fetchone()
        if row[0] > 0:
            return True
        else:
            return False
