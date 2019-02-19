from ._sqlite3 import SQLite3Adaptor as _SQLite3Adaptor
import gzip

SQLITE3 = _SQLite3Adaptor


class KVStorage:
    @staticmethod
    def _compress(b):
        return gzip.compress(b)

    @staticmethod
    def _decompress(b):
        return gzip.decompress(b)

    def __init__(self, database, table, key_type=str, value_type=str,
                 database_type=SQLITE3, compress_value=False, **kwargs):
        self._key_type = key_type
        self._value_type = value_type
        self._compress_value = compress_value
        if self._compress_value:
            if self._value_type not in (str, bytes):
                raise TypeError("only str and bytes values are supported for "
                                "compress function")
            value_type = bytes
        kwargs.update({"database": database,
                       "table": table,
                       "key_type": key_type,
                       "value_type": value_type})
        self._database_adaptor = database_type(**kwargs)

    def _check_key_type(self, k):
        if not isinstance(k, self._key_type):
            raise TypeError("mismatched key type: {}, excepted: {}"
                             .format(type(k), self._key_type))

    def _check_value_type(self, v):
        if not isinstance(v, self._value_type):
            raise TypeError("mismatched value type: {}, excepted: {}"
                             .format(type(v), self._value_type))

    def put(self, k, v):
        self._check_key_type(k)
        self._check_value_type(v)
        if self._compress_value:
            v = self._compress(v) \
                if self._value_type == bytes \
                else self._compress(v.encode())
        self._database_adaptor.put(k, v)

    def get(self, k):
        self._check_key_type(k)
        v = self._database_adaptor.get(k)
        if self._compress_value:
            v = self._decompress(v)
            if self._value_type == str:
                v = v.decode()
        return v

    def delete(self, k):
        self._check_key_type(k)
        self._database_adaptor.delete(k)

    def contains(self, k):
        self._check_key_type(k)
        return self._database_adaptor.contains(k)
