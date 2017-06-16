"""Module defines NorimDb class"""

from os import path
import sqlite3
import pybinn

from .exceptions import *
from .docid import DocId


class NorimDb:
    """NorimDb class"""

    def __init__(self, dir_path):
        if not path.isdir(dir_path):
            raise DbError(ERR_PATH, path=dir_path)
        self._conn = sqlite3.connect(path.join(dir_path, "storage.ndb"))
        self.opened = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def get_collection(self, name):
        """Gets the collection"""
        return Collection(name, self._conn, self)

    def close(self):
        """Close database"""
        self._conn.close()
        self.opened = False


class Collection:
    """Collection class"""

    def __init__(self, name: str, conn: sqlite3.Connection, db: NorimDb):
        if name[0] == '_':
            raise DbError(ERR_COL_NAME, name=name)
        self._name = name
        self._conn = conn
        self._db = db

    def add(self, dict_value: dict):
        """Add value to collection"""
        if not isinstance(dict_value, dict):
            raise DbError(ERR_DOC_TYPE)
        self._ensure_collection() 

        id_bytes = DocId().to_bytes()
        dict_value['_id'] = id_bytes
        data = pybinn.dumps(dict_value)
        dict_value['_id'] = DocId.from_bytes(id_bytes)

        cursor = self._conn.cursor()
        query = "INSERT INTO {} VALUES(?, ?)".format(self._name)
        cursor.execute(query, (id_bytes, data))
        self._conn.commit()
        cursor.close()

    def get(self, doc_id):
        """Gets an item by id"""
        self._ensure_collection()
        query = "SELECT * FROM {} WHERE key=?".format(self._name)
        cursor = self._conn.cursor()
        cursor.execute(query, (doc_id.to_bytes(),))
        row = cursor.fetchone()
        cursor.close()

        if row:
            obj = pybinn.loads(row[1])
            obj['_id'] = DocId.from_bytes(obj['_id'])
            return obj
        return None

    def _ensure_collection(self):
        if not self._db.opened: 
            raise DbError(ERR_DB_CLOSED)
        cursor = self._conn.cursor()
        query = "CREATE TABLE IF NOT EXISTS {name}(key BLOB PRIMARY KEY, value BLOB)".format(
            name=self._name
        )
        cursor.execute(query)
        self._conn.commit()
        cursor.close()
        