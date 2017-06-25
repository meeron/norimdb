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

    # TODO: add batch
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

    def remove(self, docid):
        """Remove entry by id"""
        self._ensure_collection()
        query = "DELETE FROM {} WHERE key=?".format(self._name)
        cursor = self._conn.cursor()
        cursor.execute(query, (docid.to_bytes(),))
        count = cursor.rowcount
        cursor.close()
        return count

    def update(self, docid, **kwargs):
        """Update fields on an entry"""
        self._ensure_collection()
        obj = self.get(docid)
        if obj:
            for key in kwargs:
                if key == '_id':
                    continue
                obj[key] = kwargs[key]
            return self.update_all(docid, obj)
        return 0

    def update_all(self, docid, obj):
        """Update whole entry"""
        self._ensure_collection()
        query = "UPDATE {} SET value=? WHERE key=?".format(self._name)
        cursor = self._conn.cursor()
        obj['_id'] = obj['_id'].to_bytes()
        cursor.execute(query, (
            pybinn.dumps(obj),
            obj['_id']
        ))
        count = cursor.rowcount
        self._conn.commit()
        cursor.close()
        obj['_id'] = DocId.from_bytes(obj['_id'])
        return count

    def find(self, query_dict, sort=None):
        """Find entries by query"""
        self._ensure_collection()
        query = "SELECT * FROM {}".format(self._name)
        cursor = self._conn.cursor()

        result = []
        for row in cursor.execute(query):
            obj = pybinn.loads(row[1])
            query_result = Collection._check_obj(query_dict, obj)
            if query_result == len(query_dict):
                result.append(obj)

        if sort is not None and isinstance(sort, str):
            sort_list = sort.split()
            sort_list_len = len(sort_list)
            if sort_list_len > 0:
                sort_field = sort_list[0]
                result.sort(key=lambda o: o[sort_field] if sort_field in o else b'\xff')
            if sort_list_len > 1 and sort_list[1] == 'desc':
                result.reverse()

        return result

    @staticmethod
    def _check_obj(query_dict, obj):
        query_result = 0
        for field in query_dict:
            if field == '$or' and isinstance(query_dict[field], tuple):
                or_query = query_dict[field]
                or_result = 0
                for sub_query in or_query:
                    or_result += Collection._check_obj(sub_query, obj)
                if or_result > 0:
                    query_result += 1
            if field in obj and isinstance(query_dict[field], dict):
                inner = query_dict[field]
                if '$lt' in inner and obj[field] < inner['$lt']:
                    query_result += 1 / len(inner)
                if '$gt' in inner and obj[field] > inner['$gt']:
                    query_result += 1 / len(inner)
                if '$in' in inner and isinstance(inner['$in'], tuple) and obj[field] in inner['$in']:
                    query_result += 1
            if field in obj and obj[field] == query_dict[field]:
                query_result += 1
        return query_result

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
        