"""Module defines NorimDb class"""

from os import path, SEEK_END

from .exceptions import *
import pybinn
from .docid import DocId


class NorimDb:
    """NorimDb class"""

    def __init__(self, dir_path):
        if not path.isdir(dir_path):
            raise DbError(ERR_PATH, path=dir_path)

        self._sys = {
            '_sys': {'size': 0}
        }

        self._sys_file = NorimDb._open(path.join(dir_path, "sys.ndb"))
        self._data_file = NorimDb._open(path.join(dir_path, "data.ndb"))

        self._sys_file.seek(0, SEEK_END)
        file_size = self._sys_file.tell()

        if file_size > 0:
            self._sys_file.seek(0)
            self._sys = pybinn.load(self._sys_file)

        self._opened = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def get_collection(self, name):
        return Collection(name, self)

    def close(self):
        self._sys_file.close()
        self._data_file.close()
        self._opened = False

    def _sync(self):
        self._sys_file.seek(0)
        pybinn.dump(self._sys, self._sys_file)
        print(pybinn.dumps(self._sys))
        print(self._sys)

    @staticmethod
    def _open(file_path):
        if path.isfile(file_path):
            return open(file_path, 'r+b')
        return open(file_path, 'w+b')


class Collection:
    """Collection class"""

    def __init__(self, name: str, db: NorimDb):
        if name[0] == '_':
            raise DbError(ERR_COL_NAME, name=name)
        self._collection = db._sys.get(name, {
            'sys': {'size': 0, 'count': 0},
            'keys': {}
        })
        db._sys[name] = self._collection
        self._name = name
        self._db = db

    def add(self, dict_value: dict):
        if not self._db._opened:
            raise DbError(ERR_DB_CLOSED)
        if not isinstance(dict_value, dict):
            raise DbError(ERR_DOC_TYPE)

        if '_id' not in dict_value:
            dict_value['_id'] = DocId()
        if dict_value['_id'] in self._collection['keys']:
            raise DbError(ERR_COL_KEY, key=dict_value['_id'], collection=self._name)

        self._collection['sys']['count'] += 1
        self._collection['keys'][dict_value['_id']] = {
            'offset': self._db._data_file.tell(),
            'size': 0
        }

        pybinn.dump(dict_value, self._db._data_file)
        self._db._sync()

    def get(self, doc_id):
        if not self._db._opened:
            raise DbError(ERR_DB_CLOSED)
        if doc_id not in self._collection['keys']:
            return None
        offset = self._collection['keys'][doc_id]['offset']
        self._db._data_file.seek(offset)
        return pybinn.load(self._db._data_file)
