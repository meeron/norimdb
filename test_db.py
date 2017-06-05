"""NorimDb test module"""

import pytest
from tempfile import TemporaryDirectory
from os import path

from norimdb import NorimDb, DbError


class TestNorimDb:
    """NorimDb test"""

    def test_open_success(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                pass
            assert path.isfile(path.join(tempdir, 'sys.ndb'))
            assert path.isfile(path.join(tempdir, 'data.ndb'))

    def test_open_fail(self):
        with pytest.raises(DbError) as err:
            NorimDb("/test/test")

    def test_add_get_value(self):
        doc = {
            'name': "test"
        }

        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                collection.add(doc)
                assert '_id' in doc
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                doc_db = collection.get(doc['_id'])
                assert doc['name'] == doc_db['name']
                assert doc['_id'] == doc_db['_id']

    def test_add_value_to_closed_db(self):
        with TemporaryDirectory() as tempdir:
            collection = None
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
            with pytest.raises(DbError) as err:
                collection.add({})
