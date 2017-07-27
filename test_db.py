"""NorimDb test module"""

import pytest
from tempfile import TemporaryDirectory
from os import path

from norimdb import NorimDb, DbError, DocId


class TestNorimDb:
    """NorimDb test"""

    def test_open_success(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                pass
            assert path.isfile(path.join(tempdir, 'storage.ndb'))

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
                assert isinstance(doc_db['_id'], DocId)

    def test_remove_collection(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                collection.add({'name': 'test'})
            with NorimDb(tempdir) as db:
                db.remove_collection('test')

    def test_add_value_to_closed_db(self):
        with TemporaryDirectory() as tempdir:
            collection = None
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
            with pytest.raises(DbError) as err:
                collection.add({})

    def test_remove(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                obj = {'name': "test"}
                collection.add(obj)
                count = collection.remove(obj['_id'])
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                objdb = collection.get(obj['_id'])
                assert count > 0
                assert objdb is None

    def test_update_whole_document(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                obj = {'name': "test"}
                collection.add(obj)
                obj_new = {'name': "test_new", 'age': 66}
                count = collection.set(obj['_id'], obj_new)
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                objdb = collection.get(obj['_id'])
                assert count > 0
                assert objdb['name'] == obj_new['name']
                assert 'age' in objdb

    def test_update_single_fields(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                obj = {'name': "test"}
                new_name = "test123"
                collection.add(obj)
                count = collection.update(obj['_id'], name=new_name)
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                objdb = collection.get(obj['_id'])
                assert count > 0
                assert objdb['name'] == new_name

    def test_set_document(self):
         with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                obj = {'name': "test"}
                obj_new = {'name': "test_new", 'age': 66}
                collection.add(obj)
                collection.set(obj['_id'], obj_new)
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                objdb = collection.get(obj['_id'])
                assert objdb['name'] == obj_new['name']
                assert 'age' in objdb

    def test_get_simple_query(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                collection.add({'age': 66, 'name': "test1"})
                collection.add({'age': 66, 'name': "test2"})
                collection.add({'age': 123, 'name': "test3"})
                result = collection.find({
                    'age': 66,
                    'name': "test2"
                })
                assert len(result) == 1
                assert isinstance(result[0]['_id'], DocId)

    def test_get_compare_query(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                collection.add({'age': 66, 'name': "test1"})
                collection.add({'age': 70, 'name': "test2"})
                collection.add({'age': 123, 'name': "test3"})
                result = collection.find({
                    'age': {'$lt':100, '$gt': 67}
                })
                assert len(result) == 1
                assert result[0]['name'] == "test2"

    def test_get_or_query(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                collection.add({'age': 66, 'name': "test1"})
                collection.add({'age': 70, 'name': "test2"})
                collection.add({'age': 123, 'name': "test3"})
                result = collection.find({
                    '$or': ({'age':66}, {'name': "test2"})
                })
                assert len(result) == 2

    def test_get_in_query(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                collection.add({'age': 66, 'name': "test1"})
                collection.add({'age': 70, 'name': "test2"})
                collection.add({'age': 123, 'name': "test3"})
                result = collection.find({
                    'age': {'$in':(66, 123)}
                })
                assert len(result) == 2

    def test_get_sort_asc_query(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                collection.add({'age': 25, 'name': "test1"})
                collection.add({'age': 15, 'name': "test2"})
                collection.add({'age': 17, 'name': "test3"})
                result = collection.find({}, 'age')
                assert result[0]['name'] == "test2"
                assert result[1]['name'] == "test3"
                assert result[2]['name'] == "test1"

    def test_get_sort_desc_query(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                collection.add({'age': 17, 'name': "test1"})
                collection.add({'age': 25, 'name': "test2"})
                collection.add({'age': 15, 'name': "test3"})
                result = collection.find({}, 'age desc')
                assert result[0]['name'] == "test2"
                assert result[1]['name'] == "test1"
                assert result[2]['name'] == "test3"

    def test_add_batch(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                data_batch = [
                    {'age': 66, 'name': "test"},
                    {'age': 66, 'name': "test"},
                    {'age': 123, 'name': "test"}
                ]
                collection.add_batch(data_batch)
            with NorimDb(tempdir) as db:
                collection = db.get_collection("test")
                result = collection.find({'name': "test"})
                assert len(result) == 3
