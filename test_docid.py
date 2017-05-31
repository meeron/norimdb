"""DocId test module"""

import pytest

from norimdb import DocId


class TestDocId:
    """DocId tests"""

    def test_new_id(self):
        """Test new id creation"""
        doc_id = DocId()
        assert len(doc_id) == DocId.SIZE

    def test_id_from_bytes(self):
        """Test creating id from bytes object"""
        doc_id = DocId()
        new_doc_id = DocId(doc_id)
        assert doc_id == new_doc_id

    def test_id_from_bytes_fail_type(self):
        """Test creating id from invalid type"""
        with pytest.raises(TypeError) as err:
            DocId("test")

    def test_id_from_bytes_fail_length(self):
        """Test creating id from bytes with invalid length"""
        with pytest.raises(OverflowError) as err:
            DocId(b'test')
