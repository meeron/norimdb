"""DocId test module"""

import pytest

from norimdb import DocId


class TestDocId:
    """DocId tests"""

    def test_new_id(self):
        """Test new id creation"""
        doc_id = DocId()
        assert doc_id.size() == DocId.SIZE

    def test_id_from_bytes(self):
        """Test creating id from bytes object"""
        doc_id = DocId()
        new_doc_id = DocId.from_bytes(doc_id.to_bytes())
        assert doc_id == new_doc_id

    def test_id_from_str(self):
        """Test creating id from string"""
        doc_id = DocId()
        new_doc_id = DocId.from_str(doc_id.to_str())
        assert doc_id == new_doc_id

    def test_id_from_bytes_fail_length(self):
        """Test creating id from bytes with invalid length"""
        with pytest.raises(OverflowError) as err:
            DocId.from_bytes(b'test')
