"""DocId test module"""

from binascii import hexlify
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
        new_doc_id = DocId(doc_id.to_bytes())
        assert doc_id == new_doc_id

    def test_id_to_string(self):
        """Test converting DocId to string"""
        buffer = b'12345678'
        doc_id = DocId(buffer)
        doc_id_str = str(doc_id)
        assert isinstance(doc_id_str, str)
        assert doc_id_str == hexlify(buffer).decode('utf8')

    def test_id_from_str(self):
        """Test creating id from string"""
        doc_id = DocId()
        new_doc_id = DocId(doc_id.to_str())
        assert doc_id == new_doc_id

    def test_id_from_invalid_str(self):
        with pytest.raises(OverflowError) as err:
            DocId("test")

    def test_id_from_bytes_fail_length(self):
        """Test creating id from bytes with invalid length"""
        with pytest.raises(OverflowError) as err:
            DocId(b'test')

    def test_id_from_invalid_type(self):
        with pytest.raises(TypeError) as err:
            DocId(123)
