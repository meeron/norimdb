"""Module that defines DocIdClass"""

from os import urandom, getpid
from time import time
from struct import pack
from io import BytesIO
from binascii import hexlify, unhexlify


class DocId:
    """Document Id class"""

    SIZE = 8

    def __init__(self):
        with BytesIO() as buffer:
            # 1. 2 bytes random
            buffer.write(urandom(2))
            # 2. 4 bytes represents Unix time
            buffer.write(pack('I', int(time())))
            # 3. 2 bytes for PID
            buffer.write(pack('H', getpid()))
            self._buffer = buffer.getvalue()

    def __str__(self):
        return self.to_str()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._buffer == other._buffer
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
    
    def to_str(self):
        """String representation of DocId instance"""
        return hexlify(self._buffer)

    def to_bytes(self):
        """Get bytes representation of DocId instance"""
        return self._buffer

    def size(self):
        """Gets size of an buffer"""
        return len(self._buffer)

    @staticmethod
    def from_bytes(buffer):
        """Create document id based on bytes"""
        if len(buffer) != DocId.SIZE:
            raise OverflowError("Invalid buffer length")
        doc_id = DocId()
        doc_id._buffer = buffer
        return doc_id
    
    @staticmethod
    def from_str(str_buffer):
        """Create document id from string"""
        return DocId.from_bytes(unhexlify(str_buffer))
        