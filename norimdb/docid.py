"""Module that defines DocIdClass"""

from os import urandom, getpid
from time import time
from struct import pack
from io import BytesIO
from binascii import hexlify, unhexlify, Error


class DocId:
    """Document Id class"""

    SIZE = 8

    def __init__(self, input_buffer=None):
        if input_buffer:
            if isinstance(input_buffer, bytes):
                self._from_bytes(input_buffer)
            elif isinstance(input_buffer, str):
                self._from_str(input_buffer)
            else:
                raise TypeError("Type %s is not supported when creating DocId object" % (type(input_buffer)))
        else:
            with BytesIO() as buffer:
                # 1. 2 bytes random
                buffer.write(urandom(2))
                # 2. 4 bytes represents Unix time
                buffer.write(pack('I', int(time())))
                # 3. 2 bytes for PID
                buffer.write(pack('H', getpid()))
                self._buffer = buffer.getvalue()
    
    def to_str(self):
        """String representation of DocId instance"""
        return hexlify(self._buffer).decode('utf8')

    def to_bytes(self):
        """Get bytes representation of DocId instance"""
        return self._buffer

    def size(self):
        """Gets size of an buffer"""
        return len(self._buffer)

    def _from_bytes(self, buffer):
        if len(buffer) != DocId.SIZE:
            raise OverflowError("Invalid buffer length")
        self._buffer = buffer

    def _from_str(self, str_buffer):
        try:
            self._from_bytes(unhexlify(str_buffer.encode('utf8')))
        except Error as binascii_err:
            raise OverflowError(binascii_err)
        except OverflowError as overflow_err:
            raise overflow_err

    def __str__(self):
        return self.to_str()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._buffer == other._buffer
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
