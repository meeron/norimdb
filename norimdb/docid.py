"""Module that defines DocIdClass"""

from os import urandom, getpid
from time import time
from struct import pack

class DocId(bytes):
    """Document Id class"""
    
    SIZE = 8

    def __new__(cls, *args, **kargs):
        if len(args) > 0:
            if isinstance(args[0], bytes):
                if len(args[0]) != DocId.SIZE:
                    msg = "Invalid bytes length. Argument should have {} bytes.".format(DocId.SIZE)
                    raise OverflowError(msg)
                id_bytes = args[0]
            else:
                raise TypeError("Argument is not 'bytes' object")
        else:
            # 1. 2 bytes random
            rand = urandom(2)
            # 2. 4 bytes represents Unix time
            unix_time = pack('I', int(time()))
            # 3. 2 bytes for PID
            pid = pack('H', getpid())
            id_bytes = rand + unix_time + pid
        
        return bytes.__new__(cls, id_bytes, **kargs)
