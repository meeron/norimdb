"""Simple file document database"""

__version__ = '0.9.3'
__all__ = [
    'DocId', 'NorimDb',
    'DbError'
]

__author__ = 'Miron Jakubowski <mijakubowski@gmail.com>'

from .docid import DocId
from .db import NorimDb
from .exceptions import DbError
