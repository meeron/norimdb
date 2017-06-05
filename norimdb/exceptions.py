"""Module defines user typed exceptions"""

ERR_PATH = 1001
ERR_DB_CLOSED = 1002
ERR_COL_NAME = 1101
ERR_COL_KEY = 1102
ERR_DOC_TYPE = 1201


def _create_msg(code, **kwargs):
    if code == ERR_PATH:
        return "Invalid database path ('{path}')".format(**kwargs)
    if code == ERR_DB_CLOSED:
        return "Cannot perform action on closed database"
    if code == ERR_COL_NAME:
        return "Invalid collection name ('{name}')".format(**kwargs)
    if code == ERR_COL_KEY:
        return "Duplicate key '{key}' in collection '{collection}'".format(**kwargs)
    if code == ERR_DOC_TYPE:
        return "Invalid item type. Only dictionaries are supported"
    return "Unknown error type"


class DbError(Exception):
    def __init__(self, err_code, **kwargs):
        super().__init__(_create_msg(err_code, **kwargs))
        self.code = err_code
