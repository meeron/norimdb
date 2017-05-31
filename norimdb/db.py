"""Module defines NorimDb class"""

from os import path


class NorimDb:
    """NorimDb class"""

    def __init__(self, dir_path):
        if not path.isdir(dir_path):
            raise NotADirectoryError("Path '{}' does not exists or not directory.")

        self._sys_file = NorimDb._open(path.join(dir_path, "sys.ndb"))
        self._data_file = NorimDb._open(path.join(dir_path, "data.ndb"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        self._sys_file.close()
        self._sys_file.close()

    @staticmethod
    def _open(file_path):
        if path.isfile(file_path):
            return open(file_path, 'r+b')
        return open(file_path, 'w+b')
