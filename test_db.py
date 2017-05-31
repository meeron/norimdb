"""NorimDb test module"""

import pytest
from tempfile import TemporaryDirectory
from os import path

from norimdb import NorimDb


class TestNorimDb:
    """NorimDb test"""

    def test_open_success(self):
        with TemporaryDirectory() as tempdir:
            with NorimDb(tempdir) as db:
                pass
            assert path.isfile(path.join(tempdir, 'sys.ndb'))
            assert path.isfile(path.join(tempdir, 'data.ndb'))

    def test_open_fail(self):
        with pytest.raises(NotADirectoryError) as err:
            NorimDb("/test/test")
