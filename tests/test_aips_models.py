#!/usr/bin/env python
import os
import unittest

from aips import models
from tests.tests_helpers import TmpDir

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
TMP_DIR = os.path.join(THIS_DIR, '.tmp-aips-models')
DATABASE_FILE = os.path.join(TMP_DIR, 'aips.db')


class TestAipsModels(unittest.TestCase):
    def test_init_success(self):
        """Test that the database, table and session are created."""
        assert not os.path.isfile(DATABASE_FILE)
        assert not hasattr(models, 'Session')
        with TmpDir(TMP_DIR):
            session = models.init(DATABASE_FILE)
            assert os.path.isfile(DATABASE_FILE)
            assert 'aip' in models.Base.metadata.tables
            assert hasattr(session, 'add')
            assert callable(getattr(session, 'add'))

    def test_init_fail(self):
        """Test that the database can't be created in a wrong path."""
        self.assertRaises(
            IOError,
            models.init,
            '/this/should/be/a/wrong/path/to.db'
        )
