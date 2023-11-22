#!/usr/bin/env python
import os

import pytest

from aips import models


def test_init_success(tmp_path):
    """Test that the database, table and session are created."""
    tmp_dir = tmp_path / "dir"
    tmp_dir.mkdir()

    DATABASE_FILE = (tmp_dir / "aips.db").as_posix()

    assert not os.path.isfile(DATABASE_FILE)
    assert not hasattr(models, "Session")

    session = models.init(DATABASE_FILE)

    assert os.path.isfile(DATABASE_FILE)
    assert "aip" in models.Base.metadata.tables
    assert hasattr(session, "add")
    assert callable(session.add)


def test_init_fail():
    """Test that the database can't be created in a wrong path."""
    with pytest.raises(IOError):
        models.init("/this/should/be/a/wrong/path/to.db")
