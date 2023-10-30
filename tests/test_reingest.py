#!/usr/bin/env python
import os

import pytest

from transfers import reingest
from transfers import reingestmodel as reingestunit


class TestReingestClass:
    dbpath = "fixtures/reingest_test.db"

    @pytest.fixture(autouse=True)
    def setup_session(self):
        reingestunit.init(self.dbpath)
        self.session = reingestunit.Session()
        yield
        os.remove(self.dbpath)

    @pytest.mark.parametrize(
        "aip_uuids, expected",
        [
            (
                {
                    "54369f6a-aa82-4b29-80c9-834d3625397d",
                    "b18801dd-30ec-46ba-ac6b-4cb561585ac9",
                    "b4d37c2c-df30-4a16-8f2f-4cb02a5d53cb",
                },
                True,
            ),
            (
                [
                    "767dcf65-5a28-4ce4-bec2-a2a31e099ad0",
                    "e634c8e7-8105-46d4-a9e2-9ccd202a64c6",
                    "e27cf619-fe07-4e1a-b3ee-f1ed12899b6e",
                ],
                True,
            ),
            (
                (
                    "35c88dbd-664c-4261-9524-61e095de4009",
                    "43e50d92-486d-4a0c-a451-fc0572feb9f4",
                    "e3bcee81-87c0-4e12-8284-66c23f9619bd",
                ),
                True,
            ),
            ("A string", False),
            (0.12345, False),
            (1234, False),
        ],
    )
    def test_load_db_iterable(self, aip_uuids, expected):
        assert reingest.load_db(self.session, aip_uuids) == expected
