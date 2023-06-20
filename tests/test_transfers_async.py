#!/usr/bin/env python
import collections
import unittest
from unittest import mock

import vcr

from transfers import models
from transfers import transfer_async


AM_URL = "http://127.0.0.1:62080"
SS_URL = "http://127.0.0.1:62081"
USER = "test"
API_KEY = "test"
SS_USER = "test"
SS_KEY = "test"

TS_LOCATION_UUID = "8107e8d1-c390-4fe2-8d51-30f89ee2e4d4"
PATH_PREFIX = b"SampleTransfers"
DEPTH = 1
COMPLETED = set()
FILES = False


class TestAutomateTransfers(unittest.TestCase):
    def setUp(self):
        models.init_session(databasefile=":memory:")

        # Setup some test data.
        transfers_dir = (
            "/var/archivematica/sharedDirectory/watchedDirectories/" "activeTransfers"
        )
        Result = collections.namedtuple(
            "Result", "transfer_type target transfer_name transfer_abs_path"
        )
        self.start_tests = [
            Result(
                transfer_type="standard",
                target="standard_1",
                transfer_name="standard_1",
                transfer_abs_path="{}/standardTransfer/standard_1/".format(
                    transfers_dir
                ),
            ),
            Result(
                transfer_type="standard",
                target="standard_1",
                transfer_name="standard_1_1",
                transfer_abs_path="{}/standardTransfer/standard_1_1/".format(
                    transfers_dir
                ),
            ),
            Result(
                transfer_type="dspace",
                target="dspace_1.zip",
                transfer_name="dspace_1.zip",
                transfer_abs_path=f"{transfers_dir}/Dspace/dspace_1.zip",
            ),
            Result(
                transfer_type="dspace",
                target="dspace_1.zip",
                transfer_name="dspace_1_1.zip",
                transfer_abs_path=f"{transfers_dir}/Dspace/dspace_1_1.zip",
            ),
        ]

    @vcr.use_cassette(
        "fixtures/vcr_cassettes/" "test_transfer_async_api_create_package.yaml"
    )
    def test_call_start_transfer(self):
        """Provide an integration test as best as we can for transfer_async's
        _start_transfer method, which overrides that from the transfer module.
        """
        for test in self.start_tests:
            models.init_session(databasefile=":memory:")
            with mock.patch(
                "transfers.transfer_async.get_next_transfer"
            ) as mock_get_next_transfer:
                mock_get_next_transfer.return_value = test.target.encode()
                res = transfer_async._api_create_package(
                    am_url=AM_URL,
                    am_user=USER,
                    am_api_key=API_KEY,
                    name=test.transfer_name,
                    package_type=test.transfer_type,
                    accession=test.transfer_name,
                    ts_location_uuid=TS_LOCATION_UUID,
                    ts_path=test.target.encode(),
                    config_file="config.cfg",
                )
                with mock.patch(
                    "transfers.transfer_async._api_create_package"
                ) as mock_api_create_package:
                    mock_api_create_package.return_value = res
                    new_transfer = transfer_async._start_transfer(
                        ss_url=SS_URL,
                        ss_user=SS_USER,
                        ss_api_key=SS_KEY,
                        ts_location_uuid=TS_LOCATION_UUID,
                        ts_path="",
                        depth="test",
                        am_url=AM_URL,
                        am_user=USER,
                        am_api_key=API_KEY,
                        transfer_type="standard",
                        see_files=False,
                        config_file="config.cfg",
                    )
                    assert new_transfer.path.decode() == test.target
                    assert new_transfer.current is True
                    assert new_transfer.unit_type == "transfer"
                    # Make a secondary call to the database to see if we can
                    # retrieve our information. Obviously this should not have
                    # changed since we wrote it to memory.
                    unit = models.retrieve_unit_by_type_and_uuid(
                        new_transfer.uuid, "transfer"
                    )
                    assert unit.uuid == new_transfer.uuid
                    assert unit.current is True
                    assert unit.unit_type == "transfer"
