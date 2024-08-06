#!/usr/bin/env python
import collections
from unittest import mock

import pytest

from transfers import models
from transfers import transfer_async

AM_URL = "http://127.0.0.1:62080"
SS_URL = "http://127.0.0.1:62081"
USER = "test"
API_KEY = "test"
SS_USER = "test"
SS_KEY = "test"

TS_LOCATION_UUID = "8107e8d1-c390-4fe2-8d51-30f89ee2e4d4"
TRANSFERS_DIR = "/var/archivematica/sharedDirectory/watchedDirectories/activeTransfers"
Result = collections.namedtuple(
    "Result", "transfer_type target transfer_name transfer_abs_path"
)


@pytest.mark.parametrize(
    "fixture",
    [
        Result(
            transfer_type="standard",
            target="standard_1",
            transfer_name="standard_1",
            transfer_abs_path=f"{TRANSFERS_DIR}/standardTransfer/standard_1/",
        ),
        Result(
            transfer_type="standard",
            target="standard_1",
            transfer_name="standard_1_1",
            transfer_abs_path=f"{TRANSFERS_DIR}/standardTransfer/standard_1_1/",
        ),
        Result(
            transfer_type="dspace",
            target="dspace_1.zip",
            transfer_name="dspace_1.zip",
            transfer_abs_path=f"{TRANSFERS_DIR}/Dspace/dspace_1.zip",
        ),
        Result(
            transfer_type="dspace",
            target="dspace_1.zip",
            transfer_name="dspace_1_1.zip",
            transfer_abs_path=f"{TRANSFERS_DIR}/Dspace/dspace_1_1.zip",
        ),
    ],
)
@mock.patch(
    "requests.post",
    side_effect=[
        mock.Mock(
            **{
                "status_code": 202,
                "json.return_value": {"id": "06c28572-4506-48ba-904e-07938dabdb7d"},
            }
        ),
        mock.Mock(
            **{
                "status_code": 202,
                "json.return_value": {"id": "f8be5d0f-0692-4025-829c-9a029b96087e"},
            }
        ),
        mock.Mock(
            **{
                "status_code": 202,
                "json.return_value": {"id": "fb6adb0f-8e6b-4f08-822f-00ec0311576b"},
            }
        ),
        mock.Mock(
            **{
                "status_code": 202,
                "json.return_value": {"id": "15c585ca-c28d-4d2d-9ad2-72fe7d49fdd0"},
            }
        ),
    ],
)
def test_call_start_transfer(_post, fixture):
    """Provide an integration test as best as we can for transfer_async's
    _start_transfer method, which overrides that from the transfer module.
    """
    models.init_session(databasefile=":memory:")
    with mock.patch(
        "transfers.transfer_async.get_next_transfer"
    ) as mock_get_next_transfer:
        mock_get_next_transfer.return_value = fixture.target.encode()
        res = transfer_async._api_create_package(
            am_url=AM_URL,
            am_user=USER,
            am_api_key=API_KEY,
            name=fixture.transfer_name,
            package_type=fixture.transfer_type,
            accession=fixture.transfer_name,
            ts_location_uuid=TS_LOCATION_UUID,
            ts_path=fixture.target.encode(),
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
            assert new_transfer.path.decode() == fixture.target
            assert new_transfer.current is True
            assert new_transfer.unit_type == "transfer"
            # Make a secondary call to the database to see if we can
            # retrieve our information. Obviously this should not have
            # changed since we wrote it to memory.
            unit = models.retrieve_unit_by_type_and_uuid(new_transfer.uuid, "transfer")
            assert unit.uuid == new_transfer.uuid
            assert unit.current is True
            assert unit.unit_type == "transfer"
