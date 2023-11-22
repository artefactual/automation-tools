#!/usr/bin/env python
import os
import unittest
from pathlib import Path
from unittest import mock

import requests
from sqlalchemy import exc

from aips import create_dips_job
from tests.tests_helpers import TmpDir


SS_URL = "http://192.168.168.192:8000"
SS_USER_NAME = "test"
SS_API_KEY = "12883879c823f6e533738c12266bfe9f7316a672"
LOCATION_UUID = "e9a08ce2-4e8e-4e01-bdea-09d8d8deff8b"
ORIGIN_PIPELINE_UUID = "ad174753-6776-47e2-9a12-ac37837e5128"

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
TMP_DIR = os.path.join(THIS_DIR, ".tmp-create-dips-job")
OUTPUT_DIR = os.path.join(TMP_DIR, "output")
DATABASE_FILE = os.path.join(TMP_DIR, "aips.db")

AIP_FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "aip.tar"
AIP_CONTENT = b""
with open(AIP_FIXTURE_PATH, "rb") as f:
    AIP_CONTENT = f.read()

AIPS_JSON = {
    "meta": {
        "limit": 20,
        "next": None,
        "offset": 0,
        "previous": None,
        "total_count": 1,
    },
    "objects": [
        {
            "current_full_path": "/var/archivematica/sharedDirectory/www/AIPsStore/3ea4/65ac/ea0a/4a9c/a057/507e/794d/e332/test_B-3ea465ac-ea0a-4a9c-a057-507e794de332",
            "current_location": "/api/v2/location/e9a08ce2-4e8e-4e01-bdea-09d8d8deff8b/",
            "current_path": "3ea4/65ac/ea0a/4a9c/a057/507e/794d/e332/test_B-3ea465ac-ea0a-4a9c-a057-507e794de332",
            "origin_pipeline": "/api/v2/pipeline/ad174753-6776-47e2-9a12-ac37837e5128/",
            "package_type": "AIP",
            "status": "UPLOADED",
            "uuid": "3ea465ac-ea0a-4a9c-a057-507e794de332",
        },
    ],
}


class TestCreateDipsJob(unittest.TestCase):
    def setUp(self):
        self.args = {
            "ss_url": SS_URL,
            "ss_user": SS_USER_NAME,
            "ss_api_key": SS_API_KEY,
            "location_uuid": LOCATION_UUID,
            "origin_pipeline_uuid": ORIGIN_PIPELINE_UUID,
            "tmp_dir": TMP_DIR,
            "output_dir": OUTPUT_DIR,
            "database_file": DATABASE_FILE,
            "delete_local_copy": True,
            "upload_type": None,
            "pipeline_uuid": "",
            "cp_location_uuid": "",
            "ds_location_uuid": "",
            "shared_directory": "",
            "atom_url": "",
            "atom_email": "",
            "atom_password": "",
            "atom_slug": "",
            "rsync_target": "",
        }

    def test_filter_aips(self):
        """
        Test that AIPs without 'uuid' or 'current_location'
        or in a different location are filtered.
        """
        aips = [
            # Okay
            {
                "current_location": "/api/v2/location/e9a08ce2-4e8e-4e01-bdea-09d8d8deff8b/",
                "origin_pipeline": "/api/v2/pipeline/ad174753-6776-47e2-9a12-ac37837e5128/",
                "uuid": "0fef53b0-0573-4398-aa4f-ebf04fe711cf",
            },
            # Wrong location
            {
                "current_location": "/api/v2/location/5c1c87e0-7d11-4f39-8dda-182b3a45031f/",
                "origin_pipeline": "/api/v2/pipeline/ad174753-6776-47e2-9a12-ac37837e5128/",
                "uuid": "7636f290-0b02-4323-b4bc-bd1ed191aaea",
            },
            # Wrong pipeline
            {
                "current_location": "/api/v2/location/e9a08ce2-4e8e-4e01-bdea-09d8d8deff8b/",
                "origin_pipeline": "/api/v2/pipeline/88050c7f-36a3-4900-9294-5a0411d69303/",
                "uuid": "b9cd796c-2231-42e6-9cd1-0236d22958fa",
            },
            # Missing UUID
            {
                "current_location": "/api/v2/location/e9a08ce2-4e8e-4e01-bdea-09d8d8deff8b/",
                "origin_pipeline": "/api/v2/pipeline/ad174753-6776-47e2-9a12-ac37837e5128/",
            },
            # Missing location
            {
                "origin_pipeline": "/api/v2/pipeline/ad174753-6776-47e2-9a12-ac37837e5128/",
                "uuid": "6bbd3dee-b52f-476f-8136-bb3f0d025096",
            },
            # Missing pipeline
            {
                "current_location": "/api/v2/location/e9a08ce2-4e8e-4e01-bdea-09d8d8deff8b/",
                "uuid": "e6409b38-20e9-4739-bb4a-892f2fb300d3",
            },
        ]
        filtered_aips = create_dips_job.filter_aips(
            aips, LOCATION_UUID, ORIGIN_PIPELINE_UUID
        )
        assert filtered_aips == ["0fef53b0-0573-4398-aa4f-ebf04fe711cf"]

    def test_main_fail_db(self):
        """Test a fail when a database can't be created."""
        self.args["database_file"] = "/this/should/be/a/wrong/path/to.db"
        ret = create_dips_job.main(**self.args)
        assert ret == 1

    @mock.patch(
        "requests.request",
        side_effect=[mock.Mock(status_code=401, headers={}, spec=requests.Response)],
    )
    def test_main_fail_request(self, _request):
        """Test a fail when an SS connection can't be established."""
        with TmpDir(TMP_DIR):
            self.args["ss_api_key"] = "bad_api_key"
            ret = create_dips_job.main(**self.args)
            assert ret == 2

    @mock.patch(
        "requests.request",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": requests.structures.CaseInsensitiveDict(
                        {"Content-Type": "application/json"}
                    ),
                    "json.return_value": AIPS_JSON,
                },
                spec=requests.Response
            )
        ],
    )
    @mock.patch(
        "requests.get",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": {},
                    "iter_content.return_value": iter([AIP_CONTENT]),
                },
                spec=requests.Response
            ),
        ],
    )
    def test_main_success(self, _get, _request):
        """Test a success where one DIP is created."""
        with TmpDir(TMP_DIR), TmpDir(OUTPUT_DIR):
            # breakpoint()
            ret = create_dips_job.main(**self.args)
            assert ret is None
            dip_path = os.path.join(
                OUTPUT_DIR, "test_B-3ea465ac-ea0a-4a9c-a057-507e794de332"
            )
            assert os.path.isdir(dip_path)

    @mock.patch(
        "requests.request",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": requests.structures.CaseInsensitiveDict(
                        {"Content-Type": "application/json"}
                    ),
                    "json.return_value": AIPS_JSON,
                },
                spec=requests.Response
            )
        ],
    )
    @mock.patch(
        "requests.get",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": {},
                    "iter_content.return_value": iter([AIP_CONTENT]),
                },
                spec=requests.Response
            ),
        ],
    )
    def test_main_success_no_dip_creation(self, _get, _request):
        """Test a success where one AIP was already processed."""
        effect = exc.IntegrityError({}, [], "")
        session_add_patch = mock.patch("sqlalchemy.orm.Session.add", side_effect=effect)
        with TmpDir(TMP_DIR), TmpDir(OUTPUT_DIR), session_add_patch:
            ret = create_dips_job.main(**self.args)
            assert ret is None
            dip_path = os.path.join(
                OUTPUT_DIR, "test_B-3ea465ac-ea0a-4a9c-a057-507e794de332"
            )
            assert not os.path.isdir(dip_path)

    @mock.patch("aips.create_dips_job.atom_upload.main")
    @mock.patch("aips.create_dips_job.create_dip.main", return_value=1)
    @mock.patch(
        "requests.request",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": requests.structures.CaseInsensitiveDict(
                        {"Content-Type": "application/json"}
                    ),
                    "json.return_value": AIPS_JSON,
                },
                spec=requests.Response
            )
        ],
    )
    @mock.patch(
        "requests.get",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": {},
                    "iter_content.return_value": iter([AIP_CONTENT]),
                },
                spec=requests.Response
            ),
        ],
    )
    def test_main_dip_creation_failed(self, _get, _request, create_dip, atom_upload):
        """Test that a fail on DIP creation doesn't trigger an upload."""
        with TmpDir(TMP_DIR), TmpDir(OUTPUT_DIR):
            self.args["upload_type"] = "atom-upload"
            create_dips_job.main(**self.args)
            assert not atom_upload.called

    @mock.patch("aips.create_dips_job.atom_upload.main", return_value=None)
    @mock.patch("aips.create_dips_job.create_dip.main", return_value="fake/path")
    @mock.patch(
        "requests.request",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": requests.structures.CaseInsensitiveDict(
                        {"Content-Type": "application/json"}
                    ),
                    "json.return_value": AIPS_JSON,
                },
                spec=requests.Response
            )
        ],
    )
    @mock.patch(
        "requests.get",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": {},
                    "iter_content.return_value": iter([AIP_CONTENT]),
                },
                spec=requests.Response
            ),
        ],
    )
    def test_main_success_atom_upload_call(
        self, _get, _request, create_dip, atom_upload
    ):
        """Test that an upload to AtoM is performed."""
        with TmpDir(TMP_DIR), TmpDir(OUTPUT_DIR):
            self.args.update(
                {
                    "upload_type": "atom-upload",
                    "atom_url": "",
                    "atom_email": "",
                    "atom_password": "",
                    "atom_slug": "",
                    "rsync_target": "",
                    "delete_local_copy": True,
                }
            )
            create_dips_job.main(**self.args)
            assert atom_upload.called

    @mock.patch("aips.create_dips_job.storage_service_upload.main", return_value=None)
    @mock.patch("aips.create_dips_job.create_dip.main", return_value="fake/path")
    @mock.patch(
        "requests.request",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": requests.structures.CaseInsensitiveDict(
                        {"Content-Type": "application/json"}
                    ),
                    "json.return_value": AIPS_JSON,
                },
                spec=requests.Response
            )
        ],
    )
    @mock.patch(
        "requests.get",
        side_effect=[
            mock.Mock(
                **{
                    "status_code": 200,
                    "headers": {},
                    "iter_content.return_value": iter([AIP_CONTENT]),
                },
                spec=requests.Response
            ),
        ],
    )
    def test_main_success_ss_upload_call(self, _get, _request, create_dip, ss_upload):
        """Test that an upload to AtoM is performed."""
        with TmpDir(TMP_DIR), TmpDir(OUTPUT_DIR):
            self.args.update(
                {
                    "upload_type": "ss-upload",
                    "pipeline_uuid": "",
                    "cp_location_uuid": "",
                    "ds_location_uuid": "",
                    "shared_directory": "",
                    "delete_local_copy": True,
                }
            )
            create_dips_job.main(**self.args)
            assert ss_upload.called
