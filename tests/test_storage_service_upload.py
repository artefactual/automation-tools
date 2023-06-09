#!/usr/bin/env python
import os
import shutil
import unittest
from unittest import mock

import vcr

from dips import storage_service_upload

SS_URL = "http://localhost:62081"
SS_USER_NAME = "test"
SS_API_KEY = "test"
PIPELINE_UUID = "7c12fdc6-8a07-499d-94ba-60e8d93bb775"
CP_LOCATION_UUID = "e0b81974-1614-4f41-a490-c153e9d30177"
DS_LOCATION_UUID = "7537c1a8-a3d3-4de7-a3ba-f6e6d4aa25c6"
SHARED_DIRECTORY = "/home/radda/.am/am-pipeline-data/"
DIP_PATH = "/tmp/fake_DIP"
AIP_UUID = "2942ac09-d55e-426b-84d3-0def52739791"


class TestSsUpload(unittest.TestCase):
    @mock.patch("dips.storage_service_upload.os.path.exists", return_value=True)
    def test_dip_folder_exists(self, mock_path_exists):
        ret = storage_service_upload.main(
            ss_url=SS_URL,
            ss_user=SS_USER_NAME,
            ss_api_key=SS_API_KEY,
            pipeline_uuid=PIPELINE_UUID,
            cp_location_uuid=CP_LOCATION_UUID,
            ds_location_uuid=DS_LOCATION_UUID,
            shared_directory=SHARED_DIRECTORY,
            dip_path=DIP_PATH,
            aip_uuid=AIP_UUID,
            delete_local_copy=True,
        )
        assert ret == 1

    @mock.patch(
        "dips.storage_service_upload.shutil.copytree", side_effect=shutil.Error("")
    )
    @mock.patch("dips.storage_service_upload.os.makedirs")
    def test_dip_folder_copy_fail(self, mock_makedirs, mock_copytree):
        ret = storage_service_upload.main(
            ss_url=SS_URL,
            ss_user=SS_USER_NAME,
            ss_api_key=SS_API_KEY,
            pipeline_uuid=PIPELINE_UUID,
            cp_location_uuid=CP_LOCATION_UUID,
            ds_location_uuid=DS_LOCATION_UUID,
            shared_directory=SHARED_DIRECTORY,
            dip_path=DIP_PATH,
            aip_uuid=AIP_UUID,
            delete_local_copy=True,
        )
        assert ret == 2

    @vcr.use_cassette(
        "fixtures/vcr_cassettes/test_storage_service_upload_request_fail.yaml"
    )
    @mock.patch("dips.storage_service_upload.shutil.copytree")
    @mock.patch("dips.storage_service_upload.os.makedirs")
    def test_request_fail(self, mock_makedirs, mock_copytree):
        ret = storage_service_upload.main(
            ss_url=SS_URL,
            ss_user=SS_USER_NAME,
            ss_api_key="fake_api_key",
            pipeline_uuid=PIPELINE_UUID,
            cp_location_uuid=CP_LOCATION_UUID,
            ds_location_uuid=DS_LOCATION_UUID,
            shared_directory=SHARED_DIRECTORY,
            dip_path=DIP_PATH,
            aip_uuid=AIP_UUID,
            delete_local_copy=True,
        )
        assert ret == 3

    @vcr.use_cassette("fixtures/vcr_cassettes/test_storage_service_upload_success.yaml")
    @mock.patch("dips.atom_upload.shutil.rmtree")
    @mock.patch("dips.storage_service_upload.shutil.copytree")
    @mock.patch("dips.storage_service_upload.os.makedirs")
    def test_success(self, mock_makedirs, mock_copytree, mock_rmtree):
        ret = storage_service_upload.main(
            ss_url=SS_URL,
            ss_user=SS_USER_NAME,
            ss_api_key=SS_API_KEY,
            pipeline_uuid=PIPELINE_UUID,
            cp_location_uuid=CP_LOCATION_UUID,
            ds_location_uuid=DS_LOCATION_UUID,
            shared_directory=SHARED_DIRECTORY,
            dip_path=DIP_PATH,
            aip_uuid=AIP_UUID,
            delete_local_copy=True,
        )
        assert ret == 0
        upload_dip_path = os.path.join(
            SHARED_DIRECTORY,
            "watchedDirectories",
            "automationToolsDIPs",
            os.path.basename(DIP_PATH),
        )
        mock_rmtree.assert_has_calls([mock.call(upload_dip_path), mock.call(DIP_PATH)])
