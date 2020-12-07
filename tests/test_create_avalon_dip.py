#!/usr/bin/env python
import csv
import os
import unittest
import vcr

import amclient

from aips import create_dip
from tests.tests_helpers import TmpDir


SS_URL = "http://127.0.0.1:62081"
SS_USER_NAME = "test"
SS_API_KEY = "test"

AVALON_AIP_UUID = "5b7144a2-5eb1-461c-83db-ade2a14b4209"
TRANSFER_NAME = "AvalonCollection"
THIS_DIR = os.path.abspath(os.path.dirname(__file__))
TMP_DIR = os.path.join(THIS_DIR, ".tmp-create-dip")
OUTPUT_DIR = os.path.join(TMP_DIR, "output")


class TestCreateAvalonDip(unittest.TestCase):
    @vcr.use_cassette(
        "fixtures/vcr_cassettes/test_create_avalon_dip_download_aip_success.yaml"
    )
    def test_create_avalon_dip_success(self):
        """
        Test full DIP creation:
            - AIP download
            - AIP extraction
            - DIP folder creation
            - METS file creation
            - Objects folder creation
            - ZIP file creation
            - Files inside ZIP created with original
            filename and lastmodified date
        """
        with TmpDir(TMP_DIR):
            # Download the AIP first
            aip_path = amclient.AMClient(
                aip_uuid=AVALON_AIP_UUID,
                ss_url=SS_URL,
                ss_user_name=SS_USER_NAME,
                ss_api_key=SS_API_KEY,
                directory=TMP_DIR,
            ).download_aip()
            # Extract it
            aip_dir = create_dip.extract_aip(aip_path, AVALON_AIP_UUID, TMP_DIR)
            # Test DIP creation
            avalon_dip_dir = create_dip.create_dip(
                aip_dir, AVALON_AIP_UUID, OUTPUT_DIR, "atom", "avalon-manifest"
            )
            # Check DIP structure
            assert avalon_dip_dir == "{}/{}/{}".format(
                OUTPUT_DIR, TRANSFER_NAME, AVALON_AIP_UUID
            )
            assert os.path.isdir(avalon_dip_dir)

            # Check that CSV and folder are present, and METS file is removed
            assert sorted(os.listdir(avalon_dip_dir)) == sorted(
                ["Demo_Manifest.csv", "assets"]
            )

            # Check contents of CSV have been updated
            csv_path = "{}/Demo_Manifest.csv".format(avalon_dip_dir)
            is_in_file = False
            with open(csv_path, "rt") as c:
                demo_manifest = csv.reader(c, delimiter=",")
                for row in demo_manifest:
                    if AVALON_AIP_UUID in row:
                        is_in_file = True
            assert is_in_file

            # Check that files are present
            avalon_files = os.listdir("{}/assets".format(avalon_dip_dir))
            assets = [
                "agz3068a.wav",
                "lunchroom_manners_512kb.mp4",
                "lunchroom_manners_512kb.mp4.structure.xml",
                "lunchroom_manners_512kb.mp4.vtt",
                "OrganClip.high.mp4",
                "OrganClip.low.mp4",
                "OrganClip.medium.mp4",
            ]
            assert sorted(assets) == sorted(avalon_files)
