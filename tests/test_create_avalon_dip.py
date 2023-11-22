#!/usr/bin/env python
import csv
import os
from pathlib import Path
from unittest import mock

import amclient
import requests

from aips import create_dip


SS_URL = "http://127.0.0.1:62081"
SS_USER_NAME = "test"
SS_API_KEY = "test"

AVALON_AIP_UUID = "5b7144a2-5eb1-461c-83db-ade2a14b4209"
TRANSFER_NAME = "AvalonCollection"
AIP_FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "avalon-aip.tar"
AIP_CONTENT = b""
with open(AIP_FIXTURE_PATH, "rb") as f:
    AIP_CONTENT = f.read()


@mock.patch(
    "requests.get",
    side_effect=[
        mock.Mock(
            **{
                "status_code": 200,
                "headers": {},
                "iter_content.return_value": iter([AIP_CONTENT]),
            },
            spec=requests.Response,
        ),
    ],
)
def test_create_avalon_dip_success(_get, tmp_path):
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
    # Create temporary directories.
    tmp_dir = tmp_path / "dir"
    tmp_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Download the AIP first
    aip_path = amclient.AMClient(
        aip_uuid=AVALON_AIP_UUID,
        ss_url=SS_URL,
        ss_user_name=SS_USER_NAME,
        ss_api_key=SS_API_KEY,
        directory=tmp_dir.as_posix(),
    ).download_aip()
    # Extract it
    aip_dir = create_dip.extract_aip(aip_path, AVALON_AIP_UUID, tmp_dir.as_posix())
    # Test DIP creation
    avalon_dip_dir = create_dip.create_dip(
        aip_dir, AVALON_AIP_UUID, output_dir.as_posix(), "atom", "avalon-manifest"
    )
    # Check DIP structure
    assert avalon_dip_dir == "{}/{}/{}".format(
        output_dir.as_posix(), TRANSFER_NAME, AVALON_AIP_UUID
    )
    assert os.path.isdir(avalon_dip_dir)

    # Check that CSV and folder are present, and METS file is removed
    assert sorted(os.listdir(avalon_dip_dir)) == sorted(["Demo_Manifest.csv", "assets"])

    # Check contents of CSV have been updated
    csv_path = f"{avalon_dip_dir}/Demo_Manifest.csv"
    is_in_file = False
    with open(csv_path) as c:
        demo_manifest = csv.reader(c, delimiter=",")
        for row in demo_manifest:
            if AVALON_AIP_UUID in row:
                is_in_file = True
    assert is_in_file

    # Check that files are present
    avalon_files = os.listdir(f"{avalon_dip_dir}/assets")
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
