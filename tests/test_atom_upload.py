#!/usr/bin/env python
import subprocess
from unittest import mock

import pytest
import requests

from dips import atom_upload

ATOM_URL = "http://192.168.168.193"
ATOM_EMAIL = "demo@example.com"
ATOM_PASSWORD = "demo"
ATOM_SLUG = "test"
RSYNC_TARGET = "192.168.168.193:/tmp"
DIP_PATH = "/tmp/fake_DIP"


def test_rsync_fail():
    effect = subprocess.CalledProcessError(1, [])
    with mock.patch("subprocess.check_output", side_effect=effect):
        with pytest.raises(subprocess.CalledProcessError):
            atom_upload.rsync(RSYNC_TARGET, DIP_PATH)


def test_rsync_success():
    with mock.patch("subprocess.check_output", return_value=None):
        ret = atom_upload.rsync(RSYNC_TARGET, DIP_PATH)

    assert ret is None


@mock.patch(
    "requests.request",
    side_effect=[mock.Mock(status_code=401, headers={}, spec=requests.Response)],
)
def test_deposit_fail(_request):
    with pytest.raises(Exception) as exc_info:
        atom_upload.deposit(
            ATOM_URL, "bad@email.com", ATOM_PASSWORD, ATOM_SLUG, DIP_PATH
        )
    assert str(exc_info.value) == "Response status code not expected"


@mock.patch(
    "requests.request",
    side_effect=[
        mock.Mock(
            status_code=302, headers={"Location": "/test"}, spec=requests.Response
        )
    ],
)
def test_deposit_success(_request):
    ret = atom_upload.deposit(ATOM_URL, ATOM_EMAIL, ATOM_PASSWORD, ATOM_SLUG, DIP_PATH)

    assert ret is None


def test_main_rsync_fail():
    effect = subprocess.CalledProcessError(1, [])
    with mock.patch("dips.atom_upload.rsync", side_effect=effect):
        ret = atom_upload.main(
            ATOM_URL,
            ATOM_EMAIL,
            ATOM_PASSWORD,
            ATOM_SLUG,
            RSYNC_TARGET,
            DIP_PATH,
            True,
        )

    assert ret == 1


def test_main_deposit_fail():
    rsync_success = mock.patch("dips.atom_upload.rsync", return_value=None)
    deposit_fail = mock.patch("dips.atom_upload.deposit", side_effect=Exception(""))
    with rsync_success, deposit_fail:
        ret = atom_upload.main(
            ATOM_URL,
            ATOM_EMAIL,
            ATOM_PASSWORD,
            ATOM_SLUG,
            RSYNC_TARGET,
            DIP_PATH,
            True,
        )

    assert ret == 2


@mock.patch("dips.atom_upload.shutil.rmtree")
def test_main_success(mock_rmtree):
    rsync_success = mock.patch("dips.atom_upload.rsync", return_value=None)
    deposit_success = mock.patch("dips.atom_upload.deposit", return_value=None)
    with rsync_success, deposit_success:
        ret = atom_upload.main(
            ATOM_URL,
            ATOM_EMAIL,
            ATOM_PASSWORD,
            ATOM_SLUG,
            RSYNC_TARGET,
            DIP_PATH,
            True,
        )

    mock_rmtree.assert_called_with(DIP_PATH)
    assert ret is None
