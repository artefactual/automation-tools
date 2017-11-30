#!/usr/bin/env python

import subprocess
import unittest
import vcr

try:
    import mock
except ImportError:
    from unittest import mock

from dips import atom_upload

ATOM_URL = 'http://192.168.168.193'
ATOM_EMAIL = 'demo@example.com'
ATOM_PASSWORD = 'demo'
ATOM_SLUG = 'test'
RSYNC_TARGET = '192.168.168.193:/tmp'
DIP_PATH = '/tmp/fake_DIP'


class TestAtomUpload(unittest.TestCase):
    def test_rsync_fail(self):
        effect = subprocess.CalledProcessError(1, [])
        with mock.patch('subprocess.check_output', side_effect=effect):
            self.assertRaises(subprocess.CalledProcessError,
                              atom_upload.rsync, RSYNC_TARGET, DIP_PATH)

    def test_rsync_success(self):
        with mock.patch('subprocess.check_output', return_value=None):
            ret = atom_upload.rsync(RSYNC_TARGET, DIP_PATH)

        assert ret is None

    @vcr.use_cassette('fixtures/vcr_cassettes/deposit_fail.yaml')
    def test_deposit_fail(self):
        self.assertRaises(
            Exception,
            atom_upload.deposit,
            ATOM_URL,
            'bad@email.com',
            ATOM_PASSWORD,
            ATOM_SLUG,
            DIP_PATH
        )

    @vcr.use_cassette('fixtures/vcr_cassettes/deposit_success.yaml')
    def test_deposit_success(self):
        ret = atom_upload.deposit(
            ATOM_URL,
            ATOM_EMAIL,
            ATOM_PASSWORD,
            ATOM_SLUG,
            DIP_PATH
        )

        assert ret is None

    def test_main_rsync_fail(self):
        effect = subprocess.CalledProcessError(1, [])
        with mock.patch('dips.atom_upload.rsync', side_effect=effect):
            ret = atom_upload.main(
                ATOM_URL,
                ATOM_EMAIL,
                ATOM_PASSWORD,
                ATOM_SLUG,
                RSYNC_TARGET,
                DIP_PATH
            )

        assert ret == 1

    def test_main_deposit_fail(self):
        rsync_success = mock.patch('dips.atom_upload.rsync', return_value=None)
        deposit_fail = mock.patch('dips.atom_upload.deposit',
                                  side_effect=Exception(''))
        with rsync_success, deposit_fail:
            ret = atom_upload.main(
                ATOM_URL,
                ATOM_EMAIL,
                ATOM_PASSWORD,
                ATOM_SLUG,
                RSYNC_TARGET,
                DIP_PATH
            )

        assert ret == 2

    def test_main_success(self):
        rsync_success = mock.patch('dips.atom_upload.rsync', return_value=None)
        deposit_success = mock.patch('dips.atom_upload.deposit',
                                     return_value=None)
        with rsync_success, deposit_success:
            ret = atom_upload.main(
                ATOM_URL,
                ATOM_EMAIL,
                ATOM_PASSWORD,
                ATOM_SLUG,
                RSYNC_TARGET,
                DIP_PATH
            )

        assert ret is None
