#!/usr/bin/env python
"""To run the tests::

    $ python -m unittest tests.test_amclient

"""
import os
import shutil
import unittest

import vcr

from transfers import amclient


AM_URL = 'http://192.168.168.192'
SS_URL = 'http://192.168.168.192:8000'
AM_USER_NAME = 'test'
AM_API_KEY = '3c23b0361887ace72b9d42963d9acbdf06644673'
SS_USER_NAME = 'test'
SS_API_KEY = '5de62f6f4817f903dcfac47fa5cffd44685a2cf2'
TMP_DIR = '.tmp-dip-downloads'
TRANSFER_SOURCE_UUID = '7609101e-15b2-4f4f-a19d-7b23673ac93b'


class TmpDir:
    """Context manager to clear and create a temporary directory and destroy it
    after usage.
    """

    def __init__(self, tmp_dir_path):
        self.tmp_dir_path = tmp_dir_path

    def __enter__(self):
        if os.path.isdir(self.tmp_dir_path):
            shutil.rmtree(self.tmp_dir_path)
        os.makedirs(self.tmp_dir_path)
        return self.tmp_dir_path

    def __exit__(self, exc_type, exc_value, traceback):
        if os.path.isdir(self.tmp_dir_path):
            shutil.rmtree(self.tmp_dir_path)
        if exc_type:
            return None


class TestAMClient(unittest.TestCase):

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/completed_transfers_transfers.yaml')
    def test_completed_transfers_transfers(self):
        """Test getting completed transfers when there are completed transfers
        to get.
        """
        completed_transfers = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).completed_transfers()
        assert (completed_transfers['message'] ==
                'Fetched completed transfers successfully.')
        results = completed_transfers['results']
        assert isinstance(results, list)
        assert len(results) == 2
        for item in results:
            assert amclient.is_uuid(item)

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/close_completed_transfers_transfers.yaml')
    def test_close_completed_transfers_transfers(self):
        """Test closing completed transfers when there are completed transfers
        to close.
        """
        response = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).close_completed_transfers()
        close_succeeded = response['close_succeeded']
        completed_transfers = response['completed_transfers']
        assert close_succeeded == completed_transfers
        assert isinstance(close_succeeded, list)
        assert len(close_succeeded) == 2
        for item in close_succeeded:
            assert amclient.is_uuid(item)

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/completed_transfers_no_transfers.yaml')
    def test_completed_transfers_no_transfers(self):
        """Test getting completed transfers when there are no completed
        transfers to get.
        """
        completed_transfers = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).completed_transfers()
        assert (completed_transfers['message'] ==
                'Fetched completed transfers successfully.')
        results = completed_transfers['results']
        assert isinstance(results, list)
        assert len(results) == 0

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/close_completed_transfers_no_transfers.yaml')
    def test_close_completed_transfers_no_transfers(self):
        """Test closing completed transfers when there are no completed
        transfers to close.
        """
        response = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).close_completed_transfers()
        close_succeeded = response['close_succeeded']
        completed_transfers = response['completed_transfers']
        assert close_succeeded == completed_transfers
        assert isinstance(close_succeeded, list)
        assert len(close_succeeded) == 0

    @vcr.use_cassette('fixtures/vcr_cassettes/completed_transfers_bad_key.yaml')
    def test_completed_transfers_bad_key(self):
        """Test getting completed transfers when a bad AM API key is
        provided.
        """
        completed_transfers = amclient.AMClient(
            am_api_key='bad api key', am_user_name=AM_USER_NAME,
            am_url=AM_URL).completed_transfers()
        assert completed_transfers is None

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/unapproved_transfers_transfers.yaml')
    def test_unapproved_transfers_transfers(self):
        """Test getting unapproved transfers when there are unapproved transfers
        to get.
        """
        unapproved_transfers = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).unapproved_transfers()
        assert (unapproved_transfers['message'] ==
                'Fetched unapproved transfers successfully.')
        results = unapproved_transfers['results']
        assert isinstance(results, list)
        assert len(results) == 1
        for unapproved_transfer in results:
            assert 'type' in unapproved_transfer
            assert 'uuid' in unapproved_transfer
            assert 'directory' in unapproved_transfer
            assert amclient.is_uuid(unapproved_transfer['uuid'])

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/unapproved_transfers_no_transfers.yaml')
    def test_unapproved_transfers_no_transfers(self):
        """Test getting unapproved transfers when there are no unapproved
        transfers to get.
        """
        unapproved_transfers = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).unapproved_transfers()
        assert (unapproved_transfers['message'] ==
                'Fetched unapproved transfers successfully.')
        results = unapproved_transfers['results']
        assert isinstance(results, list)
        assert len(results) == 0

    @vcr.use_cassette('fixtures/vcr_cassettes/transferables.yaml')
    def test_transferables(self):
        """Test that we can get all transferable entities in the Storage
        Service.
        """
        transferables = amclient.AMClient(
            ss_api_key=SS_API_KEY,
            transfer_source=TRANSFER_SOURCE_UUID,
            ss_user_name=SS_USER_NAME,
            ss_url=SS_URL,
            transfer_path='').transferables()
        assert isinstance(transferables, dict)
        assert 'directories' in transferables
        assert 'entries' in transferables
        assert 'properties' in transferables
        assert transferables['directories'] == ['ubuntu', 'vagrant']

    @vcr.use_cassette('fixtures/vcr_cassettes/transferables_path.yaml')
    def test_transferables_path(self):
        """Test that we can get all transferable entities in the Storage
        Service under a given path.
        """
        transferables = amclient.AMClient(
            transfer_path=b'vagrant/archivematica-sampledata',
            ss_api_key=SS_API_KEY,
            transfer_source=TRANSFER_SOURCE_UUID,
            ss_user_name=SS_USER_NAME,
            ss_url=SS_URL).transferables()
        assert isinstance(transferables, dict)
        assert 'directories' in transferables
        assert 'entries' in transferables
        assert 'properties' in transferables
        assert transferables['directories'] == [
            'OPF format-corpus', 'SampleTransfers', 'TestTransfers']

    @vcr.use_cassette('fixtures/vcr_cassettes/transferables_bad_path.yaml')
    def test_transferables_bad_path(self):
        """Test that we get empty values when we request all transferable
        entities in the Storage Service with a non-existent path.
        """
        transferables = amclient.AMClient(
            transfer_path=b'vagrant/archivematica-sampledataz',
            ss_api_key=SS_API_KEY,
            transfer_source=TRANSFER_SOURCE_UUID,
            ss_user_name=SS_USER_NAME,
            ss_url=SS_URL).transferables()
        assert isinstance(transferables, dict)
        assert 'directories' in transferables
        assert 'entries' in transferables
        assert 'properties' in transferables
        assert transferables['directories'] == []
        assert transferables['entries'] == []
        assert transferables['properties'] == {}

    @vcr.use_cassette('fixtures/vcr_cassettes/aips_aips.yaml')
    def test_aips_aips(self):
        """Test that we can get all AIPs in the Storage Service.

        Note that for this vcr cassette, the SS TastyPie API was modified to
        return pages of only one package at a time, just to make sure that
        AMClient handles the pagination correctly.
        """
        aips = amclient.AMClient(
            ss_url=SS_URL,
            ss_user_name=SS_USER_NAME,
            ss_api_key=SS_API_KEY).aips()
        assert isinstance(aips, list)
        assert len(aips) == 2
        for aip in aips:
            assert isinstance(aip, dict)
            assert 'uuid' in aip
            assert amclient.is_uuid(aip['uuid'])
            assert aip['package_type'] == 'AIP'
            assert 'AIPsStore' in aip['current_full_path']

    @vcr.use_cassette('fixtures/vcr_cassettes/dips_dips.yaml')
    def test_dips_dips(self):
        """Test that we can get all DIPs in the Storage Service."""
        dips = amclient.AMClient(
            ss_url=SS_URL,
            ss_user_name=SS_USER_NAME,
            ss_api_key=SS_API_KEY).dips()
        assert isinstance(dips, list)
        assert len(dips) == 2
        for dip in dips:
            assert isinstance(dip, dict)
            assert 'uuid' in dip
            assert amclient.is_uuid(dip['uuid'])
            assert dip['package_type'] == 'DIP'
            assert 'DIPsStore' in dip['current_full_path']

    @vcr.use_cassette('fixtures/vcr_cassettes/dips_no_dips.yaml')
    def test_dips_no_dips(self):
        """Test that we get no DIPs from the Storage Service if there are none.
        """
        dips = amclient.AMClient(
            ss_url=SS_URL,
            ss_user_name=SS_USER_NAME,
            ss_api_key=SS_API_KEY).dips()
        assert isinstance(dips, list)
        assert dips == []

    @vcr.use_cassette('fixtures/vcr_cassettes/aips2dips.yaml')
    def test_aips2dips(self):
        """Test that we can get all AIPs in the Storage Service and their
        corresonding DIPs.
        """
        aips2dips = amclient.AMClient(
            ss_url=SS_URL,
            ss_user_name=SS_USER_NAME,
            ss_api_key=SS_API_KEY).aips2dips()
        assert isinstance(aips2dips, dict)
        assert len(aips2dips) == 4
        assert aips2dips['3500aee0-08ca-40ff-8d2d-9fe9a2c3ae3b'] == []
        assert aips2dips['979cce65-2a6f-407f-a49c-1bcf13bd8571'] == []
        assert (aips2dips['721b98b9-b894-4cfb-80ab-624e52263300'] ==
                ['c0e37bab-e51e-482d-a066-a277330de9a7'])
        assert (aips2dips['99bb20ee-69c6-43d0-acf0-c566020357d2'] ==
                ['7e49afa4-116b-4650-8bbb-9341906bdb21'])

    @vcr.use_cassette('fixtures/vcr_cassettes/aip2dips_dip.yaml')
    def test_aip2dips_dips(self):
        """Test that we can get all of the DIPs from the Storage Service for a
        given AIP.
        """
        aip_uuid = '721b98b9-b894-4cfb-80ab-624e52263300'
        dip_uuid = 'c0e37bab-e51e-482d-a066-a277330de9a7'
        dips = amclient.AMClient(
            aip_uuid=aip_uuid,
            ss_url=SS_URL,
            ss_user_name=SS_USER_NAME,
            ss_api_key=SS_API_KEY).aip2dips()
        assert isinstance(dips, list)
        assert len(dips) == 1
        dip = dips[0]
        assert isinstance(dip, dict)
        assert dip['package_type'] == 'DIP'
        assert dip['uuid'] == dip_uuid

    @vcr.use_cassette('fixtures/vcr_cassettes/aip2dips_no_dip.yaml')
    def test_aip2dips_no_dips(self):
        """Test that we get no DIPs when attempting to get all DIPs
        corresponding to an AIP that has none.
        """
        aip_uuid = '3500aee0-08ca-40ff-8d2d-9fe9a2c3ae3b'
        dips = amclient.AMClient(
            aip_uuid=aip_uuid,
            ss_url=SS_URL,
            ss_user_name=SS_USER_NAME,
            ss_api_key=SS_API_KEY).aip2dips()
        assert isinstance(dips, list)
        assert len(dips) == 0

    @vcr.use_cassette('fixtures/vcr_cassettes/download_dip_dip.yaml')
    def test_download_dip_dip(self):
        """Test that we can download a DIP when there is one."""
        with TmpDir(TMP_DIR):
            dip_uuid = 'c0e37bab-e51e-482d-a066-a277330de9a7'
            dip_path = amclient.AMClient(
                dip_uuid=dip_uuid,
                ss_url=SS_URL,
                ss_user_name=SS_USER_NAME,
                ss_api_key=SS_API_KEY,
                directory=TMP_DIR).download_dip()
            assert (dip_path ==
                    '{}/package-c0e37bab-e51e-482d-a066-a277330de9a7.7z'.format(
                        TMP_DIR))
            assert os.path.isfile(dip_path)

    @vcr.use_cassette('fixtures/vcr_cassettes/download_dip_no_dip.yaml')
    def test_download_dip_no_dip(self):
        """Test that we can try to download a DIP that does not exist."""
        dip_uuid = 'bad dip uuid'
        dip_path = amclient.AMClient(
            dip_uuid=dip_uuid,
            ss_url=SS_URL,
            ss_user_name=SS_USER_NAME,
            ss_api_key=SS_API_KEY).download_dip()
        assert dip_path is None

    @vcr.use_cassette('fixtures/vcr_cassettes/download_aip_success.yaml')
    def test_download_aip_success(self):
        """Test that we can download an AIP when there is one."""
        with TmpDir(TMP_DIR):
            aip_uuid = '216dd8a6-c366-41f8-b11e-0c70814b3992'
            transfer_name = 'transfer'
            # Changing the SS_API_KEY global var to generate the cassetes
            # for the new test cases makes all the other cassetes to fail.
            # Adding a local var to be able to generate the new cassetes.
            ss_api_key = '7021334bee4c9155c07e531608dd28a9d8039420'
            aip_path = amclient.AMClient(
                aip_uuid=aip_uuid,
                ss_url=SS_URL,
                ss_user_name=SS_USER_NAME,
                ss_api_key=ss_api_key,
                directory=TMP_DIR).download_aip()
            assert (aip_path ==
                    '{}/{}-{}.7z'.format(
                        TMP_DIR, transfer_name, aip_uuid))
            assert os.path.isfile(aip_path)

    @vcr.use_cassette('fixtures/vcr_cassettes/download_aip_fail.yaml')
    def test_download_aip_fail(self):
        """Test that we can try to download an AIP that does not exist."""
        aip_uuid = 'bad-aip-uuid'
        # Changing the SS_API_KEY global var to generate the cassetes
        # for the new test cases makes all the other cassetes to fail.
        # Adding a local var to be able to generate the new cassetes.
        ss_api_key = '7021334bee4c9155c07e531608dd28a9d8039420'
        aip_path = amclient.AMClient(
            aip_uuid=aip_uuid,
            ss_url=SS_URL,
            ss_user_name=SS_USER_NAME,
            ss_api_key=ss_api_key).download_aip()
        assert aip_path is None

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/completed_ingests_ingests.yaml')
    def test_completed_ingests_ingests(self):
        """Test getting completed ingests when there are completed ingests
        to get.
        """
        completed_ingests = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).completed_ingests()
        assert (completed_ingests['message'] ==
                'Fetched completed ingests successfully.')
        results = completed_ingests['results']
        assert isinstance(results, list)
        assert len(results) == 2
        for item in results:
            assert amclient.is_uuid(item)

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/close_completed_ingests_ingests.yaml')
    def test_close_completed_ingests_ingests(self):
        """Test closing completed ingests when there are completed ingests
        to close.
        """
        response = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).close_completed_ingests()
        close_succeeded = response['close_succeeded']
        completed_ingests = response['completed_ingests']
        assert close_succeeded == completed_ingests
        assert isinstance(close_succeeded, list)
        assert len(close_succeeded) == 2
        for item in close_succeeded:
            assert amclient.is_uuid(item)

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/completed_ingests_no_ingests.yaml')
    def test_completed_ingests_no_ingests(self):
        """Test getting completed ingests when there are no completed
        ingests to get.
        """
        completed_ingests = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).completed_ingests()
        assert (completed_ingests['message'] ==
                'Fetched completed ingests successfully.')
        results = completed_ingests['results']
        assert isinstance(results, list)
        assert len(results) == 0

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/close_completed_ingests_no_ingests.yaml')
    def test_close_completed_ingests_no_ingests(self):
        """Test closing completed ingests when there are no completed
        ingests to close.
        """
        response = amclient.AMClient(
            am_api_key=AM_API_KEY, am_user_name=AM_USER_NAME,
            am_url=AM_URL).close_completed_ingests()
        close_succeeded = response['close_succeeded']
        completed_ingests = response['completed_ingests']
        assert close_succeeded == completed_ingests
        assert isinstance(close_succeeded, list)
        assert len(close_succeeded) == 0


if __name__ == '__main__':
    unittest.main()
