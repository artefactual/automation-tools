#!/usr/bin/env python
# -*- coding: utf-8 -*-
import collections
import os
import unittest

import mock
import vcr

from transfers import errors, transfer, models

AM_URL = 'http://127.0.0.1'
SS_URL = 'http://127.0.0.1:8000'
USER = 'demo'
API_KEY = '1c34274c0df0bca7edf9831dd838b4a6345ac2ef'
SS_USER = 'test'
SS_KEY = '7016762e174c940df304e8343c659af5005b4d6b'

TS_LOCATION_UUID = '2a3d8d39-9cee-495e-b7ee-5e629254934d'
PATH_PREFIX = b'SampleTransfers'
DEPTH = 1
COMPLETED = set()
FILES = False


class TestAutomateTransfers(unittest.TestCase):

    def setUp(self):
        models.init_session(databasefile=":memory:")

        # Setup some data to be used for test_call_start_transfer_endpoint(..)
        # and def test_call_start_transfer(..).
        transfers_dir = (
            "/var/archivematica/sharedDirectory/watchedDirectories/"
            "activeTransfers"
        )
        Result = collections.namedtuple(
            'Result', 'transfer_type target transfer_name '
            'transfer_abs_path')
        self.start_tests = [
            Result(transfer_type="standard", target="standard_1",
                   transfer_name='standard_1',
                   transfer_abs_path="{}/standardTransfer/standard_1/"
                   .format(transfers_dir),
                   ),
            Result(transfer_type="standard", target="standard_1",
                   transfer_name='standard_1_1',
                   transfer_abs_path="{}/standardTransfer/standard_1_1/"
                   .format(transfers_dir),
                   ),
            Result(transfer_type="dspace", target="dspace_1.zip",
                   transfer_name='dspace_1.zip',
                   transfer_abs_path="{}/Dspace/dspace_1.zip"
                   .format(transfers_dir),
                   ),
            Result(transfer_type="dspace", target="dspace_1.zip",
                   transfer_name='dspace_1_1.zip',
                   transfer_abs_path="{}/Dspace/dspace_1_1.zip"
                   .format(transfers_dir),
                   ),
        ]

    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_transfer.yaml')
    def test_get_status_transfer(self):
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        transfer_name = 'test1'
        info = transfer.get_status(AM_URL, USER, API_KEY, SS_URL, SS_USER,
                                   SS_KEY, transfer_uuid, 'transfer')
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'transfer'
        assert info['name'] == transfer_name
        assert info['uuid'] == transfer_uuid
        assert info['directory'] == transfer_name
        assert info['path'] == ('/var/archivematica/sharedDirectory/'
                                'watchedDirectories/activeTransfers/'
                                'standardTransfer/test1/')

    @vcr.use_cassette('fixtures/vcr_cassettes/'
                      'get_status_transfer_to_ingest.yaml')
    def test_get_status_transfer_to_ingest(self):
        # Reference values
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        unit_name = 'test1'
        sip_uuid = 'f2248e2a-b593-43db-b60c-fa8513021785'
        # Setup transfer in DB
        models._update_unit(
            uuid=transfer_uuid, path=b'/foo', unit_type="transfer",
            status="PROCESSING", current=True)
        # Run test
        info = transfer.get_status(AM_URL, USER, API_KEY, SS_URL, SS_USER,
                                   SS_KEY, transfer_uuid, 'transfer')
        # Verify
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'SIP'
        assert info['name'] == unit_name
        assert info['uuid'] == sip_uuid
        assert info['directory'] == unit_name + '-' + sip_uuid
        assert info['path'] == ('/var/archivematica/sharedDirectory/'
                                'watchedDirectories/workFlowDecisions/'
                                'selectFormatIDToolIngest/'
                                'test1-f2248e2a-b593-43db-b60c-fa8513021785/')

    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_ingest.yaml')
    def test_get_status_ingest(self):
        sip_uuid = 'f2248e2a-b593-43db-b60c-fa8513021785'
        sip_name = 'test1'
        info = transfer.get_status(AM_URL, USER, API_KEY, SS_URL, SS_USER,
                                   SS_KEY, sip_uuid, 'ingest')
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'SIP'
        assert info['name'] == sip_name
        assert info['uuid'] == sip_uuid
        assert info['directory'] == sip_name + '-' + sip_uuid
        assert info['path'] == ('/var/archivematica/sharedDirectory/'
                                'watchedDirectories/workFlowDecisions/'
                                'selectFormatIDToolIngest/'
                                'test1-f2248e2a-b593-43db-b60c-fa8513021785/')

    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_no_unit.yaml')
    def test_get_status_no_unit(self):
        transfer_uuid = 'deadc0de-c0de-c0de-c0de-deadc0dec0de'
        info = transfer.get_status(AM_URL, USER, API_KEY, SS_URL, SS_USER,
                                   SS_KEY, transfer_uuid, 'transfer')
        self.assertEqual(info,
                         errors.error_lookup(errors.ERR_INVALID_RESPONSE))

    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_not_json.yaml')
    def test_get_status_not_json(self):
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        info = transfer.get_status(AM_URL, USER, API_KEY, SS_URL, SS_USER,
                                   SS_KEY, transfer_uuid, 'transfer')
        self.assertEqual(info,
                         errors.error_lookup(errors.ERR_INVALID_RESPONSE))

    def test_get_accession_id_no_script(self):
        accession_id = transfer.get_accession_id(os.path.curdir)
        self.assertEqual(accession_id, None)

    @vcr.use_cassette('fixtures/vcr_cassettes/'
                      'get_next_transfer_first_run.yaml')
    def test_get_next_transfer_first_run(self):
        # All default values
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY,
                                          TS_LOCATION_UUID, PATH_PREFIX,
                                          DEPTH, COMPLETED, FILES)
        # Verify
        self.assertEqual(path, b'SampleTransfers/BagTransfer')

    @vcr.use_cassette('fixtures/vcr_cassettes/'
                      'get_next_transfer_existing_set.yaml')
    def test_get_next_transfer_existing_set(self):
        # Set completed set
        completed = {b'SampleTransfers/BagTransfer'}
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY,
                                          TS_LOCATION_UUID, PATH_PREFIX,
                                          DEPTH, completed, FILES)
        # Verify
        self.assertEqual(path, b'SampleTransfers/CSVmetadata')

    @vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_depth.yaml')
    def test_get_next_transfer_depth(self):
        # Set depth
        depth = 2
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY,
                                          TS_LOCATION_UUID, PATH_PREFIX,
                                          depth, COMPLETED, FILES)
        # Verify
        self.assertEqual(path, b'SampleTransfers/BagTransfer/data')

    @vcr.use_cassette('fixtures/vcr_cassettes/'
                      'get_next_transfer_no_prefix.yaml')
    def test_get_next_transfer_no_prefix(self):
        # Set no prefix
        path_prefix = b''
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY,
                                          TS_LOCATION_UUID, path_prefix,
                                          DEPTH, COMPLETED, FILES)
        # Verify
        self.assertEqual(path, b'OPF format-corpus')

    @vcr.use_cassette('fixtures/vcr_cassettes/'
                      'get_next_transfer_all_complete.yaml')
    def test_get_next_transfer_all_complete(self):
        # Set completed set to be all elements
        completed = {b'SampleTransfers/BagTransfer',
                     b'SampleTransfers/CSVmetadata',
                     b'SampleTransfers/DigitizationOutput',
                     b'SampleTransfers/DSpaceExport',
                     b'SampleTransfers/Images',
                     b'SampleTransfers/ISODiskImage',
                     b'SampleTransfers/Multimedia',
                     b'SampleTransfers/OCRImage',
                     b'SampleTransfers/OfficeDocs',
                     b'SampleTransfers/RawCameraImages',
                     b'SampleTransfers/structMapSample'}
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY,
                                          TS_LOCATION_UUID, PATH_PREFIX,
                                          DEPTH, completed, FILES)
        # Verify
        self.assertEqual(path, None)

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/get_next_transfer_bad_source.yaml')
    def test_get_next_transfer_bad_source(self):
        # Set bad TS Location UUID
        ts_location_uuid = 'badd8d39-9cee-495e-b7ee-5e6292549bad'
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY,
                                          ts_location_uuid, PATH_PREFIX,
                                          DEPTH, COMPLETED, FILES)
        # Verify
        self.assertEqual(path, None)

    @vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_files.yaml')
    def test_get_next_transfer_files(self):
        # See files
        files = True
        completed = {b'SampleTransfers/BagTransfer'}
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY,
                                          TS_LOCATION_UUID, PATH_PREFIX,
                                          DEPTH, completed, files)
        # Verify
        self.assertEqual(path, b'SampleTransfers/BagTransfer.zip')

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/get_next_transfer_failed_auth.yaml')
    def test_get_next_transfer_failed_auth(self):
        # All default values
        ss_user = 'demo'
        ss_key = 'dne'
        # Test
        path = transfer.get_next_transfer(SS_URL, ss_user, ss_key,
                                          TS_LOCATION_UUID, PATH_PREFIX,
                                          DEPTH, COMPLETED, FILES)
        # Verify.
        self.assertEqual(path, None)

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/test_transfer_approve_transfer.yaml')
    def test_approve_transfer(self):
        """Test the process of approving transfers and make sure that the
        outcome is as expected.
        """
        Result = collections.namedtuple('Result', 'dirname expected')
        approve_tests = [
            Result(dirname="unzipped_bag_1",
                   expected='8779909c-20e8-4471-beb2-c45591b7abb0'),
            Result(dirname="dspace_1",
                   expected='f25c71e6-1f1e-4e69-bf57-580a64d4e051'),
            Result(dirname="standard_1",
                   expected='0d16e57f-df1b-4a66-a93c-989f0dc9f16f'),
            Result(dirname="dirname_four",
                   expected=None)
        ]
        for test in approve_tests:
            res = transfer.approve_transfer(test.dirname,
                                            AM_URL,
                                            API_KEY,
                                            USER)
            assert res == test.expected

    @vcr.use_cassette(
        'fixtures/vcr_cassettes/test_call_start_transfer_endpoint.yaml')
    def test_call_start_transfer_endpoint(self):
        """Archivematica will rename a transfer if it is already trying to
        start one with an identical name. In the tests below, we observe (and
        test) this behavior when there is an identical name for a transfer
        twice, across two transfer types. We also make sure that the transfer
        path is preserved. This path is used in pre-transfer scripts to enable
        the automation tools to create manifests, perform arrangement tasks, or
        manipulate content prior to the transfer being approved.
        """
        for test in self.start_tests:
            transfer_name, transfer_abs_path = transfer.call_start_transfer_endpoint(
                am_url=AM_URL, am_user=USER,
                am_api_key=API_KEY, target=test.target.encode(),
                transfer_type=test.transfer_type.encode(),
                accession=test.transfer_name.encode(),
                ts_location_uuid=TS_LOCATION_UUID)
            assert transfer_name == test.transfer_name
            assert transfer_abs_path == test.transfer_abs_path

    @mock.patch(
        "transfers.transfer.approve_transfer",
        return_value="4bd2006a-1178-4695-9463-5c72eec6257a")
    @vcr.use_cassette(
        'fixtures/vcr_cassettes/test_call_start_transfer_endpoint.yaml')
    def test_call_start_transfer(self, mock_approve_transfer):
        """Provide an integration test as best as we can for the
        transfer.start_transfer function where the returned values are crucial
        to the automation of Archivematica work-flows. The test reuses the
        test_call_start_transfer_endpoint.yaml fixtures as this function is
        crucial to what eventual gets stored in the model and we can test this
        more realistically by using it instead of mocking it.
        """
        returned_uuid = "4bd2006a-1178-4695-9463-5c72eec6257a"
        for test in self.start_tests:
            models.init_session(databasefile=":memory:")
            with mock.patch("transfers.transfer.get_next_transfer") \
                    as mock_get_next_transfer:
                mock_get_next_transfer.return_value = \
                    test.target.encode()
                res = transfer.call_start_transfer_endpoint(
                    am_url=AM_URL, am_user=USER,
                    am_api_key=API_KEY, target=test.target.encode(),
                    transfer_type=test.transfer_type.encode(),
                    accession=test.transfer_name.encode(),
                    ts_location_uuid=TS_LOCATION_UUID
                )
                result_encoded = (res[0], res[1].encode())
                with mock.patch(
                        "transfers.transfer.call_start_transfer_endpoint") \
                        as mock_call_start_transfer_endpoint:
                    mock_call_start_transfer_endpoint.return_value \
                        = result_encoded
                    new_transfer = transfer.start_transfer(
                        ss_url="http://127.0.0.1:62081",
                        ss_user="test",
                        ss_api_key="test",
                        ts_location_uuid=None,
                        ts_path="",
                        depth="test",
                        am_url="http://127.0.0.1:62090",
                        am_user="test",
                        am_api_key="test",
                        transfer_type="standard",
                        see_files=False,
                        config_file="config.cfg",
                    )
                    assert new_transfer.path.decode() == test.target
                    assert new_transfer.uuid == returned_uuid
                    assert new_transfer.current is True
                    assert new_transfer.unit_type == 'transfer'
                    # Make a secondary call to the database to see if we can
                    # retrieve our information. Obviously this should not have
                    # changed since we wrote it to memory.
                    unit = models.retrieve_unit_by_type_and_uuid(
                        returned_uuid, 'transfer')
                    assert unit.uuid == returned_uuid
                    assert unit.current is True
                    assert unit.unit_type == 'transfer'
