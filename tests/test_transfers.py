#!/usr/bin/env python
from collections import namedtuple
from datetime import datetime
import os
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import vcr

from transfers import transfer
from transfers import models

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
TimestampsMock = namedtuple('TimestampsMock', ['path', 'started_timestamp'])

my_vcr = vcr.VCR(
    filter_query_parameters=['username', 'api_key']
)

class TestAutomateTransfers(unittest.TestCase):

    engine = create_engine('sqlite:///:memory:')

    def setUp(self):
        models.Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tearDown(self):
        models.Base.metadata.drop_all(self.engine)

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_status_transfer.yaml')
    def test_get_status_transfer(self):
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        transfer_name = 'test1'
        info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', self.session)
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'transfer'
        assert info['name'] == transfer_name
        assert info['uuid'] == transfer_uuid
        assert info['directory'] == transfer_name
        assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/activeTransfers/standardTransfer/test1/'

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_status_transfer_to_ingest.yaml')
    def test_get_status_transfer_to_ingest(self):
        # Reference values
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        unit_name = 'test1'
        sip_uuid = 'f2248e2a-b593-43db-b60c-fa8513021785'
        # Setup transfer in DB
        new_transfer = models.Unit(uuid=transfer_uuid, path=b'/foo', unit_type='transfer', status='PROCESSING', current=True)
        self.session.add(new_transfer)

        # Run test
        info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', self.session)
        # Verify
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'SIP'
        assert info['name'] == unit_name
        assert info['uuid'] == sip_uuid
        assert info['directory'] == unit_name + '-' + sip_uuid
        assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/workFlowDecisions/selectFormatIDToolIngest/test1-f2248e2a-b593-43db-b60c-fa8513021785/'

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_status_ingest.yaml')
    def test_get_status_ingest(self):
        sip_uuid = 'f2248e2a-b593-43db-b60c-fa8513021785'
        sip_name = 'test1'
        info = transfer.get_status(AM_URL, USER, API_KEY, sip_uuid, 'ingest', self.session)
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'SIP'
        assert info['name'] == sip_name
        assert info['uuid'] == sip_uuid
        assert info['directory'] == sip_name + '-' + sip_uuid
        assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/workFlowDecisions/selectFormatIDToolIngest/test1-f2248e2a-b593-43db-b60c-fa8513021785/'

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_status_no_unit.yaml')
    def test_get_status_no_unit(self):
        transfer_uuid = 'deadc0de-c0de-c0de-c0de-deadc0dec0de'
        info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', self.session)
        assert info is None

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_status_not_json.yaml')
    def test_get_status_not_json(self):
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', self.session)
        assert info is None

    def test_get_accession_id_no_script(self):
        accession_id = transfer.get_accession_id(os.path.curdir)
        assert accession_id is None

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_first_run.yaml')
    def test_get_next_transfer_first_run(self):
        # All default values
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, PATH_PREFIX, DEPTH, COMPLETED, FILES)
        # Verify
        assert path == b'SampleTransfers/BagTransfer'

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_existing_set.yaml')
    def test_get_next_transfer_existing_set(self):
        # Set completed set
        completed = {b'SampleTransfers/BagTransfer'}
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, PATH_PREFIX, DEPTH, completed, FILES)
        # Verify
        assert path == b'SampleTransfers/CSVmetadata'

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_depth.yaml')
    def test_get_next_transfer_depth(self):
        # Set depth
        depth = 2
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, PATH_PREFIX, depth, COMPLETED, FILES)
        # Verify
        assert path == b'SampleTransfers/BagTransfer/data'

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_no_prefix.yaml')
    def test_get_next_transfer_no_prefix(self):
        # Set no prefix
        path_prefix = b''
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, path_prefix, DEPTH, COMPLETED, FILES)
        # Verify
        assert path == b'OPF format-corpus'

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_all_complete.yaml')
    def test_get_next_transfer_all_complete(self):
        # Set completed set to be all elements
        completed = {b'SampleTransfers/BagTransfer', b'SampleTransfers/CSVmetadata', b'SampleTransfers/DigitizationOutput', b'SampleTransfers/DSpaceExport', b'SampleTransfers/Images', b'SampleTransfers/ISODiskImage', b'SampleTransfers/Multimedia', b'SampleTransfers/OCRImage', b'SampleTransfers/OfficeDocs', b'SampleTransfers/RawCameraImages', b'SampleTransfers/structMapSample'}
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, PATH_PREFIX, DEPTH, completed, FILES)
        # Verify
        assert path is None

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_bad_source.yaml')
    def test_get_next_transfer_bad_source(self):
        # Set bad TS Location UUID
        ts_location_uuid = 'badd8d39-9cee-495e-b7ee-5e6292549bad'
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, ts_location_uuid, PATH_PREFIX, DEPTH, COMPLETED, FILES)
        # Verify
        assert path is None

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_files.yaml')
    def test_get_next_transfer_files(self):
        # See files
        files = True
        completed = {b'SampleTransfers/BagTransfer'}
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, PATH_PREFIX, DEPTH, completed, files)
        # Verify
        assert path == b'SampleTransfers/BagTransfer.zip'

    # Not ignoring the auth parameters
    @vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_failed_auth.yaml')
    def test_get_next_transfer_failed_auth(self):
        # All default values
        ss_user = 'demo'
        ss_key = 'dne'
        # Test
        path = transfer.get_next_transfer(SS_URL, ss_user, ss_key, TS_LOCATION_UUID, PATH_PREFIX, DEPTH, COMPLETED, FILES)
        # Verify
        assert path is None

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_updated_timestamp.yaml')
    def test_get_next_transfer_updated_timestamp(self):
        # Set timestamps
        completed = {b'SampleTransfers/BagTransfer', b'SampleTransfers/CSVmetadata', b'SampleTransfers/DigitizationOutput', b'SampleTransfers/DSpaceExport', b'SampleTransfers/Images', b'SampleTransfers/ISODiskImage', b'SampleTransfers/Multimedia', b'SampleTransfers/OCRImage', b'SampleTransfers/OfficeDocs', b'SampleTransfers/RawCameraImages', b'SampleTransfers/structMapSample'}
        started_timestamps = [
            TimestampsMock(b'SampleTransfers/BagTransfer', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/Images', datetime(2010, 1, 1)),
        ]
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, PATH_PREFIX, DEPTH, completed, FILES, started_timestamps)
        # Verify
        assert path == b'SampleTransfers/Images'

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_no_new_timestamp.yaml')
    def test_get_next_transfer_no_new_timestamp(self):
        # Set timestamps
        completed = {b'SampleTransfers/BagTransfer', b'SampleTransfers/CSVmetadata', b'SampleTransfers/DigitizationOutput', b'SampleTransfers/DSpaceExport', b'SampleTransfers/Images', b'SampleTransfers/ISODiskImage', b'SampleTransfers/Multimedia', b'SampleTransfers/OCRImage', b'SampleTransfers/OfficeDocs', b'SampleTransfers/RawCameraImages', b'SampleTransfers/structMapSample'}
        started_timestamps = [
            TimestampsMock(b'SampleTransfers/BagTransfer', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/CSVmetadata', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/DigitizationOutput', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/DSpaceExport', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/Images', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/ISODiskImage', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/Multimedia', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/OCRImage', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/OfficeDocs', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/RawCameraImages', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/structMapSample', datetime(2020, 1, 1)),
        ]
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, PATH_PREFIX, DEPTH, completed, FILES, started_timestamps)
        # Verify
        assert path is None

    @my_vcr.use_cassette('fixtures/vcr_cassettes/get_next_transfer_missing_timestamps.yaml')
    def test_get_next_transfer_missing_timestamps(self):
        # Set timestamps
        completed = {b'SampleTransfers/BagTransfer', b'SampleTransfers/CSVmetadata', b'SampleTransfers/DigitizationOutput', b'SampleTransfers/DSpaceExport', b'SampleTransfers/Images', b'SampleTransfers/ISODiskImage', b'SampleTransfers/Multimedia', b'SampleTransfers/OCRImage', b'SampleTransfers/OfficeDocs', b'SampleTransfers/RawCameraImages', b'SampleTransfers/structMapSample'}
        started_timestamps = [
            TimestampsMock(b'SampleTransfers/BagTransfer', None),
            TimestampsMock(b'SampleTransfers/CSVmetadata', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/DigitizationOutput', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/DSpaceExport', None),
            TimestampsMock(b'SampleTransfers/Images', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/ISODiskImage', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/Multimedia', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/OCRImage', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/OfficeDocs', datetime(2020, 1, 1)),
            TimestampsMock(b'SampleTransfers/RawCameraImages', None),
            TimestampsMock(b'SampleTransfers/structMapSample', datetime(2020, 1, 1)),
        ]
        # Test
        path = transfer.get_next_transfer(SS_URL, SS_USER, SS_KEY, TS_LOCATION_UUID, PATH_PREFIX, DEPTH, completed, FILES, started_timestamps)
        # Verify
        assert path is None

    def test_create_or_update_insert_new(self):
        path = b'SampleTransfers/BagTransfer'
        uuid = 'bfda3299-3e6c-4a49-bce2-cba1e229b18d'
        unit_type = 'transfer'
        current = True
        timestamp = datetime(2010, 1, 1)
        assert list(self.session.query(models.Unit).filter_by(path=path)) == []
        # Test
        new_unit = transfer.create_or_update_unit(self.session, path, uuid=uuid, unit_type=unit_type, current=current, started_timestamp=timestamp)

        # Verify
        assert new_unit
        unit = self.session.query(models.Unit).filter_by(path=path).one()
        assert unit.id == new_unit.id
        assert unit.uuid == uuid == new_unit.uuid
        assert unit.path == path == new_unit.path
        assert unit.unit_type == unit_type == new_unit.unit_type
        assert unit.current == current == new_unit.current
        assert unit.started_timestamp == timestamp == new_unit.started_timestamp

    def test_create_or_updated_update(self):
        path = b'SampleTransfers/BagTransfer'
        uuid = 'bfda3299-3e6c-4a49-bce2-cba1e229b18d'
        unit_type = 'transfer'
        current = True
        timestamp = datetime(2010, 1, 1)
        self.session.add(models.Unit(path=path, uuid=uuid, unit_type=unit_type, current=current, started_timestamp=timestamp))
        assert len(list(self.session.query(models.Unit).filter_by(path=path))) == 1
        unit = self.session.query(models.Unit).filter_by(path=path).one()
        assert unit.id
        assert unit.uuid == uuid
        assert unit.path == path
        assert unit.unit_type == unit_type
        assert unit.current == current
        assert unit.started_timestamp == timestamp

        new_timestamp = datetime(2020, 2, 2)
        # Test
        new_unit = transfer.create_or_update_unit(self.session, path, started_timestamp=new_timestamp)
        self.session.commit()
        # Verify
        assert len(list(self.session.query(models.Unit).filter_by(path=path))) == 1
        unit = self.session.query(models.Unit).filter_by(path=path)[0]
        assert unit.id == new_unit.id
        assert unit.uuid == uuid == new_unit.uuid
        assert unit.path == path == new_unit.path
        assert unit.unit_type == unit_type == new_unit.unit_type
        assert unit.current == current == new_unit.current
        assert unit.started_timestamp == new_timestamp == new_unit.started_timestamp
