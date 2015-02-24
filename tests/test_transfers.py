#!/usr/bin/env python

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import vcr

from transfers import transfer
from transfers import models

AM_URL = 'http://127.0.0.1'
USER = 'demo'
API_KEY = '1c34274c0df0bca7edf9831dd838b4a6345ac2ef'

engine = create_engine('sqlite:///:memory:')
models.Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


class TestAutomateTransfers(unittest.TestCase):
    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_transfer.yaml')
    def test_get_status_transfer(self):
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        transfer_name = 'test1'
        info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', session)
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'transfer'
        assert info['name'] == transfer_name
        assert info['uuid'] == transfer_uuid
        assert info['directory'] == transfer_name
        assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/activeTransfers/standardTransfer/test1/'

    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_transfer_to_ingest.yaml')
    def test_get_status_transfer_to_ingest(self):
        # Reference values
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        unit_name = 'test1'
        sip_uuid = 'f2248e2a-b593-43db-b60c-fa8513021785'
        # Setup transfer in DB
        new_transfer = models.Unit(uuid=transfer_uuid, path=b'/foo', unit_type='transfer', status='PROCESSING', current=True)
        session.add(new_transfer)
        session.commit()

        # Run test
        info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', session)
        # Verify
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'SIP'
        assert info['name'] == unit_name
        assert info['uuid'] == sip_uuid
        assert info['directory'] == unit_name + '-' + sip_uuid
        assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/workFlowDecisions/selectFormatIDToolIngest/test1-f2248e2a-b593-43db-b60c-fa8513021785/'

    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_ingest.yaml')
    def test_get_status_ingest(self):
        sip_uuid = 'f2248e2a-b593-43db-b60c-fa8513021785'
        sip_name = 'test1'
        info = transfer.get_status(AM_URL, USER, API_KEY, sip_uuid, 'ingest', session)
        assert isinstance(info, dict)
        assert info['status'] == 'USER_INPUT'
        assert info['type'] == 'SIP'
        assert info['name'] == sip_name
        assert info['uuid'] == sip_uuid
        assert info['directory'] == sip_name + '-' + sip_uuid
        assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/workFlowDecisions/selectFormatIDToolIngest/test1-f2248e2a-b593-43db-b60c-fa8513021785/'

    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_no_unit.yaml')
    def test_get_status_no_unit(self):
        transfer_uuid = 'deadc0de-c0de-c0de-c0de-deadc0dec0de'
        info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', session)
        assert info is None

    @vcr.use_cassette('fixtures/vcr_cassettes/get_status_not_json.yaml')
    def test_get_status_not_json(self):
        transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
        info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', session)
        assert info is None
