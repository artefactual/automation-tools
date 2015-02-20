#!/usr/bin/env python

from transfers import transfer

import vcr


AM_URL = 'http://127.0.0.1'
USER = 'demo'
API_KEY = '1c34274c0df0bca7edf9831dd838b4a6345ac2ef'


@vcr.use_cassette('fixtures/vcr_cassettes/get_status_transfer.yaml')
def test_get_status_transfer():
    transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
    transfer_name = 'test1'
    info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', 'last_unit')
    assert isinstance(info, dict)
    assert info['status'] == 'USER_INPUT'
    assert info['type'] == 'transfer'
    assert info['name'] == transfer_name
    assert info['uuid'] == transfer_uuid
    assert info['directory'] == transfer_name
    assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/activeTransfers/standardTransfer/test1/'

@vcr.use_cassette('fixtures/vcr_cassettes/get_status_transfer_to_ingest.yaml')
def test_get_status_transfer_to_ingest():
    transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
    unit_name = 'test1'
    sip_uuid = 'f2248e2a-b593-43db-b60c-fa8513021785'
    info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', 'last_unit')
    assert isinstance(info, dict)
    assert info['status'] == 'USER_INPUT'
    assert info['type'] == 'SIP'
    assert info['name'] == unit_name
    assert info['uuid'] == sip_uuid
    assert info['directory'] == unit_name + '-' + sip_uuid
    assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/workFlowDecisions/selectFormatIDToolIngest/test1-f2248e2a-b593-43db-b60c-fa8513021785/'

@vcr.use_cassette('fixtures/vcr_cassettes/get_status_ingest.yaml')
def test_get_status_ingest():
    sip_uuid = 'f2248e2a-b593-43db-b60c-fa8513021785'
    sip_name = 'test1'
    info = transfer.get_status(AM_URL, USER, API_KEY, sip_uuid, 'ingest', 'last_unit')
    assert isinstance(info, dict)
    assert info['status'] == 'USER_INPUT'
    assert info['type'] == 'SIP'
    assert info['name'] == sip_name
    assert info['uuid'] == sip_uuid
    assert info['directory'] == sip_name + '-' + sip_uuid
    assert info['path'] == '/var/archivematica/sharedDirectory/watchedDirectories/workFlowDecisions/selectFormatIDToolIngest/test1-f2248e2a-b593-43db-b60c-fa8513021785/'

@vcr.use_cassette('fixtures/vcr_cassettes/get_status_no_unit.yaml')
def test_get_status_no_unit():
    transfer_uuid = 'deadc0de-c0de-c0de-c0de-deadc0dec0de'
    info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', 'last_unit')
    assert info is None

@vcr.use_cassette('fixtures/vcr_cassettes/get_status_not_json.yaml')
def test_get_status_not_json():
    transfer_uuid = 'dfc8cf5f-b5b1-408c-88b1-34215964e9d6'
    info = transfer.get_status(AM_URL, USER, API_KEY, transfer_uuid, 'transfer', 'last_unit')
    assert info is None
