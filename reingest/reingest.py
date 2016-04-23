#!/usr/bin/env python
"""
Automate Reingest

Example script to automate reingesting AIPS.

"""

from __future__ import print_function, unicode_literals
import argparse
import base64
import logging
import logging.config  # Has to be imported separately
import os
import requests
import sys
import time

LOGGER = logging.getLogger('reingest')


def start_reingest(ss_url, aip_uuid, pipeline, reingest_type, processing_config='default'):
    """
    Start reingest on an AIP.

    :param ss_url: URL of the storage service.
    :param aip_uuid: UUID of the AIP to reingest.
    :param pipeline: UUID of the pipeline to reingest on.
    :param reingest_type: Type of reingest to start. One of objects, metadata, full
    :param processing_config: Processing configuration to specify for a full reingest.
    """
    url = ss_url + '/api/v2/file/' + aip_uuid + '/reingest/'
    data = {
        'pipeline': pipeline,
        'reingest_type': reingest_type,
        'processing_config': processing_config,
    }
    LOGGER.debug('URL: %s; JSON body: %s', url, data)
    try:
        response = requests.post(url, json=data)
    except Exception:
        LOGGER.exception('Error POSTing to start reingest')
        return None
    LOGGER.debug('Response: %s', response)
    if response.status_code != requests.codes.accepted:  # 202
        LOGGER.warning('Request to %s returned %s %s', url, response.status_code, response.reason)
        LOGGER.warning('Response: %s', response.text)
        return None
    try:
        return response.json()
    except ValueError:  # JSON could not be decoded
        LOGGER.warning('Could not parse JSON from response: %s', response.text)
        return None

# From transfers/transfer.py
def _call_url_json(url, params):
    """
    Helper to GET a URL where the expected response is 200 with JSON.

    :param str url: URL to call
    :param dict params: Params to pass to requests.get
    :returns: Dict of the returned JSON or None
    """
    LOGGER.debug('URL: %s; params: %s;', url, params)
    response = requests.get(url, params=params)
    LOGGER.debug('Response: %s', response)
    if not response.ok:
        LOGGER.warning('Request to %s returned %s %s', url, response.status_code, response.reason)
        LOGGER.debug('Response: %s', response.text)
        return None
    try:
        return response.json()
    except ValueError:  # JSON could not be decoded
        LOGGER.warning('Could not parse JSON from response: %s', response.text)
        return None

# Slightly modified from transfers/transfer.py
def approve_transfer(unit_uuid, url, api_key, user_name):
    """
    Approve transfer with unit_uuid.

    :returns: UUID of the approved transfer or None.
    """
    time.sleep(6)
    # List available transfers
    get_url = url + "/api/transfer/unapproved"
    params = {'username': user_name, 'api_key': api_key}
    waiting_transfers = _call_url_json(get_url, params)
    if waiting_transfers is None:
        LOGGER.warning('No waiting transfer ')
        return None
    for a in waiting_transfers['results']:
        LOGGER.debug("Found waiting transfer: %s", a['directory'])
        if a['uuid'] == unit_uuid:
            # Post to approve transfer
            post_url = url + "/api/transfer/approve/"
            params = {'username': user_name, 'api_key': api_key, 'type': a['type'], 'directory': a['directory']}
            LOGGER.debug('URL: %s; Params: %s;', post_url, params)
            r = requests.post(post_url, data=params)
            LOGGER.debug('Response: %s', r)
            LOGGER.debug('Response text: %s', r.text)
            if r.status_code != 200:
                return None
            return a['uuid']
        else:
            LOGGER.debug("%s is not what we are looking for", a['directory'])
    else:
        return None


def reingest(ss_url, aip_uuid, pipeline, reingest_type, processing_config='default', am_url=None, user_name=None, api_key=None):
    # Start reingest
    LOGGER.info('Starting %s reingest of AIP %s on pipeline %s with %s config', reingest_type, aip_uuid, pipeline, processing_config)
    response = start_reingest(ss_url, aip_uuid, pipeline, reingest_type, processing_config)
    if response is None:
        LOGGER.info('Exiting')
        return
    reingest_uuid = response.get('reingest_uuid')
    LOGGER.info('Reingested UUID: %s', reingest_uuid)

    # Approve reingest
    LOGGER.info("About to approve reingest for %s", reingest_uuid)
    if am_url and api_key and user_name:
        retry_count = 3
        for i in range(retry_count):
            result = approve_transfer(reingest_uuid, am_url, api_key, user_name)
            # Mark as started
            if result:
                LOGGER.info('Approved %s', result)
                break
            LOGGER.info('Failed approve, try %s of %s', i + 1, retry_count)
        else:
            LOGGER.warning('Not approved')
            return None
    else:
        LOGGER.info('Archivematica API not information provided, cannot approve transfer.')
    LOGGER.info('Done %s reingest of AIP %s on pipeline %s with %s config and reingest UUID of %s', reingest_type, aip_uuid, pipeline, processing_config, reingest_uuid)


def metadata(sip_uuid, paths, am_url, user_name, api_key):
    LOGGER.info('Starting adding metadata files to SIP %s', sip_uuid)
    url = am_url + "/api/ingest/copy_metadata_files/"
    params = {'username': user_name, 'api_key': api_key}
    paths = [base64.b64encode(bytes(p, encoding='utf8')) for p in paths]
    data = {'sip_uuid': sip_uuid, 'source_paths[]': paths}
    try:
        response = requests.post(url, params=params, data=data)
    except Exception:
        LOGGER.error('Error POSTing to add metadata files.')
        return
    if response.status_code != requests.codes.created:  # 201
        LOGGER.warning('Request to %s returned %s %s', url, response.status_code, response.reason)
        LOGGER.warning('Response: %s', response.text)
        return
    try:
        LOGGER.info(response.json())
    except ValueError:  # JSON could not be decoded
        LOGGER.warning('Could not parse JSON from response: %s', response.text)
        return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--debug', default='INFO', action='store_const', const='DEBUG')
    subparsers = parser.add_subparsers(help='Use <subcommand> --help for more information')

    # Reingest subparser
    parser_reingest = subparsers.add_parser('reingest', help='Reingest an AIP')
    parser_reingest.set_defaults(func=reingest)
    parser_reingest.add_argument('aip_uuid', help='UUID of the AIP to reingest')
    parser_reingest.add_argument('pipeline', help='Pipeline to reingest this on.')
    parser_reingest.add_argument('reingest_type', choices=['metadata', 'objects', 'full'], help='Type of reingest to start')

    parser_reingest.add_argument('--config', '-c', default='default', help='Processing config to use for a full reingest.')
    parser_reingest.add_argument('--ss-url', '-s', metavar='URL', help='Storage Service URL. Default: http://127.0.0.1:8000', default='http://127.0.0.1:8000')

    parser_reingest.add_argument('--am-url', '-a', metavar='URL', help='Archivematica URL. Default: http://127.0.0.1', default='http://127.0.0.1')
    parser_reingest.add_argument('-u', '--user', metavar='USERNAME', help='Username of the dashboard user to authenticate as.')
    parser_reingest.add_argument('-k', '--api-key', metavar='KEY', help='API key of the dashboard user.')

    # Add metadata subparser
    parser_metadata = subparsers.add_parser('metadata', help='Add metadata to the AIP')
    parser_metadata.set_defaults(func=metadata)
    parser_metadata.add_argument('sip_uuid', help='UUID of the SIP to add metadata to')
    parser_metadata.add_argument('paths', nargs='+', help='Paths to add. Format is <location uuid>:<path within location> E.g. 1250af65-57ff-4dbd-beef-0c487708e761:SampleTransfers/CSVmetadata/metadata/metadata.csv')

    parser_metadata.add_argument('--am-url', '-a', metavar='URL', help='Archivematica URL. Default: http://127.0.0.1', default='http://127.0.0.1')
    parser_metadata.add_argument('-u', '--user', metavar='USERNAME', required=True, help='Username of the dashboard user to authenticate as.')
    parser_metadata.add_argument('-k', '--api-key', metavar='KEY', required=True, help='API key of the dashboard user.')

    args = parser.parse_args()

    # Configure logging
    CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '%(levelname)-8s  %(asctime)s  %(filename)s:%(lineno)-4s %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
            },
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': os.path.join(os.path.abspath(os.path.dirname(__file__)), 'automate-reingest.log'),
                'backupCount': 2,
                'maxBytes': 10 * 1024,
            },
        },
        'loggers': {
            'reingest': {
                'level': args.debug,  # One of INFO, DEBUG, WARNING, ERROR, CRITICAL
                'handlers': ['console', 'file'],
            },
        },
    }
    logging.config.dictConfig(CONFIG)

    if args.func == reingest:
        sys.exit(args.func(
            args.ss_url,
            args.aip_uuid,
            args.pipeline,
            args.reingest_type,
            args.config,
            args.am_url,
            args.user,
            args.api_key,
        ))
    elif args.func == metadata:
        sys.exit(args.func(
            args.sip_uuid,
            args.paths,
            args.am_url,
            args.user,
            args.api_key,
        ))
    else:
        LOGGER.error('Error selecting a function. Exiting.')
        sys.exit(-1)
