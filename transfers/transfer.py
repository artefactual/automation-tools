#!/usr/bin/env python2
"""
Automate Transfers

Helper script to automate running transfers through Archivematica.

Usage:
    transfer.py --pipeline=UUID --user=USERNAME --api-key=KEY --transfer-source=UUID [--transfer-path=PATH] [--am-url=URL] [--ss-url=URL]
    transfer.py -h | --help

-p UUID --pipeline UUID         Pipeline UUID to start the transfers on
-u USERNAME --user USERNAME     Username of the dashboard user to authenticate as
-k KEY --api-key KEY            API key of the dashboard user
-t UUID --transfer-source UUID  Transfer Source Location UUID to fetch transfers from
--transfer-path PATH            Relative path within the Transfer Source (optional)
-a URL --am-url URL             Archivematica URL [default: http://127.0.0.1]
-s URL --ss-url URL             Storage Service URL [default: http://127.0.0.1:8000]
--transfer-type TYPE            Type of transfer to start. Unimplemented.
--files                         Start transfers from files as well as folders. Unimplemeted. [default: False]
"""

from __future__ import print_function
import base64
from docopt import docopt
import json
import logging
import logging.config  # Has to be imported separately
import os
import requests
import sys
import time

LOGGER = logging.getLogger('transfer')
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
            'filename': os.path.join(os.path.abspath(os.path.dirname(__file__)), 'automate-transfer.log'),
            'backupCount': 2,
            'maxBytes': 10 * 1024,
        },
    },
    'loggers': {
        'transfer': {
            'level': 'INFO',  # One of INFO, DEBUG, WARNING, ERROR, CRITICAL
            'handlers': ['console'],
        },
    },
}
logging.config.dictConfig(CONFIG)


def get_status(am_url, user, api_key, unit_uuid, unit_type, last_unit_file):
    """ Get status of the SIP or Transfer with unit_uuid.

    :param str unit_uuid: UUID of the unit to query for.
    :param str unit_type: 'ingest' or 'transfer'
    :returns: Status of the unit from Archivematica or None.
    """
    # Get status
    url = am_url + '/api/' + unit_type + '/status/' + unit_uuid + '/'
    params = {'user': user, 'api_key': api_key}
    LOGGER.debug('URL: %s; params: %s;', url, params)
    response = requests.get(url, params=params)
    LOGGER.debug('Response: %s', response)
    if not response.ok:
        LOGGER.warning('Request to %s returned %s %s', url, response.status_code, response.reason)
        return None
    unit_info = response.json()

    # If Transfer is complete, get the SIP's status
    if unit_type == 'transfer' and unit_info['status'] == 'COMPLETE' and unit_info['sip_uuid'] != 'BACKLOG':
        LOGGER.info('%s is a complete transfer, fetching SIP %s status.', unit_uuid, unit_info['sip_uuid'])
        # Update last_unit to refer to this one
        with open(last_unit_file, 'w') as f:
            print(unit_info['sip_uuid'], 'ingest', file=f)
        # Get SIP status
        url = am_url + '/api/ingest/status/' + unit_info['sip_uuid'] + '/'
        LOGGER.debug('URL: %s; params: %s;', url, params)
        response = requests.get(url, params=params)
        LOGGER.debug('Response: %s', response)
        if not response.ok:
            LOGGER.warning('Request to %s returned %s %s', url, response.status_code, response.reason)
            return None
        unit_info = response.json()

    return unit_info.get('status')

def send_email():
    pass


def start_transfer(ss_url, ts_location_uuid, ts_path, pipeline_uuid, am_url, user_name, api_key, last_unit_file):
    # Start new transfer
    # Get sorted list from source dir
    url = ss_url + '/api/v2/location/' + ts_location_uuid + '/browse/'
    params = {}
    if ts_path:
        params = {'path': base64.b64encode(ts_path)}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        LOGGER.error('Unable to browse transfer source location %s', ts_location_uuid)
        sys.exit(1)
    dirs = response.json()['directories']
    dirs = map(base64.b64decode, dirs)

    # Find first one not already started (store in DB?)
    # TODO keep a list of what's be processed and compare the fetched list against that
    count_file = "count"
    try:
        with open(count_file, 'r') as f:
            last_transfer = int(f.readline())
    except Exception:
        last_transfer = 0
    start_at = last_transfer
    target = dirs[start_at]
    LOGGER.info("Starting with %s", target)
    # Get CP Loc UUID
    url = ss_url + '/api/v2/location/'
    params = {'pipeline__uuid': pipeline_uuid, 'purpose': 'CP'}
    response = requests.get(url, params=params)
    resource_uri = response.json()['objects'][0]['resource_uri']
    # Copy to pipeline
    url = ss_url + resource_uri
    source = os.path.join(ts_path, target)
    destination = os.path.join("watchedDirectories", "activeTransfers", "standardTransfer", target)
    data = {
        'origin_location': '/api/v2/location/' + ts_location_uuid + '/',
        'pipeline': pipeline_uuid,
        'files': [{'source': source, 'destination': destination}]
    }
    response = requests.post(url, data=json.dumps(data), headers={'Content-Type': 'application/json'})

    # Run munging scripts (What params to scripts?)
    # Update default config
    # TODO pick correct processingMCP based on existence of access dir
    # processing_available = '/home/users/hbecker/archivematica/src/MCPServer/share/sharedDirectoryStructure/sharedMicroServiceTasksConfigs/processingMCPConfigs/'
    # shutil.copyfile(processing_available + "defaultProcessingMCP.xml", destination + "/processingMCP.xml")

    # Approve transfer
    LOGGER.info("Ready to start")
    result = approve_transfer(target, am_url, api_key, user_name)
    # TODO Mark as started
    if result:
        start_at = start_at + 1
        LOGGER.info('Approved %s', result)
        with open(count_file, 'w') as f:
            print(start_at, file=f)
        with open(last_unit_file, 'w') as f:
            print(result, 'transfer', file=f)
    else:
        LOGGER.warning('Not approved')

    LOGGER.info('Finished %s', target)


def list_transfers(url, api_key, user_name):
    get_url = url + "/api/transfer/unapproved"
    g = requests.get(get_url, params={'username': user_name, 'api_key': api_key})
    return g.json()


def approve_transfer(directory_name, url, api_key, user_name):
    LOGGER.info("Approving %s", directory_name)
    time.sleep(6)
    # List available transfers
    post_url = url + "/api/transfer/approve"
    waiting_transfers = list_transfers(url, api_key, user_name)
    for a in waiting_transfers['results']:
        LOGGER.info("Found waiting transfer: %s", a['directory'])
        if a['directory'] == directory_name:
            # Post to approve transfer
            params = {'username': user_name, 'api_key': api_key, 'type': a['type'], 'directory': directory_name}
            r = requests.post(post_url, data=params)
            if r.status_code != 200:
                return False
            return a['uuid']
        else:
            LOGGER.debug("%s is not what we are looking for", a['directory'])
    else:
        return False

def main(pipeline, user, api_key, ts_uuid, ts_path, am_url, ss_url):
    LOGGER.info("Waking up")
    # FIXME Set the cwd to the same as this file so count_file works
    this_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    os.chdir(this_dir)

    # Check status of last unit
    # Find last unit started
    last_unit_file = 'last_unit'
    try:
        with open(last_unit_file, 'r') as f:
            last_unit = f.readline()
        unit_uuid, unit_type = last_unit.split()
    except Exception:
        unit_uuid = unit_type = ''
    LOGGER.info('Last unit: %s %s', unit_type, unit_uuid)
    # Get status
    status = get_status(am_url, user, api_key, unit_uuid, unit_type, last_unit_file)
    LOGGER.info('Status: %s', status)
    if not status:
        LOGGER.error('Could not fetch status for %s. Exiting.', unit_uuid)
        sys.exit(1)
    # If processing, exit
    if status == 'PROCESSING':
        LOGGER.info('Last transfer still processing, nothing to do.')
        sys.exit(0)
    # If waiting on input, send email, exit
    elif status == 'USER_INPUT':
        LOGGER.info('Waiting on user input, sending email.')
        # TODO only do this one per USER_INPUT per unit
        # TODO change this to run scripts in a dir?
        send_email()
        sys.exit(0)
    # If failed, rejected, completed etc, start new transfer
    start_transfer(ss_url, ts_uuid, ts_path, pipeline, am_url, user, api_key, last_unit_file)

if __name__ == '__main__':
    args = docopt(__doc__)
    sys.exit(main(
        pipeline=args['--pipeline'],
        user=args['--user'],
        api_key=args['--api-key'],
        ts_uuid=args['--transfer-source'],
        ts_path=args['--transfer-path'],
        am_url=args['--am-url'],
        ss_url=args['--ss-url'],
    ))
