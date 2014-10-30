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
--transfer-type TYPE            Type of transfer to start. Unimplemented. [default: standard]
--files                         Start transfers from files as well as folders. Unimplemeted. [default: False]
"""

from __future__ import print_function
import base64
from docopt import docopt
import logging
import logging.config  # Has to be imported separately
import os
import requests
import subprocess
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


def get_status(am_url, user, api_key, unit_uuid, unit_type, last_unit_file):
    """
    Get status of the SIP or Transfer with unit_uuid.

    :param str unit_uuid: UUID of the unit to query for.
    :param str unit_type: 'ingest' or 'transfer'
    :returns: Dict with status of the unit from Archivematica or None.
    """
    # Get status
    url = am_url + '/api/' + unit_type + '/status/' + unit_uuid + '/'
    params = {'user': user, 'api_key': api_key}
    unit_info = _call_url_json(url, params)

    # If Transfer is complete, get the SIP's status
    if unit_info and unit_type == 'transfer' and unit_info['status'] == 'COMPLETE' and unit_info['sip_uuid'] != 'BACKLOG':
        LOGGER.info('%s is a complete transfer, fetching SIP %s status.', unit_uuid, unit_info['sip_uuid'])
        # Update last_unit to refer to this one
        with open(last_unit_file, 'w') as f:
            print(unit_info['sip_uuid'], 'ingest', file=f)
        # Get SIP status
        url = am_url + '/api/ingest/status/' + unit_info['sip_uuid'] + '/'
        unit_info = _call_url_json(url, params)
    return unit_info


def run_scripts(directory, *args):
    """
    Run all executable scripts in directory relative to this file.

    :param str directory: Directory in the same folder as this file to run scripts from.
    :param args: All other parameters will be passed to called scripts.
    :return: None
    """
    if not os.path.isdir(directory):
        LOGGER.warning('%s is not a directory. No scripts to run.', directory)
        return
    script_args = list(args)
    LOGGER.debug('script_args: %s', script_args)
    for script in sorted(os.listdir(directory)):
        LOGGER.debug('Script: %s', script)
        script_path = os.path.join(directory, script)
        if not os.access(script_path, os.X_OK):
            LOGGER.info('%s is not executable, skipping', script)
            continue
        LOGGER.info('Running %s "%s"', script_path, '" "'.join(args))
        p = subprocess.Popen([script_path] + script_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        LOGGER.info('Return code: %s', p.returncode)
        LOGGER.info('stdout: %s', stdout)
        if stderr:
            LOGGER.warning('stderr: %s', stderr)


def start_transfer(ss_url, ts_location_uuid, ts_path, pipeline_uuid, am_url, user_name, api_key, last_unit_file):
    # Start new transfer
    # Get sorted list from source dir
    url = ss_url + '/api/v2/location/' + ts_location_uuid + '/browse/'
    params = {}
    if ts_path:
        params = {'path': base64.b64encode(ts_path)}
    browse_info = _call_url_json(url, params)
    if browse_info is None:
        return 1
    dirs = browse_info['directories']
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
    # Start transfer
    url = am_url + '/api/transfer/start_transfer/'
    params = {'user': user_name, 'api_key': api_key}
    data = {
        'name': target,
        'type': 'standard',
        'accession': None,  # TODO set accession number
        'paths[]': [base64.b64encode(ts_location_uuid + ':' + os.path.join(ts_path, target))],
        'row_ids[]': [''],
    }
    LOGGER.debug('URL: %s; Params: %s; Data: %s', url, params, data)
    response = requests.post(url, params=params, data=data)
    LOGGER.debug('Response: %s', response)
    try:
        resp_json = response.json()
    except ValueError:
        LOGGER.warning('Could not parse JSON from response: %s', response.text)
        return 1
    if not response.ok or resp_json.get('error'):
        LOGGER.error('Unable to start transfer.')
        LOGGER.debug('Response: %s', resp_json)
        return 1

    # Run all scripts in pre-transfer directory
    # TODO what inputs do we want?
    run_scripts('pre-transfer',
        resp_json['path'],  # Absolute path
        'standard',  # Transfer type
    )

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


def approve_transfer(directory_name, url, api_key, user_name):
    """
    Approve transfer with directory_name.

    :returns: UUID of the approved transfer or None.
    """
    LOGGER.info("Approving %s", directory_name)
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
        if a['directory'] == directory_name:
            # Post to approve transfer
            post_url = url + "/api/transfer/approve/"
            params = {'username': user_name, 'api_key': api_key, 'type': a['type'], 'directory': directory_name}
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

def main(pipeline, user, api_key, ts_uuid, ts_path, am_url, ss_url):
    LOGGER.info("Waking up")
    # FIXME Set the cwd to the same as this file so count_file works
    this_dir = os.path.dirname(os.path.abspath(__file__))
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
        LOGGER.info('Last unit: unknown.  Assuming new run.')
        status = 'UNKNOWN'
    else:
        LOGGER.info('Last unit: %s %s', unit_type, unit_uuid)
        # Get status
        status_info = get_status(am_url, user, api_key, unit_uuid, unit_type, last_unit_file)
        LOGGER.info('Status info: %s', status_info)
        if not status_info:
            LOGGER.error('Could not fetch status for %s. Exiting.', unit_uuid)
            return 1
        status = status_info.get('status')
    # If processing, exit
    if status == 'PROCESSING':
        LOGGER.info('Last transfer still processing, nothing to do.')
        return 0
    # If waiting on input, send email, exit
    elif status == 'USER_INPUT':
        LOGGER.info('Waiting on user input, running scripts in user-input directory.')
        # TODO What inputs do we want?
        run_scripts('user-input',
            status_info.get('microservice'),  # Microservice name
            status_info['path'],  # Absolute path
            status_info['uuid'],  # SIP/Transfer UUID
            status_info['name'],  # SIP/Transfer name
            status_info['type'],  # SIP or transfer
        )
        return 0
    # If failed, rejected, completed etc, start new transfer
    return start_transfer(ss_url, ts_uuid, ts_path, pipeline, am_url, user, api_key, last_unit_file)

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
