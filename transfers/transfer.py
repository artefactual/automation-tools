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
import datetime
from docopt import docopt
import json
import os
import requests
import sys
import time


def get_status(am_url, user, api_key, unit_uuid, unit_type, last_unit_file):
    """ Get status of the SIP or Transfer with unit_uuid.

    :param str unit_uuid: UUID of the unit to query for.
    :param str unit_type: 'ingest' or 'transfer'
    :returns: Status of the unit from Archivematica or None.
    """
    # Get status
    url = am_url + '/api/' + unit_type + '/status/' + unit_uuid + '/'
    params = {'user': user, 'api_key': api_key}
    print('url', url, 'params', params)
    response = requests.get(url, params=params)
    print('response', response)
    if not response.ok:
        return None
    unit_info = response.json()

    # If Transfer is complete, get the SIP's status
    if unit_type == 'transfer' and unit_info['status'] == 'COMPLETE' and unit_info['sip_uuid'] != 'BACKLOG':
        # Update last_unit to refer to this one
        with open(last_unit_file, 'w') as f:
            print(unit_info['sip_uuid'], 'ingest', file=f)
        # Get SIP status
        url = am_url + '/api/ingest/status/' + unit_info['sip_uuid'] + '/'
        response = requests.get(url, params=params)
        print('response', response)
        if not response.ok:
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
        print('Unable to browse transfer source location', ts_location_uuid, file=sys.stderr)
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
    print("Starting with", target)
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
    print("Ready to start")
    result = approve_transfer(target, am_url, api_key, user_name)
    # TODO Mark as started
    if result:
        start_at = start_at + 1
        print('Approved', result)
        with open(count_file, 'w') as f:
            print(start_at, file=f)
        with open(last_unit_file, 'w') as f:
            print(result, 'transfer', file=f)
    else:
        print('Not approved')

    print("Finished " + target + " at " + str(datetime.datetime.now()))
    print(" ")


def list_transfers(url, api_key, user_name):
    get_url = url + "/api/transfer/unapproved"
    g = requests.get(get_url, params={'username': user_name, 'api_key': api_key})
    return g.json()


def approve_transfer(directory_name, url, api_key, user_name):
    print("Approving " + directory_name)
    time.sleep(6)
    # List available transfers
    post_url = url + "/api/transfer/approve"
    waiting_transfers = list_transfers(url, api_key, user_name)
    for a in waiting_transfers['results']:
        print("Found waiting transfer: " + a['directory'])
        if a['directory'] == directory_name:
            # Post to approve transfer
            params = {'username': user_name, 'api_key': api_key, 'type': a['type'], 'directory': directory_name}
            r = requests.post(post_url, data=params)
            if r.status_code != 200:
                return False
            return a['uuid']
        else:
            print(a['directory'] + " is not what we are looking for")
    else:
        return False

def main(pipeline, user, api_key, ts_uuid, ts_path, am_url, ss_url):
    now = datetime.datetime.now()
    print("Waking up at ", now)
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
    print('Unit UUID', unit_uuid, 'Unit Type', unit_type)
    # Get status
    status = get_status(am_url, user, api_key, unit_uuid, unit_type, last_unit_file)
    print('Status', status)
    if not status:
        sys.exit(1)
    # If processing, exit
    if status == 'PROCESSING':
        print('Last transfer still processing, nothing to do.')
        sys.exit(0)
    # If waiting on input, send email, exit
    elif status == 'USER_INPUT':
        print('Waiting on user input, sending email.')
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
