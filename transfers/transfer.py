#!/usr/bin/env python
"""
Automate Transfers

Helper script to automate running transfers through Archivematica.
"""

from __future__ import print_function, unicode_literals
import argparse
import ast
import base64
import logging
import logging.config  # Has to be imported separately
import os
import requests
import subprocess
import sys
import time

try:
    from os import fsencode, fsdecode
except ImportError:
    # Cribbed & modified from Python3's OS module to support Python2
    def fsencode(filename):
        encoding = sys.getfilesystemencoding()
        if isinstance(filename, str):
            return filename
        elif isinstance(filename, unicode):
            return filename.encode(encoding)
        else:
            raise TypeError("expect bytes or str, not %s" % type(filename).__name__)

    def fsdecode(filename):
        encoding = sys.getfilesystemencoding()
        if isinstance(filename, unicode):
            return filename
        elif isinstance(filename, str):
            return filename.decode(encoding)
        else:
            raise TypeError("expect bytes or str, not %s" % type(filename).__name__)

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(THIS_DIR)
from models import Unit, Session

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
            'handlers': ['console', 'file'],
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


def get_status(am_url, user, api_key, unit_uuid, unit_type, session):
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
        # Update DB to refer to this one
        db_unit = session.query(Unit).filter_by(unit_type=unit_type, uuid=unit_uuid).one()
        db_unit.unit_type = 'ingest'
        db_unit.uuid = unit_info['sip_uuid']
        # Get SIP status
        url = am_url + '/api/ingest/status/' + unit_info['sip_uuid'] + '/'
        unit_info = _call_url_json(url, params)
    return unit_info


def get_accession_id(dirname):
    """
    Call get-accession-number and return literal_eval stdout as accession ID.

    get-accession-number should be in the same directory as transfer.py. Its
    only output to stdout should be the accession number surrounded by
    quotes.  Eg. "accession number"

    :param str dirname: Directory name of folder to become transfer
    :returns: accession number or None.
    """
    script_path = os.path.join(THIS_DIR, 'get-accession-number')
    p = subprocess.Popen([script_path, dirname], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()
    if p.returncode != 0:
        LOGGER.info('Error running %s %s: RC: %s; stdout: %s; stderr: %s', script_path, dirname, p.returncode, output, err)
        return None
    output = fsdecode(output)
    try:
        return ast.literal_eval(output)
    except Exception:
        LOGGER.info('Unable to parse output from %s. Output: %s', script_path, output)
        return None


def run_scripts(directory, *args):
    """
    Run all executable scripts in directory relative to this file.

    :param str directory: Directory in the same folder as this file to run scripts from.
    :param args: All other parameters will be passed to called scripts.
    :return: None
    """
    directory = os.path.join(THIS_DIR, directory)
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


def get_next_transfer(ss_url, ts_location_uuid, path_prefix, depth, completed, see_files):
    """
    Helper to find the first directory that doesn't have an associated transfer.

    :param ss_url: URL of the Storage Sevice to query
    :param ts_location_uuid: UUID of the transfer source Location
    :param path_prefix: Relative path inside the Location to work with.
    :param depth: Depth relative to path_prefix to create a transfer from. Should be 1 or greater.
    :param set completed: Set of the paths of completed transfers. Ideally, relative to the same transfer source location, including the same path_prefix, and at the same depth.
    :param bool see_files: Return files as well as folders to become transfers.
    :returns: Path relative to TS Location of the new transfer
    """
    # Get sorted list from source dir
    url = ss_url + '/api/v2/location/' + ts_location_uuid + '/browse/'
    params = {}
    if path_prefix:
        params = {'path': base64.b64encode(path_prefix)}
    browse_info = _call_url_json(url, params)
    if browse_info is None:
        return None
    if see_files:
        entries = browse_info['entries']
    else:
        entries = browse_info['directories']
    entries = [base64.b64decode(e.encode('utf8')) for e in entries]
    LOGGER.debug('Entries: %s', entries)
    entries = [os.path.join(path_prefix, e) for e in entries]
    # If at the correct depth, check if any of these have not been made into transfers yet
    if depth <= 1:
        # Find the directories that are not already in the DB using sets
        entries = set(entries) - completed
        LOGGER.debug("New transfer candidates: %s", entries)
        # Sort, take the first
        entries = sorted(list(entries))
        if not entries:
            LOGGER.info("All potential transfers in %s have been created.", path_prefix)
            return None
        target = entries[0]
        return target
    else:  # if depth > 1
        # Recurse on each directory
        for e in entries:
            LOGGER.debug('New path: %s', e)
            target = get_next_transfer(ss_url, ts_location_uuid, e, depth - 1, completed, see_files)
            if target:
                return target
    return None


def start_transfer(ss_url, ts_location_uuid, ts_path, depth, am_url, user_name, api_key, transfer_type, see_files, session):
    """
    Starts a new transfer.

    :param ss_url: URL of the Storage Sevice to query
    :param ts_location_uuid: UUID of the transfer source Location
    :param ts_path: Relative path inside the Location to work with.
    :param depth: Depth relative to ts_path to create a transfer from. Should be 1 or greater.
    :param am_url: URL of Archivematica pipeline to start transfer on
    :param user_name: User on Archivematica for authentication
    :param api_key: API key for user on Archivematica for authentication
    :param bool see_files: If true, start transfers from files as well as directories
    :param session: SQLAlchemy session with the DB
    :returns: Tuple of Transfer information about the new transfer or None on error.
    """
    # Start new transfer
    completed = {x[0] for x in session.query(Unit.path).all()}
    target = get_next_transfer(ss_url, ts_location_uuid, ts_path, depth, completed, see_files)
    if not target:
        LOGGER.warning("All potential transfers in %s have been created. Exiting", ts_path)
        return None
    LOGGER.info("Starting with %s", target)
    # Get accession ID
    accession = get_accession_id(target)
    LOGGER.info("Accession ID: %s", accession)
    # Start transfer
    url = am_url + '/api/transfer/start_transfer/'
    params = {'user': user_name, 'api_key': api_key}
    target_name = os.path.basename(target)
    data = {
        'name': target_name,
        'type': transfer_type,
        'accession': accession,
        'paths[]': [base64.b64encode(fsencode(ts_location_uuid) + b':' + target)],
        'row_ids[]': [''],
    }
    LOGGER.debug('URL: %s; Params: %s; Data: %s', url, params, data)
    response = requests.post(url, params=params, data=data)
    LOGGER.debug('Response: %s', response)
    try:
        resp_json = response.json()
    except ValueError:
        LOGGER.warning('Could not parse JSON from response: %s', response.text)
        return None
    if not response.ok or resp_json.get('error'):
        LOGGER.error('Unable to start transfer.')
        LOGGER.debug('Response: %s', resp_json)
        new_transfer = Unit(path=target, unit_type='transfer', status='FAILED', current=False)
        session.add(new_transfer)
        return None

    # Run all scripts in pre-transfer directory
    # TODO what inputs do we want?
    run_scripts('pre-transfer',
        resp_json['path'],  # Absolute path
        'standard',  # Transfer type
    )

    # Approve transfer
    LOGGER.info("Ready to start")
    retry_count = 3
    for i in range(retry_count):
        result = approve_transfer(target_name, am_url, api_key, user_name)
        # Mark as started
        if result:
            LOGGER.info('Approved %s', result)
            new_transfer = Unit(uuid=result, path=target, unit_type='transfer', current=True)
            LOGGER.info('New transfer: %s', new_transfer)
            session.add(new_transfer)
            break
        LOGGER.info('Failed approve, try %s of %s', i + 1, retry_count)
    else:
        LOGGER.warning('Not approved')
        new_transfer = Unit(uuid=None, path=target, unit_type='transfer', current=False)
        session.add(new_transfer)
        return None

    LOGGER.info('Finished %s', target)
    return new_transfer


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
        if fsencode(a['directory']) == directory_name:
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

def main(user, api_key, ts_uuid, ts_path, depth, am_url, ss_url, transfer_type, see_files):
    LOGGER.info("Waking up")
    session = Session()

    # Check for evidence that this is already running
    pid_file = os.path.join(THIS_DIR, 'pid.lck')
    try:
        # Open PID file only if it doesn't exist for read/write
        f = os.fdopen(os.open(pid_file, os.O_CREAT | os.O_EXCL | os.O_RDWR), 'r+')
    except:
        LOGGER.info('This script is already running. To override this behaviour and start a new run, remove %s', pid_file)
        return 0
    else:
        pid = os.getpid()
        f.write(str(pid))
        f.close()

    # Check status of last unit
    current_unit = None
    try:
        current_unit = session.query(Unit).filter_by(current=True).one()
        unit_uuid = current_unit.uuid
        unit_type = current_unit.unit_type
    except Exception:
        LOGGER.debug('No current unit', exc_info=True)
        unit_uuid = unit_type = ''
        LOGGER.info('Current unit: unknown.  Assuming new run.')
        status = 'UNKNOWN'
    else:
        LOGGER.info('Current unit: %s', current_unit)
        # Get status
        status_info = get_status(am_url, user, api_key, unit_uuid, unit_type, session)
        LOGGER.info('Status info: %s', status_info)
        if not status_info:
            LOGGER.error('Could not fetch status for %s. Exiting.', unit_uuid)
            os.remove(pid_file)
            return 1
        status = status_info.get('status')
        current_unit.status = status
    # If processing, exit
    if status == 'PROCESSING':
        LOGGER.info('Current transfer still processing, nothing to do.')
        session.commit()
        os.remove(pid_file)
        return 0
    # If waiting on input, send email, exit
    elif status == 'USER_INPUT':
        LOGGER.info('Waiting on user input, running scripts in user-input directory.')
        # TODO What inputs do we want?
        microservice = status_info.get('microservice', '')
        run_scripts('user-input',
            microservice,  # Current microservice name
            str(microservice != current_unit.microservice),  # String True or False if this is the first time at this wait point
            status_info['path'],  # Absolute path
            status_info['uuid'],  # SIP/Transfer UUID
            status_info['name'],  # SIP/Transfer name
            status_info['type'],  # SIP or transfer
        )
        current_unit.microservice = microservice
        session.commit()
        os.remove(pid_file)
        return 0
    # If failed, rejected, completed etc, start new transfer
    if current_unit:
        current_unit.current = False
    new_transfer = start_transfer(ss_url, ts_uuid, ts_path, depth, am_url, user, api_key, transfer_type, see_files, session)

    session.commit()
    os.remove(pid_file)
    return 0 if new_transfer else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-u', '--user', metavar='USERNAME', required=True, help='Username of the dashboard user to authenticate as.')
    parser.add_argument('-k', '--api-key', metavar='KEY', required=True, help='API key of the dashboard user.')
    parser.add_argument('-t', '--transfer-source', metavar='UUID', required=True, help='Transfer Source Location UUID to fetch transfers from.')
    parser.add_argument('--transfer-path', metavar='PATH', help='Relative path within the Transfer Source. Default: ""', type=fsencode, default=b'')  # Convert to bytes from unicode str provided by command line
    parser.add_argument('--depth', '-d', help='Depth to create the transfers from relative to the transfer source location and path. Default of 1 creates transfers from the children of transfer-path.', type=int, default=1)
    parser.add_argument('--am-url', '-a', metavar='URL', help='Archivematica URL. Default: http://127.0.0.1', default='http://127.0.0.1')
    parser.add_argument('--ss-url', '-s', metavar='URL', help='Storage Service URL. Default: http://127.0.0.1:8000', default='http://127.0.0.1:8000')
    parser.add_argument('--transfer-type', metavar='TYPE', help="Type of transfer to start. One of: 'standard' (default), 'unzipped bag', 'zipped bag', 'dspace'.", default='standard', choices=['standard', 'unzipped bag', 'zipped bag', 'dspace'])
    parser.add_argument('--files', action='store_true', help='If set, start transfers from files as well as folders.')
    args = parser.parse_args()

    sys.exit(main(
        user=args.user,
        api_key=args.api_key,
        ts_uuid=args.transfer_source,
        ts_path=args.transfer_path,
        depth=args.depth,
        am_url=args.am_url,
        ss_url=args.ss_url,
        transfer_type=args.transfer_type,
        see_files=args.files,
    ))
