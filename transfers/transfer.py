#!/usr/bin/env python
"""
Automate Transfers

Helper script to automate running transfers through Archivematica.
"""

from __future__ import print_function, unicode_literals
import argparse
import ast
import base64
import datetime
from dateutil.parser import parse
import logging
import logging.config  # Has to be imported separately
import os
import requests
from six.moves import configparser
from sqlalchemy import inspect
import subprocess
import sys
import time

from . import models

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

LOGGER = logging.getLogger('transfer')

CONFIG_FILE = None


def get_setting(setting, default=None):
    config = configparser.SafeConfigParser()
    try:
        config.read(CONFIG_FILE)
        return config.get('transfers', setting)
    except Exception:
        return default


def setup(config_file, log_level):
    global CONFIG_FILE
    CONFIG_FILE = config_file
    models.init(get_setting('databasefile', os.path.join(THIS_DIR, 'transfers.db')))

    # Configure logging
    default_logfile = os.path.join(THIS_DIR, 'automate-transfer.log')
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
                'filename': get_setting('logfile', default_logfile),
                'backupCount': 2,
                'maxBytes': 10 * 1024,
            },
        },
        'loggers': {
            'transfer': {
                'level': log_level,
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


def get_status(am_url, am_user, am_api_key, unit_uuid, unit_type, session, hide_on_complete=False):
    """
    Get status of the SIP or Transfer with unit_uuid.

    :param str unit_uuid: UUID of the unit to query for.
    :param str unit_type: 'ingest' or 'transfer'
    :param bool hide_on_complete: If True, hide the unit in the dashboard if COMPLETE
    :returns: Dict with status of the unit from Archivematica or None.
    """
    # Get status
    url = am_url + '/api/' + unit_type + '/status/' + unit_uuid + '/'
    params = {'username': am_user, 'api_key': am_api_key}
    unit_info = _call_url_json(url, params)

    # If complete, hide in dashboard
    if hide_on_complete and unit_info and unit_info['status'] == 'COMPLETE':
        LOGGER.info('Hiding %s %s in dashboard', unit_type, unit_uuid)
        url = am_url + '/api/' + unit_type + '/' + unit_uuid + '/delete/'
        LOGGER.debug('Method: DELETE; URL: %s; params: %s;', url, params)
        response = requests.delete(url, params=params)
        LOGGER.debug('Response: %s', response)

    # If Transfer is complete, get the SIP's status
    if unit_info and unit_type == 'transfer' and unit_info['status'] == 'COMPLETE' and unit_info['sip_uuid'] != 'BACKLOG':
        LOGGER.info('%s is a complete transfer, fetching SIP %s status.', unit_uuid, unit_info['sip_uuid'])
        # Update DB to refer to this one
        db_unit = session.query(models.Unit).filter_by(unit_type=unit_type, uuid=unit_uuid).one()
        db_unit.unit_type = 'ingest'
        db_unit.uuid = unit_info['sip_uuid']
        # Get SIP status
        url = am_url + '/api/ingest/status/' + unit_info['sip_uuid'] + '/'
        unit_info = _call_url_json(url, params)

        # If complete, hide in dashboard
        if hide_on_complete and unit_info and unit_info['status'] == 'COMPLETE':
            LOGGER.info('Hiding SIP %s in dashboard', db_unit.uuid)
            url = am_url + '/api/ingest/' + db_unit.uuid + '/delete/'
            LOGGER.debug('Method: DELETE; URL: %s; params: %s;', url, params)
            response = requests.delete(url, params=params)
            LOGGER.debug('Response: %s', response)

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
    try:
        p = subprocess.Popen([script_path, dirname], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception:
        LOGGER.info('Error when trying to run %s', script_path)
        return None
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
        if not os.path.isfile(script_path):
            LOGGER.info('%s is not a file, skipping', script)
            continue
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


def get_next_transfer(ss_url, ss_user, ss_api_key, ts_location_uuid, path_prefix, depth, completed, see_files, transfer_start_times=None):
    """
    Helper to find the first directory that doesn't have an associated transfer.

    :param ss_url: URL of the Storage Sevice to query
    :param ss_user: User on the Storage Service for authentication
    :param ss_api_key: API key for user on the Storage Service for authentication
    :param ts_location_uuid: UUID of the transfer source Location
    :param path_prefix: Relative path inside the Location to work with.
    :param depth: Depth relative to path_prefix to create a transfer from. Should be 1 or greater.
    :param set completed: Set of the paths of completed transfers. Ideally, relative to the same transfer source location, including the same path_prefix, and at the same depth.
    :param bool see_files: Return files as well as folders to become transfers.
    :param transfer_start_times: List of objects with attributes 'path' and 'started_timestamp'. Likely a SQLAlchemy query, but could also be a NamedTuple.
    :returns: Path relative to TS Location of the new transfer
    """
    # Get sorted list from source dir
    url = ss_url + '/api/v2/location/' + ts_location_uuid + '/browse/'
    params = {
        'username': ss_user,
        'api_key': ss_api_key,
    }
    if path_prefix:
        params['path'] = base64.b64encode(path_prefix)
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
        if not entries:
            LOGGER.info("All potential transfers in %s have been created. Checking for updated transfers", path_prefix)
            if not transfer_start_times:
                LOGGER.info('Starting times for transfers not provided.')
                return None
            # Find transfers whose started_timestamp is newer than the one provided by browse
            browse_timestamps = {
                os.path.join(path_prefix, base64.b64decode(e)): parse(d['timestamp'])
                for e, d in browse_info['properties'].items()
                if 'timestamp' in d
            }
            LOGGER.debug('browse_timestamps: %s', browse_timestamps)
            LOGGER.debug('transfer_start_times: %s', list(transfer_start_times))
            updated = {
                e.path
                for e in transfer_start_times
                if e.started_timestamp and e.path in browse_timestamps and browse_timestamps[e.path] > e.started_timestamp}
            LOGGER.debug('Updated transfer candidates: %s', updated)
            entries = updated
        if not entries:
            return None
        # Sort, take the first
        entries = sorted(list(entries))
        target = entries[0]
        return target
    else:  # if depth > 1
        # Recurse on each directory
        for e in entries:
            LOGGER.debug('New path: %s', e)
            target = get_next_transfer(ss_url, ss_user, ss_api_key, ts_location_uuid, e, depth - 1, completed, see_files, transfer_start_times)
            if target:
                return target
    return None


def create_or_update_unit(session, path, **kwargs):
    """
    Create a new Unit, or update an existing one with the same path.

    :param session: SQLAlchemy session with the DB
    :param path: Path for the new Unit, or Unit to be updated
    :parma kwargs: Other attributes for the new Unit. Should be attributes of Unit.
    :return: New or updated transfer
    """
    unit_attrs = [c.key for c in inspect(models.Unit).attrs if c.key not in ('id', 'path',)]
    params = {k: v for k, v in kwargs.items() if k in unit_attrs}
    params['path'] = path
    try:
        new_unit = session.query(models.Unit).filter_by(path=path)[0]
        for attr, value in params.items():
            setattr(new_unit, attr, value)
    except IndexError:
        new_unit = models.Unit(**params)
    new_unit = session.merge(new_unit)
    return new_unit


def start_transfer(ss_url, ss_user, ss_api_key, ts_location_uuid, ts_path, depth, am_url, am_user, am_api_key, transfer_type, see_files, session):
    """
    Starts a new transfer.

    :param ss_url: URL of the Storage Sevice to query
    :param ss_user: User on the Storage Service for authentication
    :param ss_api_key: API key for user on the Storage Service for authentication
    :param ts_location_uuid: UUID of the transfer source Location
    :param ts_path: Relative path inside the Location to work with.
    :param depth: Depth relative to ts_path to create a transfer from. Should be 1 or greater.
    :param am_url: URL of Archivematica pipeline to start transfer on
    :param am_user: User on Archivematica for authentication
    :param am_api_key: API key for user on Archivematica for authentication
    :param bool see_files: If true, start transfers from files as well as directories
    :param session: SQLAlchemy session with the DB
    :returns: Tuple of Transfer information about the new transfer or None on error.
    """
    # Start new transfer
    completed = {x[0] for x in session.query(models.Unit.path).all()}
    transfer_start_times = session.query(models.Unit.path, models.Unit.started_timestamp)
    target = get_next_transfer(ss_url, ss_user, ss_api_key, ts_location_uuid, ts_path, depth, completed, see_files, transfer_start_times)
    if not target:
        LOGGER.warning("All potential transfers in %s have been created. Exiting", ts_path)
        return None
    LOGGER.info("Starting with %s", target)
    # Get accession ID
    accession = get_accession_id(target)
    LOGGER.info("Accession ID: %s", accession)
    # Start transfer
    url = am_url + '/api/transfer/start_transfer/'
    params = {'username': am_user, 'api_key': am_api_key}
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
        LOGGER.error('Response: %s', resp_json)
        new_transfer = create_or_update_unit(session, path=target, unit_type='transfer', status='FAILED', current=False, started_timestamp=datetime.datetime.now())
        return None

    # Run all scripts in pre-transfer directory
    # TODO what inputs do we want?
    run_scripts(
        'pre-transfer',
        resp_json['path'],  # Absolute path
        'standard',  # Transfer type
    )

    # Approve transfer
    LOGGER.info("Ready to start")
    retry_count = 3
    for i in range(retry_count):
        result = approve_transfer(target_name, am_url, am_api_key, am_user)
        # Mark as started
        if result:
            LOGGER.info('Approved %s', result)
            new_transfer = create_or_update_unit(session, path=target, uuid=result, unit_type='transfer', current=True, started_timestamp=datetime.datetime.now())
            LOGGER.info('New transfer: %s', new_transfer)
            break
        LOGGER.info('Failed approve, try %s of %s', i + 1, retry_count)
    else:
        LOGGER.warning('Not approved')
        new_transfer = create_or_update_unit(session, path=target, uuid=None, unit_type='transfer', current=False, started_timestamp=datetime.datetime.now())
        return None

    LOGGER.info('Finished %s', target)
    return new_transfer


def approve_transfer(directory_name, url, am_api_key, am_user):
    """
    Approve transfer with directory_name.

    :returns: UUID of the approved transfer or None.
    """
    LOGGER.info("Approving %s", directory_name)
    time.sleep(6)
    # List available transfers
    get_url = url + "/api/transfer/unapproved"
    params = {'username': am_user, 'api_key': am_api_key}
    waiting_transfers = _call_url_json(get_url, params)
    if waiting_transfers is None:
        LOGGER.warning('No waiting transfer ')
        return None
    for a in waiting_transfers['results']:
        LOGGER.debug("Found waiting transfer: %s", a['directory'])
        if fsencode(a['directory']) == directory_name:
            # Post to approve transfer
            post_url = url + "/api/transfer/approve/"
            params = {'username': am_user, 'api_key': am_api_key, 'type': a['type'], 'directory': directory_name}
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


def main(am_user, am_api_key, ss_user, ss_api_key, ts_uuid, ts_path, depth, am_url, ss_url, transfer_type, see_files, hide_on_complete=False, config_file=None, log_level='INFO'):

    setup(config_file, log_level)
    LOGGER.info("Waking up")

    session = models.Session()

    # Check for evidence that this is already running
    default_pidfile = os.path.join(THIS_DIR, 'pid.lck')
    pid_file = get_setting('pidfile', default_pidfile)
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
        current_unit = session.query(models.Unit).filter_by(current=True).one()
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
        status_info = get_status(am_url, am_user, am_api_key, unit_uuid, unit_type, session, hide_on_complete)
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
        run_scripts(
            'user-input',
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
    new_transfer = start_transfer(ss_url, ss_user, ss_api_key, ts_uuid, ts_path, depth, am_url, am_user, am_api_key, transfer_type, see_files, session)

    session.commit()
    os.remove(pid_file)
    return 0 if new_transfer else 1


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-u', '--user', metavar='USERNAME', required=True, help='Username of the Archivematica dashboard user to authenticate as.')
    parser.add_argument('-k', '--api-key', metavar='KEY', required=True, help='API key of the Archivematica dashboard user.')
    parser.add_argument('--ss-user', metavar='USERNAME', required=True, help='Username of the Storage Service user to authenticate as.')
    parser.add_argument('--ss-api-key', metavar='KEY', required=True, help='API key of the Storage Service user.')
    parser.add_argument('-t', '--transfer-source', metavar='UUID', required=True, help='Transfer Source Location UUID to fetch transfers from.')
    parser.add_argument('--transfer-path', metavar='PATH', help='Relative path within the Transfer Source. Default: ""', type=fsencode, default=b'')  # Convert to bytes from unicode str provided by command line
    parser.add_argument('--depth', '-d', help='Depth to create the transfers from relative to the transfer source location and path. Default of 1 creates transfers from the children of transfer-path.', type=int, default=1)
    parser.add_argument('--am-url', '-a', metavar='URL', help='Archivematica URL. Default: http://127.0.0.1', default='http://127.0.0.1')
    parser.add_argument('--ss-url', '-s', metavar='URL', help='Storage Service URL. Default: http://127.0.0.1:8000', default='http://127.0.0.1:8000')
    parser.add_argument('--transfer-type', metavar='TYPE', help="Type of transfer to start. One of: 'standard' (default), 'unzipped bag', 'zipped bag', 'dspace'.", default='standard', choices=['standard', 'unzipped bag', 'zipped bag', 'dspace'])
    parser.add_argument('--files', action='store_true', help='If set, start transfers from files as well as folders.')
    parser.add_argument('--hide', action='store_true', help='If set, hide the Transfers and SIPs in the dashboard once they complete.')
    parser.add_argument('-c', '--config-file', metavar='FILE', help='Configuration file(log/db/PID files)', default=None)

    # Logging
    parser.add_argument('--verbose', '-v', action='count', default=0, help='Increase the debugging output.')
    parser.add_argument('--quiet', '-q', action='count', default=0, help='Decrease the debugging output')
    parser.add_argument('--log-level', choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'], default=None, help='Set the debugging output level. This will override -q and -v')

    args = parser.parse_args()

    log_levels = {
        2: 'ERROR',
        1: 'WARNING',
        0: 'INFO',
        -1: 'DEBUG',
    }
    if args.log_level is None:
        level = args.quiet - args.verbose
        level = max(level, -1)  # No smaller than -1
        level = min(level, 2)  # No larger than 2
        log_level = log_levels[level]
    else:
        log_level = args.log_level

    sys.exit(main(
        am_user=args.user,
        am_api_key=args.api_key,
        ss_user=args.ss_user,
        ss_api_key=args.ss_api_key,
        ts_uuid=args.transfer_source,
        ts_path=args.transfer_path,
        depth=args.depth,
        am_url=args.am_url,
        ss_url=args.ss_url,
        transfer_type=args.transfer_type,
        see_files=args.files,
        hide_on_complete=args.hide,
        config_file=args.config_file,
        log_level=log_level,
    ))
