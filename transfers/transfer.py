#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Automate Transfers

Helper script to automate running transfers through Archivematica.
"""

from __future__ import print_function, unicode_literals

import argparse
import ast
import base64
import os
import sys
import subprocess
import time

import urllib3

import requests
from six import binary_type, text_type
from six.moves import configparser

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append('../')

from transfers import defaults
from transfers import loggingconfig
from transfers import models


# Directory for various processing decisions, below.
THIS_DIR = os.path.abspath(os.path.dirname(__file__))


def get_setting(config_file, setting, default=None):
    config = configparser.SafeConfigParser()
    try:
        config.read(config_file)
        return config.get('transfers', setting)
    except Exception:
        return default


def get_logger(log_file_name, log_level):
    return loggingconfig.setup(log_level, log_file_name, "transfer")


# Default logging if no other logging is provided in the class.
LOGGER = get_logger(get_setting('logfile', defaults.TRANSFER_LOG_FILE), "INFO")


try:
    from os import fsencode, fsdecode
except ImportError:
    # Cribbed & modified from Python3's OS module to support Python2
    def fsencode(filename):
        encoding = sys.getfilesystemencoding()
        if isinstance(filename, binary_type):
            return filename
        elif isinstance(filename, text_type):
            return filename.encode(encoding)
        else:
            raise TypeError("expect bytes or str, not %s" %
                            type(filename).__name__)

    def fsdecode(filename):
        encoding = sys.getfilesystemencoding()
        if isinstance(filename, text_type):
            return filename
        elif isinstance(filename, binary_type):
            return filename.decode(encoding)
        else:
            raise TypeError("expect bytes or str, not %s" %
                            type(filename).__name__)


def _call_url_json(url, params, method='GET'):
    """Helper to GET a URL where the expected response is 200 with JSON.
    :param str url: URL to call
    :param dict params: Params to pass to requests.get
    :returns: Dict of the returned JSON or None
    """
    method = method.upper()
    LOGGER.debug('URL: %s; params: %s; method: %s', url, params, method)

    try:
        response = requests.request(method, url=url, params=params)
        LOGGER.debug('Response: %s', response)
        LOGGER.debug('type(response.text): %s ', type(response.text))
        LOGGER.debug('Response content-type: %s',
                     response.headers['content-type'])

        if not response.ok:
            LOGGER.warning('%s Request to %s returned %s %s', method, url,
                           response.status_code, response.reason)
            LOGGER.debug('Response: %s', response.text)
            return None
        try:
            return response.json()
        except ValueError:  # JSON could not be decoded
            LOGGER.warning('Could not parse JSON from response: %s',
                           response.text)
            return None

    except (urllib3.exceptions.NewConnectionError,
            requests.exceptions.ConnectionError) as e:
        LOGGER.error("Connection error %s", e)
        return None


def get_status(am_url, am_user, am_api_key, unit_uuid, unit_type, session,
               hide_on_complete=False):
    """
    Get status of the SIP or Transfer with unit_uuid.

    :param str unit_uuid: UUID of the unit to query for.
    :param str unit_type: 'ingest' or 'transfer'
    :param bool hide_on_complete: Hide the unit in the dashboard if COMPLETE
    :returns: Dict with status of the unit from Archivematica or None.
    """
    # Get status
    url = am_url + '/api/' + unit_type + '/status/' + unit_uuid + '/'
    params = {'username': am_user, 'api_key': am_api_key}
    unit_info = _call_url_json(url, params)

    if unit_info is None:
        return None

    # If complete, hide in dashboard
    if hide_on_complete and unit_info and unit_info['status'] == 'COMPLETE':
        LOGGER.info('Hiding %s %s in dashboard', unit_type, unit_uuid)
        url = am_url + '/api/' + unit_type + '/' + unit_uuid + '/delete/'
        LOGGER.debug('Method: DELETE; URL: %s; params: %s;', url, params)
        response = requests.delete(url, params=params)
        LOGGER.debug('Response: %s', response)

    # If Transfer is complete, get the SIP's status
    if unit_info and unit_type == 'transfer' and \
        unit_info['status'] == 'COMPLETE' and \
            unit_info['sip_uuid'] != 'BACKLOG':
        LOGGER.info('%s is a complete transfer, fetching SIP %s status.',
                    unit_uuid, unit_info['sip_uuid'])
        # Update DB to refer to this one
        db_unit = session.query(models.Unit).filter_by(
            unit_type=unit_type, uuid=unit_uuid).one()
        db_unit.unit_type = 'ingest'
        db_unit.uuid = unit_info['sip_uuid']
        # Get SIP status
        url = am_url + '/api/ingest/status/' + unit_info['sip_uuid'] + '/'
        unit_info = _call_url_json(url, params)

        if unit_info is None:
            return None

        # If complete, hide in dashboard
        if hide_on_complete and unit_info and \
                unit_info['status'] == 'COMPLETE':
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
        p = subprocess.Popen(
            [script_path, dirname], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception:
        LOGGER.info('Error when trying to run %s', script_path)
        return None
    output, err = p.communicate()
    if p.returncode != 0:
        LOGGER.info('Error running %s %s: RC: %s; stdout: %s; stderr: %s',
                    script_path, dirname, p.returncode, output, err)
        return None
    output = fsdecode(output)
    try:
        return ast.literal_eval(output)
    except Exception:
        LOGGER.info(
            'Unable to parse output from %s. Output: %s', script_path, output)
        return None


def run_scripts(directory, *args):
    """
    Run all executable scripts in directory relative to this file.

    :param str directory: Dir in the same folder as this file to run scripts
    :param args: All other parameters will be passed to called scripts.
    :return: None
    """
    directory = os.path.join(THIS_DIR, directory)
    if not os.path.isdir(directory):
        LOGGER.warning('%s is not a directory. No scripts to run.', directory)
        return
    script_args = list(args)
    LOGGER.debug('script_args: %s', script_args)
    script_extensions = get_setting('scriptextensions', '').split(':')
    LOGGER.debug('script_extensions: %s', script_extensions)
    for script in sorted(os.listdir(directory)):
        LOGGER.debug('Script: %s', script)
        script_path = os.path.realpath(os.path.join(directory, script))
        if not os.path.isfile(script_path):
            LOGGER.info('%s is not a file, skipping', script_path)
            continue
        if not os.access(script_path, os.X_OK):
            LOGGER.info('%s is not executable, skipping', script_path)
            continue
        script_name, script_ext = os.path.splitext(script)
        if script_extensions and script_ext not in script_extensions:
            LOGGER.info(("'%s' for '%s' not in configured list of script file "
                         "extensions, skipping", script_ext, script_path))
            continue
        LOGGER.info('Running %s "%s"', script_path, '" "'.join(args))
        p = subprocess.Popen(
            [script_path] + script_args, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        LOGGER.info('Return code: %s', p.returncode)
        LOGGER.info('stdout: %s', stdout)
        if stderr:
            LOGGER.warning('stderr: %s', stderr)


def get_next_transfer(ss_url, ss_user, ss_api_key, ts_location_uuid,
                      path_prefix, depth, completed, see_files):
    """
    Helper to find the first directory that doesn't have an associated
    transfer.

    :param ss_url:           URL of the Storage Sevice to query
    :param ss_user:          User on the Storage Service for authentication
    :param ss_api_key:       API key for user on the Storage Service for
                             authentication
    :param ts_location_uuid: UUID of the transfer source Location
    :param path_prefix:      Relative path inside the Location to work with.
    :param depth:            Depth relative to path_prefix to create a transfer
                             from. Should be 1 or greater.
    :param set completed:    Set of the paths of completed transfers. Ideally,
                             relative to the same transfer source location,
                             including the same path_prefix, and at the same
                             depth.
    :param bool see_files:   Return files as well as folders to become
                             transfers.
    :returns:                Path relative to TS Location of the new transfer.
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
    # If at the correct depth, check if any of these have not been made into
    # transfers yet
    if depth <= 1:
        # Find the directories that are not already in the DB using sets
        entries = set(entries) - completed
        LOGGER.debug("New transfer candidates: %s", entries)
        # Sort, take the first
        entries = sorted(list(entries))
        if not entries:
            LOGGER.info(
                "All potential transfers in %s have been created.",
                path_prefix)
            return None
        target = entries[0]
        return target
    else:  # if depth > 1
        # Recurse on each directory
        for e in entries:
            LOGGER.debug('New path: %s', e)
            target = get_next_transfer(
                ss_url, ss_user, ss_api_key, ts_location_uuid, e, depth - 1,
                completed, see_files)
            if target:
                return target
    return None


def start_transfer(ss_url, ss_user, ss_api_key, ts_location_uuid, ts_path,
                   depth, am_url, am_user, am_api_key, transfer_type,
                   see_files, session):
    """
    Starts a new transfer.

    :param ss_url: URL of the Storage Sevice to query
    :param ss_user: User on the Storage Service for authentication
    :param ss_api_key: API key for user on the Storage Service for
                       authentication
    :param ts_location_uuid: UUID of the transfer source Location
    :param ts_path: Relative path inside the Location to work with.
    :param depth: Depth relative to ts_path to create a transfer from. Should
                  be 1 or greater.
    :param am_url: URL of Archivematica pipeline to start transfer on
    :param am_user: User on Archivematica for authentication
    :param am_api_key: API key for user on Archivematica for authentication
    :param bool see_files: If true, start transfers from files as well as
                           directories
    :param session: SQLAlchemy session with the DB
    :returns: Tuple of Transfer information about the new transfer or None on
              error.
    """
    # Start new transfer
    completed = {x[0] for x in session.query(models.Unit.path).all()}
    target = get_next_transfer(
        ss_url, ss_user, ss_api_key, ts_location_uuid, ts_path, depth,
        completed, see_files)
    if not target:
        LOGGER.warning(
            "All potential transfers in %s have been created. Exiting",
            ts_path)
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
        'paths[]': [base64.b64encode(fsencode(ts_location_uuid) + b':' +
                                     target)],
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
        new_transfer = models.Unit(
            path=target, unit_type='transfer', status='FAILED', current=False)
        session.add(new_transfer)
        return None

    try:
        # Run all scripts in pre-transfer directory
        # TODO what inputs do we want?
        run_scripts(
            'pre-transfer',
            resp_json['path'],  # Absolute path
            'standard',  # Transfer type
        )
    except BaseException as err:
        # An error occurred, log and recover cleanly, returning to main process
        LOGGER.error("Failed to run pre-transfer scripts: %s", err)
        return None

    # Approve transfer
    LOGGER.info("Ready to start")
    retry_count = 3
    for i in range(retry_count):
        result = approve_transfer(target_name, am_url, am_api_key, am_user)
        # Mark as started
        if result:
            LOGGER.info('Approved %s', result)
            new_transfer = models.Unit(
                uuid=result, path=target, unit_type='transfer', current=True)
            LOGGER.info('New transfer: %s', new_transfer)
            session.add(new_transfer)
            break
        LOGGER.info('Failed approve, try %s of %s', i + 1, retry_count)
    else:
        LOGGER.warning('Not approved')
        new_transfer = models.Unit(
            uuid=None, path=target, unit_type='transfer', current=False)
        session.add(new_transfer)
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
        return waiting_transfers
    for a in waiting_transfers['results']:
        LOGGER.debug("Found waiting transfer: %s", a['directory'])
        if fsencode(a['directory']) == directory_name:
            # Post to approve transfer
            post_url = url + "/api/transfer/approve/"
            params = {'username': am_user, 'api_key': am_api_key,
                      'type': a['type'], 'directory': directory_name}
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


def main(am_user, am_api_key, ss_user, ss_api_key, ts_uuid, ts_path, depth,
         am_url, ss_url, transfer_type, see_files, hide_on_complete=False,
         config_file=None, log_level='INFO'):

    global LOGGER
    LOGGER = get_logger(
        get_setting('logfile', defaults.TRANSFER_LOG_FILE), log_level)

    LOGGER.info("Waking up")

    session = models.Session()

    # Check for evidence that this is already running
    default_pidfile = os.path.join(THIS_DIR, 'pid.lck')
    pid_file = get_setting('pidfile', default_pidfile)
    try:
        # Open PID file only if it doesn't exist for read/write
        f = os.fdopen(
            os.open(pid_file, os.O_CREAT | os.O_EXCL | os.O_RDWR), 'r+')
    except:
        LOGGER.info('This script is already running. To override this '
                    'behaviour and start a new run, remove %s', pid_file)
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
        status_info = get_status(
            am_url, am_user, am_api_key, unit_uuid, unit_type, session,
            hide_on_complete)
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
        LOGGER.info(
            'Waiting on user input, running scripts in user-input directory.')
        # TODO What inputs do we want?
        microservice = status_info.get('microservice', '')
        run_scripts(
            'user-input',
            microservice,  # Current microservice name
            # String True or False if this is the first time at this prompt
            str(microservice != current_unit.microservice),
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
    new_transfer = start_transfer(
        ss_url, ss_user, ss_api_key, ts_uuid, ts_path,
        depth, am_url, am_user, am_api_key,
        transfer_type, see_files, session)

    session.commit()
    os.remove(pid_file)
    return 0 if new_transfer else 1


if __name__ == '__main__':

    # Variable for conformance to flake8 line lenght below.
    rawformatter = argparse.RawDescriptionHelpFormatter

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=rawformatter)
    parser.add_argument('-u', '--user', metavar='USERNAME', required=True,
                        help=('Username of the Archivematica dashboard user '
                              'to authenticate as.'))
    parser.add_argument('-k', '--api-key', metavar='KEY',
                        required=True, help=('API key of the Archivematica '
                                             'dashboard user.'))
    parser.add_argument('--ss-user', metavar='USERNAME', required=True,
                        help=('Username of the Storage Service user to '
                              'authenticate as.'))
    parser.add_argument('--ss-api-key', metavar='KEY',
                        required=True, help=('API key of the Storage Service '
                                             'user.'))
    parser.add_argument(
        '-t', '--transfer-source', metavar='UUID', required=True,
        help='Transfer Source Location UUID to fetch transfers from.')
    parser.add_argument(
        # default=b'' to convert to bytes from unicode str provided by
        # command line.
        '--transfer-path', metavar='PATH', help=('Relative path within the '
                                                 'Transfer Source. Default: ""'
                                                 ), type=fsencode, default=b'')
    parser.add_argument(
        '--depth', '-d', help=('Depth to create the transfers from relative '
                               'to the transfer source location and path. '
                               'Default of 1 creates transfers from the '
                               'children of transfer-path.'), type=int,
        default=1)
    parser.add_argument('--am-url', '-a', metavar='URL',
                        help='Archivematica URL. Default: %s' %
                        defaults.DEF_AM_URL,
                        default='%s' % defaults.DEF_AM_URL)
    parser.add_argument('--ss-url', '-s', metavar='URL',
                        help='Storage Service URL. Default: %s' %
                        defaults.DEF_SS_URL,
                        default='%s' % defaults.DEF_SS_URL)
    parser.add_argument(
        '--transfer-type', metavar='TYPE', help=("Type of transfer to start. "
                                                 "One of: 'standard' "
                                                 "(default), 'unzipped bag', "
                                                 "'zipped bag', 'dspace'."),
        default='standard', choices=['standard', 'unzipped bag',
                                     'zipped bag', 'dspace'])
    parser.add_argument('--files', action='store_true',
                        help=('If set, start transfers from files as well as '
                              'folders.'))
    parser.add_argument('--hide', action='store_true',
                        help=('If set, hide the Transfers and SIPs in the '
                              'dashboard once they complete.'))
    parser.add_argument('-c', '--config-file', metavar='FILE',
                        help='Configuration file(log/db/PID files)',
                        default=None)

    # Logging
    parser.add_argument('--verbose', '-v', action='count',
                        default=0, help='Increase the debugging output.')
    parser.add_argument('--quiet', '-q', action='count',
                        default=0, help='Decrease the debugging output')
    parser.add_argument(
        '--log-level', choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default=None, help=('Set the debugging output level. This will '
                            'override -q and -v'))

    args = parser.parse_args()

    log_level = loggingconfig.set_log_level(
        args.log_level, args.quiet, args.verbose)

    models.init(
        get_setting(args.config_file, 'databasefile', os.path.join(THIS_DIR,
                    'transfers.db')))

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
