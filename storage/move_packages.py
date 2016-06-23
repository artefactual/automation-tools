#!/usr/bin/env python
"""
Automate Package Moving

Helper script to automate moving packages in the Archivematica Storage Service.
"""

from __future__ import print_function
import argparse
import json
import logging
import logging.config  # Has to be imported separately
import os
import requests
from six.moves import configparser
import sys

# This project
from common import utils

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(THIS_DIR)

LOGGER = logging.getLogger('storage')

CONFIG_FILE = None


def get_setting(setting, default=None):
    return utils.get_setting(CONFIG_FILE, 'storage', setting, default)


def setup(config_file):
    global CONFIG_FILE
    CONFIG_FILE = config_file

    # Configure logging
    default_logfile = os.path.join(THIS_DIR, 'automate-storage.log')
    logfile = get_setting('logfile', default_logfile)
    utils.configure_logging('storage', logfile)


def get_first_eligible_package_in_location(ss_url, location_uuid):
    """
    Get first package in a location that has a status of either UPLOADED or MOVING.

    :param str ss_url: Storage service URL
    :param str location_uuid: UUID of location to fetch package details from
    :returns: Dict containing package details or None if none found
    """
    get_url = "%s/api/v2/file/" % (ss_url)

    # Specify order so what query returns is consistent
    params = [
        ("current_location__uuid", location_uuid),
        ("status__in", "UPLOADED"),
        ("status__in", "MOVING"),
        ("order_by", "uuid")]

    result = utils.call_url_json(get_url, params, LOGGER)
    if 'objects' in result and len(result['objects']):
        return result['objects'][0]
    else:
        return None


def move_to_location(ss_url, package_uuid, location_uuid):
    """
    Send request to move package to another location.

    :param str ss_url: Storage service URL
    :param str package_uuid: UUID of package to move
    :param str location_uuid: UUID of location to move package to
    :returns: Dict representing JSON response.
    """
    LOGGER.info("Moving package %s to location %s", package_uuid, location_uuid)

    post_url = '%s/api/v2/file/%s/move/' % (ss_url, package_uuid)
    post_data = {'location_uuid': location_uuid}
    LOGGER.debug('URL: %s; Body: %s;', post_url, json.dumps(post_data))

    r = requests.post(post_url,
                      json=post_data,
                      headers={'content-type': 'application/json'})
    LOGGER.debug('Response: %s', r)
    LOGGER.debug('Response text: %s', r.text)
    if r.status_code != 200:
        return None

    return r.json()


def main(ss_url, from_location_uuid, to_location_uuid, config_file=None):

    setup(config_file)

    LOGGER.info("Waking up")

    # Check for evidence that this is already running
    default_pidfile = os.path.join(THIS_DIR, 'pid.lck')
    pid_file = get_setting('pidfile', default_pidfile)
    if utils.open_pid_file(pid_file, LOGGER) is None:
        return 0

    # Check statuis of last package and attempt move
    move_result = None
    package = get_first_eligible_package_in_location(ss_url, from_location_uuid)
    if package is None:
        LOGGER.info('No packages remain in location, nothing to do.')
    elif package['status'] == 'MOVING':
        LOGGER.info('Current package %s still processing, nothing to do.', package['uuid'])
    else:
        LOGGER.info('Moving package %s.', package['uuid'])
        move_result = move_to_location(ss_url, package['uuid'], to_location_uuid)
        if move_result is None:
            LOGGER.info('Move request failed')
        else:
            LOGGER.info('Move result: %s', move_result['message'])

    os.remove(pid_file)
    return 0 if move_result is not None and move_result['success'] else 1


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ss-url', '-s', metavar='URL', help='Storage Service URL. Default: http://127.0.0.1:8000', default='http://127.0.0.1:8000')
    parser.add_argument('--from-location', '-f', metavar='SOURCE', help="UUID of source location.", required=True)
    parser.add_argument('--to-location', '-t', metavar='DEST', help="UUID of destination location.", required=True)
    parser.add_argument('--config-file', '-c', metavar='FILE', help='Configuration file(log/db/PID files)', default=None)
    args = parser.parse_args()

    sys.exit(main(
        ss_url=args.ss_url,
        from_location_uuid=args.from_location,
        to_location_uuid=args.to_location,
        config_file=args.config_file
    ))
