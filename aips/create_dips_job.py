#!/usr/bin/env python
"""
Create DIPs from an SS location

Get all AIPs from an existing SS instance, filtering them by location,
creating DIPs using the create_dip.py script and keeping track of them
in an SQLite database.

POSSIBLE ENHANCEMENT: Add status to Aip table in database:
The create_dip.main() function returns different error values, some of
them could allow a retry of the DIP creation in following executions.
More info in comments bellow.
"""

import argparse
import logging
import logging.config  # Has to be imported separately
import os
import sys

from sqlalchemy import exc

from transfers import amclient
from aips import create_dip
from aips import models

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger('create_dip')

# POSSIBLE ENHANCEMENT:
# Create Aip status constants, better in create_dip.py
# and use them in its returns and in here.


def setup_logger(log_file, log_level='INFO'):
    """Configures the logger to output to console and log file"""
    if not log_file:
        log_file = os.path.join(THIS_DIR, 'create_dip.log')

    CONFIG = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'default': {
                'format': '%(levelname)-8s  %(asctime)s  %(message)s',
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
                'filename': log_file,
                'backupCount': 2,
                'maxBytes': 10 * 1024,
            },
        },
        'loggers': {
            'create_dip': {
                'level': log_level,
                'handlers': ['console', 'file'],
            },
        },
    }

    logging.config.dictConfig(CONFIG)


def main(ss_url, ss_user, ss_api_key, location_uuid, tmp_dir, output_dir, database_file):
    LOGGER.info('Processing AIPs in SS location: %s', location_uuid)

    # Idempotently create database and Aip table and create session
    try:
        session = models.init(database_file)
    except IOError:
        LOGGER.error('Could not create database in: %s', database_file)
        return 1

    # Get UPLOADED and VERIFIED AIPs from the SS
    try:
        am_client = amclient.AMClient(
            ss_url=ss_url,
            ss_user_name=ss_user,
            ss_api_key=ss_api_key)
        # There is an issue in the SS API that avoids
        # filtering the results by location. See:
        # https://github.com/artefactual/archivematica-storage-service/issues/298
        aips = am_client.aips({'status__in': 'UPLOADED,VERIFIED'})
    except Exception as e:
        LOGGER.error(e)
        return 2

    # Get only AIPs from the specified location
    aip_uuids = filter_aips(aips, location_uuid)

    # Create DIPs for those AIPs
    for uuid in aip_uuids:
        try:
            # To avoid race conditions while checking for an existing AIP
            # and saving it, create the row directly and check for an
            # integrity error exception (the uuid is a unique column)
            db_aip = models.Aip(uuid=uuid)
            session.add(db_aip)
            session.commit()
        except exc.IntegrityError:
            session.rollback()
            # POSSIBLE ENHANCEMENT:
            # Check Aip status and allow retry in some of them
            LOGGER.debug('Skipping AIP (already processed/processing): %s', uuid)
            continue

        create_dip.main(
            ss_url=ss_url,
            ss_user=ss_user,
            ss_api_key=ss_api_key,
            aip_uuid=uuid,
            tmp_dir=tmp_dir,
            output_dir=output_dir
        )

        # POSSIBLE ENHANCEMENT:
        # Save return value from create_dip.main() and update Aip status

    LOGGER.info('All AIPs have been processed')


def filter_aips(aips, location_uuid):
    """
    Filters a list of AIPs based on a location UUID.

    :param list aips: list of AIPs from the results of an SS response
    :param str location_uuid: UUID from the SS location
    :returns: list of UUIDs from the AIPs in that location
    """
    location = '/api/v2/location/{}/'.format(location_uuid)
    filtered_aips = []

    for aip in aips:
        if 'uuid' not in aip:
            LOGGER.warning('Skipping AIP (missing UUID in SS response)')
            continue
        if 'current_location' not in aip:
            LOGGER.debug('Skipping AIP (missing location): %s', aip['uuid'])
            continue
        if aip['current_location'] != location:
            LOGGER.debug('Skipping AIP (different location): %s', aip['uuid'])
            continue
        filtered_aips.append(aip['uuid'])

    return filtered_aips


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ss-url', metavar='URL', help='Storage Service URL. Default: http://127.0.0.1:8000', default='http://127.0.0.1:8000')
    parser.add_argument('--ss-user', metavar='USERNAME', required=True, help='Username of the Storage Service user to authenticate as.')
    parser.add_argument('--ss-api-key', metavar='KEY', required=True, help='API key of the Storage Service user.')
    parser.add_argument('--location-uuid', metavar='UUID', required=True, help='UUID of an AIP Storage location in the Storage Service.')
    parser.add_argument('--database-file', metavar='PATH', required=True, help='Absolute path to an SQLite database file.')
    parser.add_argument('--tmp-dir', metavar='PATH', help='Absolute path to the directory used for temporary files. Default: /tmp.', default='/tmp')
    parser.add_argument('--output-dir', metavar='PATH', help='Absolute path to the directory used to place the final DIP. Default: /tmp.', default='/tmp')

    # Logging
    parser.add_argument('--log-file', metavar='FILE', help='Location of log file', default=None)
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

    setup_logger(args.log_file, log_level)

    sys.exit(main(
        ss_url=args.ss_url,
        ss_user=args.ss_user,
        ss_api_key=args.ss_api_key,
        location_uuid=args.location_uuid,
        tmp_dir=args.tmp_dir,
        output_dir=args.output_dir,
        database_file=args.database_file
    ))
