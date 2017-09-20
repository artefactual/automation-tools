#!/usr/bin/env python
"""
Create DIP from AIP

Downloads and AIP from the Storage Service and creates a DIP
"""

import argparse
import logging
import logging.config  # Has to be imported separately
import os
import sys
import requests
import re

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger('create_dip')


def setup_logger(log_file, log_level='INFO'):
    """Configures the logger to output to console and log file"""
    if not log_file:
        log_file = os.path.join(THIS_DIR, 'create_dip.log')

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


def main(ss_url, ss_user, ss_api_key, aip_uuid):
    aip_filename = download_aip(ss_url, ss_user, ss_api_key, aip_uuid)

    if not aip_filename:
        LOGGER.error('Unable to download AIP %s', aip_uuid)
        return

    LOGGER.info('AIP downloaded to %s', aip_filename)


def download_aip(ss_url, ss_user, ss_api_key, aip_uuid):
    """Download the AIP from Storage Service"""
    aip_url = '{}/api/v2/file/{}/download/'.format(ss_url, aip_uuid)
    params = { 'username': ss_user, 'api_key': ss_api_key }

    LOGGER.info('Downloading AIP from Storage Service, URL: %s', aip_url)

    response = requests.get(aip_url, params, stream=True)
    if response.status_code == 200:
        try:
            local_filename = re.findall(
                'filename="(.+)"',
                response.headers['content-disposition'])[0]
        except KeyError:
            # NOTE: assuming .7z format
            local_filename = 'aip-{}.7z'.format(aip_uuid)
        local_filename = os.path.join('/tmp', local_filename)
        with open(local_filename, 'wb') as file_:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file_.write(chunk)
        return local_filename


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ss-url', metavar='URL', help='Storage Service URL. Default: http://127.0.0.1:8000', default='http://127.0.0.1:8000')
    parser.add_argument('--ss-user', metavar='USERNAME', required=True, help='Username of the Storage Service user to authenticate as.')
    parser.add_argument('--ss-api-key', metavar='KEY', required=True, help='API key of the Storage Service user.')
    parser.add_argument('--aip-uuid', metavar='UUID', required=True, help='UUID of the AIP in the Storage Service')

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
        aip_uuid=args.aip_uuid
    ))
