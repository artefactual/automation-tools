#!/usr/bin/env python
"""
Uploads a DIP to AtoM

Sends the DIP to the AtoM host using rsync and executes a deposit request to the
AtoM instance. A passwordless SSH connection is required to the AtoM host for the
user running this script and it must be already added to the list of known hosts.
"""

import argparse
import logging
import logging.config  # Has to be imported separately
import os
import subprocess
import sys

import requests


THIS_DIR = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger('atom_upload')


def setup_logger(log_file, log_level='INFO'):
    """Configures the logger to output to console and log file"""
    if not log_file:
        log_file = os.path.join(THIS_DIR, 'atom_upload.log')

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
            'atom_upload': {
                'level': log_level,
                'handlers': ['console', 'file'],
            },
        },
    }

    logging.config.dictConfig(CONFIG)


def main(atom_url, atom_email, atom_password, atom_slug, rsync_target, dip_path):
    """Sends the DIP to the AtoM host and a deposit request to the AtoM instance"""
    LOGGER.info('Starting DIP upload to AtoM from: %s', dip_path)

    try:
        rsync(rsync_target, dip_path)
    except subprocess.CalledProcessError as e:
        LOGGER.error('Rsync ended unexpectedly: %s', e.output)
        return 1

    LOGGER.info('DIP folder sent to: %s', rsync_target)

    try:
        deposit(atom_url, atom_email, atom_password, atom_slug, dip_path)
    except Exception as e:
        LOGGER.error('Deposit request to AtoM failed: %s', e)
        return 2

    LOGGER.info('DIP deposited in AtoM')


def rsync(rsync_target, dip_path):
    """
    Build and launch rsync command.

    :param str rsync_target: host and path target for rsync
    :param str dip_path: absolute path to the folder to rsync
    :returns: None
    """
    command = ['rsync', '--protect-args', '-rltz', '-P', '--chmod=ugo=rwX', dip_path, rsync_target]
    subprocess.check_output(command, stderr=subprocess.STDOUT)


def deposit(atom_url, atom_email, atom_password, atom_slug, dip_path):
    """
    Generate and make deposit request to AtoM.

    :param str atom_url: URL to the AtoM instance
    :param str atom_email: AtoM user email
    :param str atom_password: AtoM user password
    :param str atom_slug: target AtoM arch. description slug
    :param str dip_path: absolute path to a DIP folder
    :raises Exception: if the AtoM response is not expected
    :returns: None
    """
    # Build headers dictionary for the deposit request
    headers = {}
    headers['User-Agent'] = 'Archivematica'
    headers['X-Packaging'] = 'http://purl.org/net/sword-types/METSArchivematicaDIP'
    headers['Content-Type'] = 'application/zip'
    headers['X-No-Op'] = 'false'
    headers['X-Verbose'] = 'false'
    headers['Content-Location'] = 'file:///{}'.format(os.path.basename(dip_path))

    # Build URL and auth
    url = '{}/sword/deposit/{}'.format(atom_url, atom_slug)
    auth = requests.auth.HTTPBasicAuth(atom_email, atom_password)

    # Make request (disable redirects)
    LOGGER.info('Making deposit request to: %s', url)
    response = requests.request('POST', url, auth=auth, headers=headers, allow_redirects=False)

    # AtoM returns 302 instead of 202, but Location header field is valid
    LOGGER.debug('Response code: %s', response.status_code)
    LOGGER.debug('Response location: %s', response.headers.get('Location'))
    LOGGER.debug('Response content:\n%s', response.content)

    # Check response status code
    if response.status_code not in [200, 201, 202, 302]:
        raise Exception('Response status code not expected')

    # Location is a must, if it is not included something went wrong
    if response.headers.get('Location') is None:
        raise Exception('Location header is missing in the response')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--atom-url', metavar='URL', help='AtoM instance URL. Default: http://192.168.168.193', default='http://192.168.168.193')
    parser.add_argument('--atom-email', metavar='EMAIL', required=True, help='Email of the AtoM user to authenticate as.')
    parser.add_argument('--atom-password', metavar='PASSWORD', required=True, help='Password of the AtoM user.')
    parser.add_argument('--atom-slug', metavar='SLUG', required=True, help='AtoM archival description slug to target the upload.')
    parser.add_argument('--rsync-target', metavar='HOST:PATH', help='Destination value passed to Rsync. Default: 192.168.168.193:/tmp.', default='192.168.168.193:/tmp')
    parser.add_argument('--dip-path', metavar='PATH', required=True, help='Absolute path to the DIP to upload.')

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
        atom_url=args.atom_url,
        atom_email=args.atom_email,
        atom_password=args.atom_password,
        atom_slug=args.atom_slug,
        rsync_target=args.rsync_target,
        dip_path=args.dip_path
    ))
