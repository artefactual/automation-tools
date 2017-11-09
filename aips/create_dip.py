#!/usr/bin/env python
"""
Create DIP from AIP

Downloads an AIP from the Storage Service and creates a DIP
"""

import argparse
import logging
import logging.config  # Has to be imported separately
import os
import sys
import subprocess
import shutil

from transfers import amclient

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger('create_dip')


def setup_logger(log_file, log_level='INFO'):
    """Configures the logger to output to console and log file"""
    if not log_file:
        log_file = os.path.join(THIS_DIR, 'create_dip.log')

    CONFIG = {
        'version': 1,
        'disable_existing_loggers': True,
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


def main(ss_url, ss_user, ss_api_key, aip_uuid, tmp_dir, output_dir):
    LOGGER.info('Starting DIP creation from AIP: %s', aip_uuid)

    if not os.path.isdir(tmp_dir):
        LOGGER.error('%s is not a valid temporary directory', tmp_dir)
        return

    if not os.path.isdir(output_dir):
        LOGGER.error('%s is not a valid output directory', output_dir)
        return

    # Create empty workspace directory
    tmp_dir = os.path.join(tmp_dir, aip_uuid)
    if os.path.exists(tmp_dir):
        LOGGER.warning('Workspace directory already exists, overwriting')
        shutil.rmtree(tmp_dir)
    try:
        os.makedirs(tmp_dir)
    except OSError:
        LOGGER.error('Could not create workspace directory: %s', tmp_dir)
        return

    LOGGER.info('Downloading AIP from Storage Service')

    am_client = amclient.AMClient(
        aip_uuid=aip_uuid,
        ss_url=ss_url,
        ss_user_name=ss_user,
        ss_api_key=ss_api_key,
        directory=tmp_dir)

    aip_file = am_client.download_aip()

    if not aip_file:
        LOGGER.error('Unable to download AIP')
        return

    LOGGER.info('Extracting AIP')
    aip_dir = extract_aip(aip_file, aip_uuid, tmp_dir)

    if not aip_dir:
        return

    LOGGER.info('Creating DIP')
    dip_dir = create_dip(aip_dir, aip_uuid, output_dir)

    if not dip_dir:
        LOGGER.error('Unable to create DIP')
        return

    LOGGER.info('DIP created in: %s', dip_dir)


def extract_aip(aip_file, aip_uuid, tmp_dir):
    """Extract a downloaded AIP to a folder."""
    command = ['7z', 'x', '-bd', '-y', '-o{0}'.format(tmp_dir), aip_file]
    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        LOGGER.error('Could not extract AIP, error: %s', e.output)
        return

    # Remove extracted file to avoid multiple entries with the same UUID
    try:
        os.remove(aip_file)
    except OSError:
        pass

    # Find extracted entry. Assuming it contains the AIP UUID
    for entry in os.listdir(tmp_dir):
        if aip_uuid in entry:
            extracted_entry = os.path.join(tmp_dir, entry)

    if not extracted_entry:
        LOGGER.error('Can not find extracted AIP by UUID')
        return

    # Return folder path if it's a directory
    if os.path.isdir(extracted_entry):
        return extracted_entry

    # Re-try extraction if it's not a directory
    return extract_aip(extracted_entry, aip_uuid, tmp_dir)


def create_dip(aip_dir, aip_uuid, output_dir):
    aip_name = os.path.basename(aip_dir)[:-37]
    dip_dir = os.path.join(output_dir, '{}_{}_DIP'.format(aip_name, aip_uuid))
    objects_dir = os.path.join(dip_dir, 'objects')
    to_zip_dir = os.path.join(objects_dir, aip_name)

    if os.path.exists(dip_dir):
        LOGGER.warning('DIP folder already exists, overwriting')
        shutil.rmtree(dip_dir)
    os.makedirs(to_zip_dir)

    LOGGER.info('Moving submissionDocumentation folder')
    aip_sub_doc = '{}/data/objects/submissionDocumentation'.format(aip_dir)
    if os.path.exists(aip_sub_doc):
        to_zip_sub_doc = os.path.join(to_zip_dir, 'submissionDocumentation')
        shutil.move(aip_sub_doc,to_zip_sub_doc)
    else:
        LOGGER.warning('submissionDocumentation folder not found')

    LOGGER.info('Moving METS file')
    aip_mets_file = '{}/data/METS.{}.xml'.format(aip_dir, aip_uuid)
    if not os.path.exists(aip_mets_file):
        LOGGER.error('Could not find AIP METS file')
        return
    to_zip_mets_file = '{}/METS.{}.xml'.format(to_zip_dir, aip_uuid)
    shutil.move(aip_mets_file, to_zip_mets_file)

    return dip_dir


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ss-url', metavar='URL', help='Storage Service URL. Default: http://127.0.0.1:8000', default='http://127.0.0.1:8000')
    parser.add_argument('--ss-user', metavar='USERNAME', required=True, help='Username of the Storage Service user to authenticate as.')
    parser.add_argument('--ss-api-key', metavar='KEY', required=True, help='API key of the Storage Service user.')
    parser.add_argument('--aip-uuid', metavar='UUID', required=True, help='UUID of the AIP in the Storage Service')
    parser.add_argument('--tmp-dir', metavar='PATH', help='Absolute path to the directory used for temporary files. Default: /tmp', default='/tmp')
    parser.add_argument('--output-dir', metavar='PATH', help='Absolute path to the directory used to place the final DIP. Default: /tmp', default='/tmp')

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
        aip_uuid=args.aip_uuid,
        tmp_dir=args.tmp_dir,
        output_dir=args.output_dir
    ))
