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
import re
import tarfile
import subprocess
import shutil
import requests

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


def main(ss_url, ss_user, ss_api_key, aip_uuid, tmp_dir):
    LOGGER.info('Starting DIP creation from AIP: %s', aip_uuid)

    if not os.path.isdir(tmp_dir):
        LOGGER.error('%s is not a valid directory', tmp_dir)
        return

    LOGGER.info('Downloading AIP from Storage Service')
    aip_file = download_aip(ss_url, ss_user, ss_api_key, aip_uuid, tmp_dir)

    if not aip_file:
        LOGGER.error('Unable to download AIP')
        return

    LOGGER.info('Extracting AIP')
    aip_name = extract_aip(aip_file, aip_uuid, tmp_dir)

    if not aip_name:
        LOGGER.error('Unable to extract AIP')
        return

    aip_dir = os.path.join(tmp_dir, aip_name)

    LOGGER.info('Creating DIP')
    dip_dir = create_dip(aip_dir, aip_uuid)

    if not dip_dir:
        LOGGER.error('Unable to create DIP')
        return

    LOGGER.info('DIP created in: %s', dip_dir)


def download_aip(ss_url, ss_user, ss_api_key, aip_uuid, tmp_dir):
    """Download the AIP from Storage Service"""
    aip_url = '{}/api/v2/file/{}/download/'.format(ss_url, aip_uuid)
    params = {'username': ss_user, 'api_key': ss_api_key}
    LOGGER.debug('SS AIP URL: %s', aip_url)

    response = requests.get(aip_url, params, stream=True)
    if response.status_code == 200:
        try:
            aip_filename = re.findall(
                'filename="(.+)"',
                response.headers['content-disposition'])[0]
        except KeyError:
            # Assuming .7z format if content-disposition is missing
            LOGGER.warning('Response headers is missing content-disposition')
            aip_filename = 'Untitled-{}.7z'.format(aip_uuid)

        LOGGER.debug('AIP filename: %s', aip_filename)

        aip_path = os.path.join(tmp_dir, aip_filename)
        with open(aip_path, 'wb') as file_:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file_.write(chunk)

        return aip_path


def extract_aip(aip_file, aip_uuid, tmp_dir):
    """Extract a downloaded AIP to a folder. Accepted formats: '.tar', '.7z'"""

    # tar archives, including those using gzip or bz2 compression
    if tarfile.is_tarfile(aip_file):
        try:
            tar = tarfile.open(aip_file)

            # Get top-level folders from tar file
            dirs = []
            for tarinfo in tar:
                if '/' not in tarinfo.name and tarinfo.isdir():
                    dirs.append(tarinfo.name)

            if len(dirs) is not 1:
                LOGGER.warning('AIP has none or more than one folder')
                return

            LOGGER.debug('AIP dir: %s', dirs[0])
            tar.extractall(tmp_dir)
            tar.close()

            return dirs[0]
        except tarfile.TarError as err:
            LOGGER.warning('Tarfile error: {}. Trying with /bin/tar'.format(err))

    # 7z, failed tar and other archives based on file last extension
    ext = aip_file.split('.')[-1]
    command = {
        'tar': ['tar', 'xvf', aip_file, '-C', tmp_dir],
        'bz2': ['tar', 'xvjf', aip_file, '-C', tmp_dir],
        'gz': ['tar', 'xvzf', aip_file, '-C', tmp_dir],
        '7z': ['7z', 'x', '-bd', '-y', '-o{0}'.format(tmp_dir), aip_file]
    }.get(ext, ['unar', '-force-overwrite', '-o', tmp_dir, aip_file])

    LOGGER.debug('Extract command: %s', command)
    if subprocess.call(command) is not 0:
        return

    # Get extracted folder name. Assuming it contains the AIP UUID
    for folder in os.listdir(tmp_dir):
        if os.path.isdir(os.path.join(tmp_dir, folder)) and aip_uuid in folder:
            return folder

    LOGGER.warning('Can not find extracted AIP folder by UUID')


def create_dip(aip_dir, aip_uuid):
    dip_dir = aip_dir + '_DIP'
    LOGGER.debug('DIP dir: %s', dip_dir)

    if os.path.exists(dip_dir):
        LOGGER.warning('DIP folder already exists, overwriting')
        shutil.rmtree(dip_dir)
    os.makedirs(os.path.join(dip_dir, 'objects'))

    LOGGER.info('Moving METS file')
    mets_file = '{}/METS.{}.xml'.format(dip_dir, aip_uuid)
    shutil.move('{}/data/METS.{}.xml'.format(aip_dir, aip_uuid), mets_file)

    return dip_dir


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--ss-url', metavar='URL', help='Storage Service URL. Default: http://127.0.0.1:8000', default='http://127.0.0.1:8000')
    parser.add_argument('--ss-user', metavar='USERNAME', required=True, help='Username of the Storage Service user to authenticate as.')
    parser.add_argument('--ss-api-key', metavar='KEY', required=True, help='API key of the Storage Service user.')
    parser.add_argument('--aip-uuid', metavar='UUID', required=True, help='UUID of the AIP in the Storage Service')
    parser.add_argument('--tmp-dir', metavar='PATH', help='Absolute path to the directory used for temporary files. Default: /tmp', default='/tmp')

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
        tmp_dir=args.tmp_dir
    ))
