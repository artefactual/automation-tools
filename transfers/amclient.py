#!/usr/bin/env python
"""Archivematica Client.

Module and CLI that holds functionality for interacting with the various
Archivematica APIs.
"""
from __future__ import print_function, unicode_literals

import argparse
import binascii
import base64
from collections import defaultdict, namedtuple
import json
import logging
import logging.config  # Has to be imported separately
import os
import pprint
import re
import sys

import requests
from six import binary_type, text_type


try:
    from os import fsencode
except ImportError:
    def fsencode(filename):
        """Cribbed & modified from Python3's OS module to support Python2."""
        encoding = sys.getfilesystemencoding()
        if isinstance(filename, binary_type):
            return filename
        elif isinstance(filename, text_type):
            return filename.encode(encoding)
        else:
            raise TypeError("expect bytes or str, not %s" %
                            type(filename).__name__)


THIS_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_LOGFILE = os.path.join(THIS_DIR, 'amclient.log')
LOGGER = logging.getLogger('amclient')
RETRY_COUNT = 5
DEF_AM_URL = 'http://127.0.0.1'
DEF_SS_URL = 'http://127.0.0.1:8000'
DEF_USER_NAME = 'test'
UUID_PATT = re.compile(
    '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
UNDECODABLE = 'UNABLE TO DECODE'
UNDEC_MSG = ('Unable to decode a transfer source component; giving up and'
             ' returning {0}'.format(UNDECODABLE))


# Reusable argument constants (for CLI).
Arg = namedtuple('Arg', ['name', 'help', 'type'])
AIP_UUID = Arg(
    name='aip_uuid',
    help='UUID of the target AIP',
    type=None)
AM_API_KEY = Arg(
    name='am_api_key',
    help='Archivematica API key',
    type=None)
DIP_UUID = Arg(
    name='dip_uuid',
    help='UUID of the target DIP',
    type=None)
SS_API_KEY = Arg(
    name='ss_api_key',
    help='Storage Service API key',
    type=None)
TRANSFER_SOURCE = Arg(
    name='transfer_source',
    help='Transfer source UUID',
    type=None)


# Reusable option constants (for CLI).
Opt = namedtuple('Opt', ['name', 'metavar', 'help', 'default', 'type'])
AM_URL = Opt(
    name='am-url',
    metavar='URL',
    help='Archivematica URL. Default: {0}'.format(DEF_AM_URL),
    default=DEF_AM_URL,
    type=None)
AM_USER_NAME = Opt(
    name='am-user-name',
    metavar='USERNAME',
    help='Archivematica username. Default: {0}'.format(DEF_USER_NAME),
    default=DEF_USER_NAME,
    type=None)
DIRECTORY = Opt(
    name='directory',
    metavar='DIR',
    help='Directory path to save the DIP in',
    default=None,
    type=None)
OUTPUT_MODE = Opt(
    name='output-mode',
    metavar='MODE',
    help='How to print output, JSON (default) or Python',
    default='json',
    type=None)
SS_URL = Opt(
    name='ss-url',
    metavar='URL',
    help='Storage Service URL. Default: {0}'.format(DEF_SS_URL),
    default=DEF_SS_URL,
    type=None)
SS_USER_NAME = Opt(
    name='ss-user-name',
    metavar='USERNAME',
    help='Storage Service username. Default: {0}'.format(DEF_USER_NAME),
    default=DEF_USER_NAME,
    type=None)
TRANSFER_PATH = Opt(
    name='transfer-path',
    metavar='PATH',
    help='Relative path within the Transfer Source. Default: ""',
    default=b'',
    type=fsencode)


# Sub-command configuration: give them a name, help text, a tuple of ``Arg``
# instances and a tuple of ``Opts`` instances.
SubCommand = namedtuple('SubCommand', ['name', 'help', 'args', 'opts'])
SUBCOMMANDS = (
    SubCommand(
        name='close-completed-transfers',
        help='Close all completed transfers.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='close-completed-ingests',
        help='Close all completed ingests.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='completed-transfers',
        help='Print all completed transfers.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='completed-ingests',
        help='Print all completed ingests.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='unapproved-transfers',
        help='Print all unapproved transfers.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='transferables',
        help='Print all transferable entities in the Storage Service.',
        args=(SS_API_KEY, TRANSFER_SOURCE),
        opts=(SS_USER_NAME, SS_URL, TRANSFER_PATH, OUTPUT_MODE)
    ),
    SubCommand(
        name='aips',
        help='Print all AIPs in the Storage Service.',
        args=(SS_API_KEY,),
        opts=(SS_USER_NAME, SS_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='dips',
        help='Print all DIPs in the Storage Service.',
        args=(SS_API_KEY,),
        opts=(SS_USER_NAME, SS_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='aips2dips',
        help='Print all AIPs in the Storage Service along with their corresponding DIPs.',
        args=(SS_API_KEY,),
        opts=(SS_USER_NAME, SS_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='aip2dips',
        help='Print the AIP with AIP_UUID along with its corresponding DIP(s).',
        args=(AIP_UUID, SS_API_KEY),
        opts=(SS_USER_NAME, SS_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='download-dip',
        help='Download the DIP with DIP_UUID.',
        args=(DIP_UUID, SS_API_KEY),
        opts=(SS_USER_NAME, SS_URL, DIRECTORY, OUTPUT_MODE)
    )
)


def get_parser():
    """Parse arguments according to the ``SUBCOMMANDS`` configuration. Return
    an argparse ``Namespace`` instance representing the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description='Archivematica Client',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--log-file', metavar='FILE', help='logfile', default=DEFAULT_LOGFILE)
    parser.add_argument(
        '--log-level', choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default='INFO', help='Set the debugging output level.')
    subparsers = parser.add_subparsers(help='sub-command help',
                                       dest='subcommand')
    for subcommand in SUBCOMMANDS:
        subparser = subparsers.add_parser(subcommand.name,
                                          help=subcommand.help)
        for arg in subcommand.args:
            subparser.add_argument(
                arg.name, help=arg.help, type=arg.type)
        for opt in subcommand.opts:
            subparser.add_argument(
                '--' + opt.name, metavar=opt.metavar, help=opt.help,
                default=opt.default, type=opt.type)
    return parser


def _call_url_json(url, params, method='GET'):
    """Helper to GET a URL where the expected response is 200 with JSON.
    :param str url: URL to call
    :param dict params: Params to pass to requests.get
    :returns: Dict of the returned JSON or None
    """
    method = method.upper()
    LOGGER.debug('URL: %s; params: %s; method: %s', url, params, method)
    response = requests.request(method, url=url, params=params)
    LOGGER.debug('Response: %s', response)
    LOGGER.debug('type(response.text): %s ', type(response.text))
    LOGGER.debug('Response content-type: %s', response.headers['content-type'])
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


def b64decode_ts_location_browse(result):
    """Base64-decode the results of a call to SS GET
    /location/UUID/browse/.
    """
    def dec(thing):
        try:
            thing = base64.b64decode(thing.encode('utf8'))
        except UnicodeEncodeError:
            LOGGER.warning('Failed to UTF8-encode output from GET call to SS'
                           ' /location/UUID/browse/: %s', result)
        except (binascii.Error, TypeError):
            LOGGER.warning('Failed to base64-decode file or directory names in'
                           ' output from GET call to SS'
                           ' /location/UUID/browse/: %s', result)
        try:
            return thing.decode('utf8')
        except ValueError:
            LOGGER.debug('Unable to decode a transfer source component using'
                         ' the UTF-8 codec; trying to guess the encoding...')
            try:
                import chardet
            except ImportError:
                LOGGER.debug(UNDEC_MSG)
                return UNDECODABLE
            encoding = chardet.detect(thing).get('encoding')
            if encoding:
                try:
                    return thing.decode(encoding)
                except ValueError:
                    LOGGER.debug(UNDEC_MSG)
                    return UNDECODABLE
            LOGGER.debug(UNDEC_MSG)
            return UNDECODABLE

    try:
        result['directories'] = [dec(d) for d in result['directories']]
        result['entries'] = [dec(e) for e in result['entries']]
        result['properties'] = {dec(key): val for key, val in
                                result['properties'].items()}
    except ValueError as error:
        LOGGER.warning('GET call to SS /location/UUID/browse/ returned an'
                       ' unrecognized data structure: %s', result)
        LOGGER.warning(error)
    return result


def setup_logger(log_file, log_level):
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': ('%(levelname)-8s  %(asctime)s  '
                           '%(filename)s:%(lineno)-4s %(message)s'),
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': log_file,
                'backupCount': 2,
                'maxBytes': 10 * 1024,
            },
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'level': 'WARNING'
            }
        },
        'loggers': {
            'amclient': {
                'level': log_level,
                'handlers': ['file'],
            },
            'requests.packages.urllib3': {
                'level': log_level,
                'handlers': ['file'],
            }
        },
    })


def is_uuid(thing):
    return UUID_PATT.search(thing) is not None


class AMClient:

    def __init__(self, **kwargs):
        """Construct an Archivematica client. Provide any of the following
        arguments, depending on what you want the client to do.
        param: ss_url
        param: ss_user_name
        param: ss_api_key
        param: am_url
        param: am_user_name
        param: am_api_key
        param: output_mode
        param: transfer_source
        param: transfer_path
        param: aip_uuid
        param: dip_uuid
        param: directory
        """
        for key, val in kwargs.items():
            setattr(self, key, val)

    def __getattr__(self, name):
        if name.startswith('print_'):
            method = name.replace('print_', '', 1)
            self.stdout(getattr(self, method)())
            return lambda: None
        else:
            raise AttributeError('AMClient has no method {0}'.format(name))

    def _am_auth(self):
        return {
            'username': self.am_user_name,
            'api_key': self.am_api_key,
        }

    def _ss_auth(self):
        return {
            'username': self.ss_user_name,
            'api_key': self.ss_api_key
        }

    def hide_unit(self, unit_uuid, unit_type):
        """GET <unit_type>/<unit_uuid>/delete/."""
        return _call_url_json(
            '{}/api/{}/{}/delete/'.format(self.am_url, unit_type, unit_uuid),
            params=self._am_auth(),
            method='DELETE'
        )

    def close_completed_transfers(self):
        """Close all completed transfers::

            $ ./amclient.py close-completed-transfers \
                --am-user-name=test \
                e8f8a0fb157f08a260045f805455e144d8ad0a5b
        """
        return self._close_completed_units('transfer')

    def close_completed_ingests(self):
        """Close all completed ingests::

            $ ./amclient.py close-completed-ingests \
                --am-user-name=test \
                e8f8a0fb157f08a260045f805455e144d8ad0a5b
        """
        return self._close_completed_units('ingest')

    def _close_completed_units(self, unit_type):
        """Close all completed transfers/ingests.  """
        try:
            _completed_units = getattr(
                self, 'completed_{0}s'.format(unit_type))().get('results')
        except AttributeError:
            _completed_units = None
        ret = defaultdict(list)
        if _completed_units is None:
            msg = ('Something went wrong when attempting to retrieve the'
                   ' completed {0}s.'.format(unit_type))
            LOGGER.warning(msg)
        else:
            for unit_uuid in _completed_units:
                ret['completed_{0}s'.format(unit_type)].append(unit_uuid)
                response = self.hide_unit(unit_uuid, unit_type)
                if response:
                    ret['close_succeeded'].append(unit_uuid)
                    LOGGER.info('Closed %s %s.', unit_type, unit_uuid)
                else:
                    ret['close_failed'].append(unit_uuid)
                    LOGGER.warning('FAILED to close %s %s.',
                                   unit_type, unit_uuid)
        return ret

    def completed_transfers(self):
        """Return all completed transfers. GET /transfer/completed::

            $ ./amclient.py completed-transfers \
                --am-user-name=test \
                e8f8a0fb157f08a260045f805455e144d8ad0a5b
        """
        return _call_url_json(
            '{}/api/transfer/completed'.format(self.am_url), self._am_auth())

    def completed_ingests(self):
        """Return all completed ingests. GET /ingest/completed::

            $ ./amclient.py completed-ingests \
                --am-user-name=test \
                e8f8a0fb157f08a260045f805455e144d8ad0a5b
        """
        return _call_url_json(
            '{}/api/ingest/completed'.format(self.am_url), self._am_auth())

    def unapproved_transfers(self):
        """Return all unapproved transfers. GET transfer/unapproved::

            $ ./amclient.py unapproved-transfers \
                --am-user-name=test \
                --am-api-key=e8f8a0fb157f08a260045f805455e144d8ad0a5b
        """
        return _call_url_json(
            '{}/api/transfer/unapproved'.format(self.am_url), self._am_auth())

    def transferables(self, b64decode=True):
        """Return all transferable entities in the Storage Service.
        GET location/<TS_LOC_UUID>/browse/::

            $ ./amclient.py transferables \
                --ss-user-name=test \
                --ss-api-key=7558e7485cf8f20aadbd95f6add8b429ba11cd2b \
                --transfer-source=7ea1eb0e-5f4e-42e0-836d-c9b4ab5692e1 \
                --transfer-path=vagrant/archivematica-sampledata
        """
        url = '{}/api/v2/location/{}/browse/'.format(
            self.ss_url, self.transfer_source)
        params = self._ss_auth()
        if self.transfer_path:
            params['path'] = base64.b64encode(self.transfer_path)
        result = _call_url_json(url, params)
        if b64decode:
            return b64decode_ts_location_browse(result)
        return result

    def get_package(self, params):
        """SS GET  /api/v2/file/?<GET_PARAMS>."""
        payload = self._ss_auth()
        payload.update(params)
        return _call_url_json(
            '{}/api/v2/file/'.format(self.ss_url), payload)

    def get_next_package_page(self, next_path):
        return _call_url_json('{}{}'.format(self.ss_url, next_path), self._ss_auth())

    def stdout(self, stuff):
        """Print to stdout, either as JSON or pretty-printed Python."""
        if self.output_mode == 'json':
            print(json.dumps(stuff))
        else:
            pprint.pprint(stuff)

    def aips(self, params=None):
        final_params = {'package_type': 'AIP'}
        if params:
            final_params.update(params)
        return self.get_all_packages(final_params)

    def dips(self, params=None):
        final_params = {'package_type': 'DIP'}
        if params:
            final_params.update(params)
        return self.get_all_packages(final_params)

    def get_all_packages(self, params=None, packages=None, next_=None):
        """Get all packages (AIPs or DIPs) in the Storage Service, following
        the pagination trail if necessary.
        """
        if not packages:
            packages = []
        if next_:
            response = self.get_next_package_page(next_)
        else:
            response = self.get_package(params)
        if not response:
            raise Exception('Error connecting to the SS')
        packages = packages + response['objects']
        if response['meta']['next']:
            packages = self.get_all_packages(
                params, packages, response['meta']['next'])
        return packages

    def aip2dips(self):
        """Get all DIPS created from AIP with UUID ``self.aip_uuid``.

        Note: although desirable, it appears that this cannot be accomplished
        by only getting DIPs that are related to the target AIP using
        tastypie's filters. That is, the current SS API does not allow a filter
        like 'current_path__endswith': self.aip_uuid nor does the
        related_packages m2m resource attribute appear to be useful in this
        area. Please inform if this is inaccurate.
        """
        _dips = self.dips()
        return [d for d in _dips if
                d['current_path'].endswith(self.aip_uuid)]

    def aips2dips(self):
        """Get all AIP UUIDs and map them to their DIP UUIDs, if any."""
        _dips = self.dips()
        return {a['uuid']: [d['uuid'] for d in _dips
                            if d['current_path'].endswith(a['uuid'])]
                for a in self.aips()}

    def download_package(self, uuid):
        """Download the package from SS by UUID."""
        url = '{}/api/v2/file/{}/download/'.format(self.ss_url, uuid)
        response = requests.get(url, params=self._ss_auth(), stream=True)
        if response.status_code == 200:
            try:
                local_filename = re.findall(
                    'filename="(.+)"',
                    response.headers['content-disposition'])[0]
            except KeyError:
                # NOTE: assuming that packages are always stored as .7z
                local_filename = 'package-{}.7z'.format(uuid)
            if getattr(self, 'directory', None):
                dir_ = self.directory
                if os.path.isdir(dir_):
                    local_filename = os.path.join(dir_, local_filename)
                else:
                    LOGGER.warning(
                        'There is no directory %s; saving %s to %s instead',
                        dir_, local_filename, os.getcwd())
            with open(local_filename, 'wb') as file_:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file_.write(chunk)
            return local_filename
        else:
            LOGGER.warning('Unable to download package %s', uuid)

    def download_dip(self):
        return self.download_package(self.dip_uuid)

    def download_aip(self):
        return self.download_package(self.aip_uuid)


def main():
    parser = get_parser()
    args = parser.parse_args()
    setup_logger(args.log_file, args.log_level)
    am_client = AMClient(**vars(args))
    try:
        getattr(am_client, 'print_{0}'.format(args.subcommand.replace('-', '_')))
    except AttributeError:
        parser.print_help()
        sys.exit(0)


if __name__ == '__main__':
    main()
