#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Archivematica Client.

Module and CLI that holds functionality for interacting with the various
Archivematica APIs.
"""

from __future__ import print_function, unicode_literals

import binascii
import base64
from collections import defaultdict
import json
import os
import pprint
import re
import sys

import requests

# AM Client module configuration

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfers import loggingconfig
from transfers import defaults
from transfers import amclientargs
from transfers import errors
from transfers import utils


def get_logger(log_file_name, log_level):
    return loggingconfig.setup(log_level, log_file_name, "amclient")


# Default logging if no other logging is provided in the class.
LOGGER = get_logger(defaults.AMCLIENT_LOG_FILE, defaults.DEFAULT_LOG_LEVEL)


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
                LOGGER.debug(defaults.UNDEC_MSG)
                return defaults.UNDECODABLE
            encoding = chardet.detect(thing).get('encoding')
            if encoding:
                try:
                    return thing.decode(encoding)
                except ValueError:
                    LOGGER.debug(defaults.UNDEC_MSG)
                    return defaults.UNDECODABLE
            LOGGER.debug(defaults.UNDEC_MSG)
            return defaults.UNDECODABLE

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


def is_uuid(thing):
    return defaults.UUID_PATT.search(thing) is not None


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

    # stdout and __getattr__ help us to deal with class output, and output
    # formatting in a useful way, e.g. returning user friendly error messages
    # from any failed calls to the AM or SS servers.
    def stdout(self, stuff):
        """Print to stdout, either as JSON or pretty-printed Python."""
        if self.output_mode.lower() == 'json':
            print(json.dumps(stuff))
        else:
            pprint.pprint(stuff)

    def __getattr__(self, name):
        if name.startswith('print_'):
            try:
                method = name.replace('print_', '', 1)
                res = getattr(self, method)()
                # Shortening variable for PEP8 conformance.
                err_lookup = errors.error_lookup
                if isinstance(res, int):
                    self.stdout(err_lookup.get(res,
                                err_lookup(errors.ERR_AMCLIENT_UNKNOWN)))
                else:
                    self.stdout(getattr(self, method)())
            except:
                self.stdout(errors.error_lookup(errors.ERR_AMCLIENT_UNKNOWN))
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
        return utils._call_url_json(
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
        return utils._call_url_json(
            '{}/api/transfer/completed'.format(self.am_url), self._am_auth())

    def completed_ingests(self):
        """Return all completed ingests. GET /ingest/completed::

            $ ./amclient.py completed-ingests \
                --am-user-name=test \
                e8f8a0fb157f08a260045f805455e144d8ad0a5b
        """
        return utils._call_url_json(
            '{}/api/ingest/completed'.format(self.am_url), self._am_auth())

    def unapproved_transfers(self):
        """Return all unapproved transfers. GET transfer/unapproved::

            $ ./amclient.py unapproved-transfers \
                --am-user-name=test \
                --am-api-key=e8f8a0fb157f08a260045f805455e144d8ad0a5b
        """
        return utils._call_url_json(
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
        result = utils._call_url_json(url, params)
        if b64decode:
            return b64decode_ts_location_browse(result)
        return result

    def get_package(self, params):
        """SS GET  /api/v2/file/?<GET_PARAMS>."""
        payload = self._ss_auth()
        payload.update(params)
        return utils._call_url_json(
            '{}/api/v2/file/'.format(self.ss_url), payload)

    def get_next_package_page(self, next_path):
        """SS GET  /api/v2/file/?<GET_PARAMS> using the next URL from
        previous responses, which includes the auth. parameters.
        """
        return utils._call_url_json('{}{}'.format(self.ss_url, next_path), {})

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

    argparser = amclientargs.get_parser()

    # Python 2.x, ensures that help is printed consistently like we see in
    # Python 3.x.
    if len(sys.argv) < 2:
        argparser.print_help()
        sys.exit(0)

    args = argparser.parse_args()
    am_client = AMClient(**vars(args))

    # Re-configure global LOGGER based on user provided parameters.
    global LOGGER
    LOGGER = get_logger(args.log_file, args.log_level)

    try:
        getattr(am_client, 'print_{0}'.format(args.subcommand.replace('-',
                                                                      '_')))
    except AttributeError:
        argparser.print_help()
        sys.exit(0)


if __name__ == '__main__':
    main()
