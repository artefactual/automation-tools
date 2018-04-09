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
import logging
import os
import pprint
import re
import sys

import requests

# AM Client module configuration

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfers import loggingconfig, defaults, amclientargs, errors, utils

LOGGER = logging.getLogger('transfers')


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


class AMClient(object):

    reingest_type = "FULL"
    transfer_type = "standard"

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
                    self.stdout(err_lookup
                                .get(res,
                                     err_lookup(errors.ERR_CLIENT_UNKNOWN)))
                else:
                    self.stdout(res)
            except requests.exceptions.InvalidURL:
                self.stdout(errors.error_lookup(errors.ERR_INVALID_URL))
            except BaseException:
                self.stdout(errors.error_lookup(errors.ERR_CLIENT_UNKNOWN))
        else:
            raise AttributeError('AMClient has no method {0}'.format(name))

    def _am_auth(self):
        """Create JSON parameters for authentication in the request body to
        the Archivematica API.
        """
        return {
            'username': self.am_user_name,
            'api_key': self.am_api_key,
        }

    def _ss_auth(self):
        """Create JSON parameters for authentication in the request body to
        the Storage Service API.
        """
        return {
            'username': self.ss_user_name,
            'api_key': self.ss_api_key
        }

    def _am_auth_headers(self):
        """Generate a HTTP request header for the Archivematica API."""
        return {"Authorization": "ApiKey {0}:{1}".format(self.am_user_name,
                                                         self.am_api_key)}

    def _ss_auth_headers(self):
        """Generate a HTTP request header for Storage Service API."""
        return {"Authorization": "ApiKey {0}:{1}".format(self.ss_user_name,
                                                         self.ss_api_key)}

    def hide_unit(self, unit_uuid, unit_type):
        """GET <unit_type>/<unit_uuid>/delete/."""
        return utils._call_url_json(
            '{}/api/{}/{}/delete/'.format(self.am_url, unit_type, unit_uuid),
            params=self._am_auth(),
            method=utils.METHOD_DELETE
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
                if isinstance(response, int):
                    ret['close_failed'].append(unit_uuid)
                    LOGGER.warning('FAILED to close %s %s.',
                                   unit_type, unit_uuid)
                else:
                    ret['close_succeeded'].append(unit_uuid)
                    LOGGER.info('Closed %s %s.', unit_type, unit_uuid)
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

    def get_package(self, params=None):
        """SS GET /api/v2/file/?<GET_PARAMS>."""
        payload = self._ss_auth()
        payload.update(params)
        return utils._call_url_json(
            '{}/api/v2/file/'.format(self.ss_url), payload)

    def get_package_details(self):
        """SS GET /api/v2/file/<uuid>. Retrieve the details of a specific
        package given a package uuid.
        """
        return utils._call_url_json(
            '{0}/api/v2/file/{1}'.format(self.ss_url, self.package_uuid),
            headers=self._ss_auth_headers())

    def get_next_package_page(self, next_path):
        """SS GET  /api/v2/file/?<GET_PARAMS> using the next URL from
        previous responses, which includes the auth. parameters.
        """
        return utils._call_url_json('{}{}'.format(self.ss_url, next_path), {})

    def aips(self, params=None):
        """Retrieve the details of a specific AIP."""
        final_params = {'package_type': 'AIP'}
        if params:
            final_params.update(params)
        return self.get_all_packages(final_params)

    def dips(self, params=None):
        """Retrieve the details of a specific DIP."""
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

    def get_all_compressed_aips(self):
        """Retrieve a dict of compressed AIPs in the Storage Service.

        The dict is indexed by the AIP UUIDs. To retrieve a list of UUIDs only,
        access the dict using aips.keys(). To access the aip metadata, call
        aips.values().
        """
        compressed_aips = {}
        for aip in self.aips():
            if aip['status'] == u"UPLOADED":
                path = aip["current_full_path"]
                compressed = self.find_compressed(path)
                if compressed:
                    compressed_aips[aip['uuid']] = aip
        return compressed_aips

    def find_compressed(self, path):
        """A .7z file extension might indicate if a file is compressed. We try
        to identify that here.
        """
        compressed_file_ext = [".7z"]
        uncompressed_file_ext = ""
        file_name, file_extension = os.path.splitext(path)
        LOGGER.debug("Found filename %s with extension %s", file_name,
                     file_extension)
        file_extension = file_extension.strip()
        if file_extension in compressed_file_ext:
            return True
        elif file_extension == uncompressed_file_ext:
            return False
        LOGGER.warning("Status of AIP compression is unconfirmed")
        return None

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

    def get_pipelines(self):
        """GET Archivematica Pipelines (dashboard instances from the storage
        service.
        """
        return utils._call_url_json('{0}/api/v2/pipeline/'.format(self.ss_url),
                                    headers=self._ss_auth_headers())

    def get_transfer_status(self):
        """Given a Transfer UUID, GET the transfer status.

        If there isn't a transfer with this UUID in the pipeline then the
        response from the server will look as follows::

            {"message": "Cannot fetch unitTransfer with UUID"
                        " ebc8a35c-6742-4264-bc30-22e263966d69",
             "type": "transfer",
             "error": true}
        The response suggesting non-existence is an error, "error": true, is
        something the caller will have to handle appropriately for their
        application.
        """
        return utils._call_url_json(
            '{0}/api/transfer/status/{1}/'.format(self.am_url,
                                                  self.transfer_uuid),
            headers=self._am_auth_headers())

    def get_ingest_status(self):
        """GET ingest status if there is an ingest in progress in the
        Archivematica pipeline.
        """
        return utils._call_url_json(
            '{0}/api/ingest/status/{1}/'.format(self.am_url, self.sip_uuid),
            headers=self._am_auth_headers())

    def get_processing_config(self, assume_json=False):
        """GET a processing configuration file from an Archivematica instance.

        if the request is successful an application/xml response is returned
        to the caller. If the request is unsuccessful then an error code is
        returned which needs to be handled via error_lookup. The default is to
        return the default processing config from the AM server.
        """
        return utils._call_url_json(
            '{0}/api/processing-configuration/{1}'
            .format(self.am_url,
                    self.processing_config),
            headers=self._am_auth_headers(),
            assume_json=assume_json)

    def approve_transfer(self):
        """Approve a transfer in the Archivematica Pipeline.

        The transfer_type informs Archivematica how to continue processing.
        Options are:
          * standard
          * unzipped bag
          * zipped bag
          * dspace
        Directory is the location where the transfer is to be picked up
        from. The directory can be found via the get_transfer_status API
        call.
        """
        url = '{0}/api/transfer/approve/'.format(self.am_url)
        params = {"type": self.transfer_type,
                  "directory": utils.fsencode(self.transfer_directory)}
        return utils._call_url_json(url,
                                    headers=self._am_auth_headers(),
                                    params=params,
                                    method=utils.METHOD_POST)

    def reingest_aip(self):
        """Initiate the reingest of an AIP via the Storage Service given the
        API UUID and Archivematica Pipeline.

        Reingest default is set to
        ``full``. Alternatives are:
            * METADATA_ONLY (metadata only re-ingest)
            * OBJECTS (partial re-ingest)
            * FULL (full re-ingest)
        """
        params = {'pipeline': self.pipeline_uuid,
                  'reingest_type': self.reingest_type,
                  'processing_config': self.processing_config}
        url = "{0}/api/v2/file/{1}/reingest/".format(self.ss_url, self.aip_uuid)
        return utils._call_url_json(url,
                                    headers=self._ss_auth_headers(),
                                    params=json.dumps(params),
                                    method=utils.METHOD_POST)

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
    loggingconfig.setup(args.log_level, args.log_file)

    am_client = AMClient(**vars(args))

    try:
        getattr(am_client, 'print_{0}'.format(args.subcommand.replace('-',
                                                                      '_')))
    except AttributeError:
        argparser.print_help()
        sys.exit(0)


if __name__ == '__main__':
    main()
