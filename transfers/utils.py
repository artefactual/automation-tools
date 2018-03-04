# -*- coding: utf-8 -*-

"""Where you put stuff when you can't think of a good name for a module."""

import logging
import sys

import requests
import urllib3
from six import binary_type, text_type

from transfers import errors


LOGGER = logging.getLogger('transfers')

METHOD_GET = "GET"
METHOD_POST = "POST"


def _call_url_json(url, params, method=METHOD_GET, headers=None):
    """Helper to GET a URL where the expected response is 200 with JSON.

    :param str url: URL to call
    :param dict params: Params to pass to requests.get
    :returns: Dict of the returned JSON or an integer error
              code to be looked up
    """
    method = method.upper()
    LOGGER.debug('URL: %s; params: %s; method: %s', url, params, method)

    try:
        if headers is None:
            response = requests.request(method, url=url, params=params)
        else:
            response = requests.request(method,
                                        url=url,
                                        data=params,
                                        headers=headers)

        LOGGER.debug('Response: %s', response)
        LOGGER.debug('type(response.text): %s ', type(response.text))
        LOGGER.debug('Response content-type: %s',
                     response.headers['content-type'])

        if not response.ok:
            LOGGER.warning('%s Request to %s returned %s %s', method, url,
                           response.status_code, response.reason)
            LOGGER.debug('Response: %s', response.text)
            return errors.ERR_INVALID_RESPONSE
        try:
            return response.json()
        except ValueError:  # JSON could not be decoded
            LOGGER.warning('Could not parse JSON from response: %s',
                           response.text)
            if not response.ok:
                return errors.ERR_PARSE_JSON
            pass
        LOGGER.warning("Response code %s is okay. Returning something other "
                       "than JSON", response.status_code)
        return response.text

    except (urllib3.exceptions.NewConnectionError,
            requests.exceptions.ConnectionError) as err:
        LOGGER.error("Connection error %s", err)
        return errors.ERR_SERVER_CONN


try:
    from os import fsencode, fsdecode
except ImportError:
    # Cribbed & modified from Python3's OS module to support Python2
    def fsencode(filename):
        """Encode path-like filename to the filesystem encoding.

        See https://docs.python.org/3/library/os.html#os.fsencode for more
        details.
        """
        encoding = sys.getfilesystemencoding()
        if isinstance(filename, binary_type):
            return filename
        elif isinstance(filename, text_type):
            return filename.encode(encoding)
        else:
            raise TypeError("expect bytes or str, not %s" %
                            type(filename).__name__)

    def fsdecode(filename):
        """Decode the path-like filename from the filesystem encoding.

        See https://docs.python.org/3/library/os.html#os.fsdecode for more
        details.
        """
        encoding = sys.getfilesystemencoding()
        if isinstance(filename, text_type):
            return filename
        elif isinstance(filename, binary_type):
            return filename.decode(encoding)
        else:
            raise TypeError("expect bytes or str, not %s" %
                            type(filename).__name__)
