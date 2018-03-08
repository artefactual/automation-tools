# -*- coding: utf-8 -*-

import logging
import requests
import urllib3

from transfers import errors

LOGGER = logging.getLogger('transfers')


def _call_url_json(url, params, method='GET'):
    """Helper to GET a URL where the expected response is 200 with JSON.
    :param str url: URL to call
    :param dict params: Params to pass to requests.get
    :returns: Dict of the returned JSON or an integer error
              code to be looked up
    """
    method = method.upper()
    LOGGER.debug('URL: %s; params: %s; method: %s', url, params, method)

    try:
        response = requests.request(method, url=url, params=params)
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
            return errors.ERR_PARSE_JSON

    except (urllib3.exceptions.NewConnectionError,
            requests.exceptions.ConnectionError) as err:
        LOGGER.error("Connection error %s", err)
        return errors.ERR_SERVER_CONN
