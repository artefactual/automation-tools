"""Where you put stuff when you can't think of a good name for a module."""
import logging

import requests
import urllib3

from transfers import errors


LOGGER = logging.getLogger("transfers")

METHOD_GET = "GET"
METHOD_POST = "POST"
METHOD_DELETE = "DELETE"


def _call_url_json(url, params=None, method=METHOD_GET, headers=None, assume_json=True):
    """Helper to GET a URL where the expected response is 200 with JSON.

    :param str url: URL to call
    :param dict params: Params to pass as HTTP query string or JSON body
    :param str method: HTTP method (e.g., 'GET')
    :param dict headers: HTTP headers
    :param bool assume_json: set to False if the response body should not be
                             decoded as JSON
    :returns: Dict of the returned JSON or an integer error
            code to be looked up
    """
    method = method.upper()
    LOGGER.debug("URL: %s; params: %s; method: %s", url, params, method)
    try:
        if method == METHOD_GET or method == METHOD_DELETE:
            response = requests.request(method, url=url, params=params, headers=headers)
        else:
            response = requests.request(method, url=url, data=params, headers=headers)
        LOGGER.debug("Response: %s", response)
        LOGGER.debug("type(response.text): %s ", type(response.text))
        LOGGER.debug("Response content-type: %s", response.headers["content-type"])
    except (
        urllib3.exceptions.NewConnectionError,
        requests.exceptions.ConnectionError,
    ) as err:
        LOGGER.error("Connection error %s", err)
        return errors.ERR_SERVER_CONN
    if not response.ok:
        LOGGER.warning(
            "%s Request to %s returned %s %s",
            method,
            url,
            response.status_code,
            response.reason,
        )
        LOGGER.debug("Response: %s", response.text)
        return errors.ERR_INVALID_RESPONSE
    if assume_json:
        try:
            return response.json()
        except ValueError:  # JSON could not be decoded
            LOGGER.warning("Could not parse JSON from response: %s", response.text)
            return errors.ERR_PARSE_JSON
    return response.text
