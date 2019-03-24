# -*- coding: utf-8 -*-

ERR_INVALID_RESPONSE = 1
ERR_PARSE_JSON = 2
ERR_SERVER_CONN = 3
ERR_INVALID_URL = 4
ERR_CLIENT_UNKNOWN = -1

error_codes = {
    ERR_INVALID_RESPONSE: "Invalid response from server, check amclient log",
    ERR_PARSE_JSON: "Could not parse JSON resposne, check amclient log",
    ERR_SERVER_CONN: "Error connecting to the server, check amclient log",
    ERR_CLIENT_UNKNOWN: "Unknown return from amclient, check logs",
    ERR_INVALID_URL: "Invalid URL passed to AM Client",
}


def error_lookup(errcode):
    try:
        return error_codes[errcode]
    except KeyError:
        # We don't need to assert anything specific other than the
        # error is not one we're controlling if there is one. The safest
        # non-mutating option seems to be to reflect the result back to
        # the calling code to be handled there.
        return errcode
