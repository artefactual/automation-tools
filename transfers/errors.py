#!/usr/bin/env python
# -*- coding: utf-8 -*-

ERR_INVALID_RESPONSE = 1
ERR_PARSE_JSON = 2
ERR_SERVER_CONN = 3
ERR_AMCLIENT_UNKNOWN = -1

error_lookup = {
    ERR_INVALID_RESPONSE: "Invalid response form server, check amclient log",
    ERR_PARSE_JSON: "Could not parse JSON resposne, check amclient log",
    ERR_SERVER_CONN: "Error connecting to the server, check amclient log",
    ERR_AMCLIENT_UNKNOWN: "Unknown return from amclient, check logs."}
