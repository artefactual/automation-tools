#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Utilities module for the de-duplication processes."""

import json

# strip /home/ add to docker_home
# /home/appraise-accruals
DOCKER_HOME = "/home/ross-spencer/.am/ss-location-data/"
EXTS = [".7z", ".tar.gz", ".tar.bz2"]


def get_docker_path(path):
    """Return a path on the local machine relative to the transfer source."""
    return path.replace("/home/", DOCKER_HOME)


def json_pretty_print(json_string):
    """Pretty print a JSON string."""
    return json.dumps(json_string, sort_keys=True, indent=4)
