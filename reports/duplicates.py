#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Return all duplicates from the Archivematica Storage Service

Example usage:

$:~/git/archivematica/automation-tools$ python -m reports.duplicates 2> /dev/null

Duplicate entries, per algorithm found, will be output to stdout, e.g.:

    9fd57a8c09e0443de485cf51b68ad8ef54486454434daed499d2f686b7efc2b4
        [u' data/objects/one_file.txt:1-18c87e78-ea18-4a95-9446-e7100f52ab86',
         u' data/objects/one_file.txt:2-44ea1ef4-1ceb-46e1-9a4f-1f8168d47b58']
    63a3a1295b5175d6b726e78660da44d4bdc0049f39eb9b951e6ca67d26c99270
        [u' data/README.html:1-18c87e78-ea18-4a95-9446-e7100f52ab86',
         u' data/README.html:2-44ea1ef4-1ceb-46e1-9a4f-1f8168d47b58']

The script utilizes the AM Client module. The fulcrum is the extract_file
endpoint and the bag manifest. An example use, if we call it via the API is:

    http -v --pretty=format \
        GET "http://127.0.0.1:62081/api/v2/file/18c87e78-ea18-4a95-9446-e7100f52ab86/extract_file/?relative_path_to_file=1-18c87e78-ea18-4a95-9446-e7100f52ab86/manifest-sha256.txt" \
        Authorization:"ApiKey test:test" | less

"""

from __future__ import print_function, unicode_literals

import logging
import json
import os
import sys

from transfers.amclient import AMClient
from transfers import loggingconfig

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger('transfers')


def json_pretty_print(json_string):
    """TODO: Possibly add to transfer.utils code..."""
    print(json.dumps(json_string, sort_keys=True, indent=4))


def main():
    """Primary script entry-point."""
    loggingconfig.setup("INFO", "reports/reports.log")

    am = AMClient()
    am.ss_url = ("http://127.0.0.1:62081")
    am.ss_user_name = "test"
    am.ss_api_key = "test"

    res = {}
    algorithms = ["ma5", "sha1", "sha256"]
    aips = am.aips()
    for aip in aips:
        uri_ = aip.get("resource_uri")
        path_ = os.path.basename(aip.get("current_path")).replace(".7z", "")
        for algo in algorithms:
            manifest_path = "{}/manifest-{}.txt".format(path_, algo)
            manifest_extract = am.extract_file(uri_, manifest_path)
            if isinstance(manifest_extract, int):
                logger.info("No result for algorithm: %s", algo)
                continue
            for line in manifest_extract.split("\n"):
                if line.strip() != "":
                    line_pair = "{}:{}".format(line, path_).split(" ", 1)
                    res.setdefault(line_pair[0], [])
                    res[line_pair[0]].append(line_pair[1])
    for k, v in res.items():
        if len(v) > 1:
            print(k, v)


if __name__ == '__main__':
    sys.exit(main())
