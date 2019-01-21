#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Return all duplicates from the Archivematica Storage Service

Example usage:

$:~/git/archivematica/automation-tools$ python -m reports.duplicates 2> /dev/null

Duplicate entries, per algorithm found, will be output to stdout, e.g.:

    {
        "63a3a1295b5175d6b726e78660da44d4bdc0049f39eb9b951e6ca67d26c99270": [
            [
                "data/README.html",
                "1-523ee501-c61b-4bac-8364-efacde21f526"
            ],
            [
                "data/README.html",
                "2-66330846-f41d-4615-adc0-bcb31f31c99e"
            ]
        ],
        "9fd57a8c09e0443de485cf51b68ad8ef54486454434daed499d2f686b7efc2b4": [
            [
                "data/objects/one_file.txt",
                "1-523ee501-c61b-4bac-8364-efacde21f526"
            ],
            [
                "data/objects/one_file.txt",
                "2-66330846-f41d-4615-adc0-bcb31f31c99e"
            ]
        ]
    }

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

logging_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfers.amclient import AMClient
from transfers import loggingconfig

logger = logging.getLogger('transfers')


def json_pretty_print(json_string):
    """TODO: Possibly add to transfer.utils code..."""
    print(json.dumps(json_string, sort_keys=True, indent=4))


def output_duplicates(manifest_data):
    """Cycle through our AIP manifests and format the output before sending
    to stdout as JSON.
    """
    json_out = {}
    for checksums, values in manifest_data.items():
        if len(values) > 1:
            for entry in values:
                entry = entry.split(":", 1)
                json_out.setdefault(checksums, [])
                json_out[checksums].append(entry)
    json_pretty_print(json_out)


def main():
    """Script's primary entry-point."""
    loggingconfig.setup("INFO", os.path.join(logging_dir, "report.log"))
    am = AMClient()
    am.ss_url = ("http://127.0.0.1:62081")
    am.ss_user_name = "test"
    am.ss_api_key = "test"

    manifest_data = {}
    algorithms = ("ma5", "sha1", "sha256")
    aips = am.aips()
    for aip in aips:
        uri_ = aip.get("resource_uri")
        transfer_name = \
            os.path.basename(aip.get("current_path")).replace(".7z", "")
        for algo in algorithms:
            manifest_path = "{}/manifest-{}.txt".format(transfer_name, algo)
            manifest_extract = am.extract_file(uri_, manifest_path)
            if isinstance(manifest_extract, int):
                logger.info("No result for algorithm: %s", algo)
                continue
            # Our dictionary keys are checksums and all filename entries with
            # that value are appended.
            for line in manifest_extract.split("\n"):
                if line.strip() != "":
                    # Attach our transfer name so we know where our duplicates
                    # are. Turn into an array to create or append to our dict
                    # entry.
                    line_pair = "{}:{}".format(line, transfer_name)\
                        .split(" ", 1)
                    manifest_data.setdefault(line_pair[0], [])
                    manifest_data[line_pair[0]].append(line_pair[1].strip())
    output_duplicates(manifest_data)


if __name__ == '__main__':
    sys.exit(main())
