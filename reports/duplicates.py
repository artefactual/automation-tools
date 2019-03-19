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
import shutil
import sys
from tempfile import mkdtemp


logging_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from amclient import AMClient
from parsemets import read_premis_data
from transfers import loggingconfig


logger = logging.getLogger("transfers")


class ExtractError(Exception):
    """Custom exception for handling extract errors."""


def json_pretty_print(json_string):
    """Pretty print a JSON string."""
    print(json.dumps(json_string, sort_keys=True, indent=4))


def retrieve_file(am, package_uuid, save_as_loc, relative_path):
    """Test"""
    # Provide those arguments to amclient.
    am.package_uuid = package_uuid
    am.saveas_filename = save_as_loc
    am.relative_path = relative_path
    # We can read the response headers if we like.
    resp = am.extract_file()
    if isinstance(resp, int) or resp is None:
        raise ExtractError("Unable to retrieve file from the Storage Service")
    return resp


def filter_aip_files(filename, package_name, package_uuid):
    """Don't return AIP special files as duplicates."""
    name_replace = "{{transfer-name}}"
    uuid_replace = "{{transfer-uuid}}"
    aip_files = [
        "data/logs/filenameCleanup.log",
        "data/README.html",
        "data/logs/fileFormatIdentification.log",
        "data/logs/transfers/{{transfer-name}}/logs/filenameCleanup.log",
        "data/METS.{{transfer-uuid}}.xml",
        "data/objects/metadata/transfers/{{transfer-name}}/directory_tree.txt",
        "data/logs/transfers/{{transfer-name}}/logs/fileFormatIdentification.log",
        "data/objects/submissionDocumentation/transfer-{{transfer-name}}/METS.xml",
    ]
    for file_ in aip_files:
        if file_.replace(name_replace, package_name) == filename:
            return True
        if file_.replace(uuid_replace, package_uuid) == filename:
            return True
        if file_ == filename:
            return True
    return False


def read_mets(mets_loc):
    """test..."""
    return read_premis_data(mets_loc)


def retrieve_mets(am, manifest_data, temp_dir):
    """test..."""
    for checksums, values in manifest_data.items():
        if len(values) > 1:
            for entry in values:
                entry = entry.split(":", 2)
                filename = entry[0]
                print(filename)
                transfer = entry[1]
                package_uuid = entry[2]
                mets = "{}/data/METS.{}.xml".format(transfer, package_uuid)
                save_as_loc = os.path.join(temp_dir, mets.replace("/", "-"))
                if not os.path.exists(save_as_loc):
                    try:
                        retrieve_file(am, package_uuid, save_as_loc, mets)
                        data = read_mets(save_as_loc)
                        for d_ in data:
                            print(d_)
                    except ExtractError as err:
                        logger.info(err)
                        continue


def output_duplicates(manifest_data):
    """Cycle through our AIP manifests and format the output before sending
    to stdout as JSON. If any JSON object has an array length greater than one
    then it represents duplicates that have been discovered. Output a list of
    those files only.
    """
    json_out = {}
    for checksums, values in manifest_data.items():
        if len(values) > 1:
            for entry in values:
                entry = entry.split(":", 2)
                json_out.setdefault(checksums, [])
                json_out[checksums].append(entry)
    json_pretty_print(json_out)


def main():
    """Script's primary entry-point."""

    temp_dir = mkdtemp()

    loggingconfig.setup("INFO", os.path.join(logging_dir, "report.log"))
    am = AMClient()
    am.ss_url = "http://127.0.0.1:62081"
    am.ss_user_name = "test"
    am.ss_api_key = "test"

    # Maintain state of all values across the aipstore {"checksum": [paths]}
    manifest_data = {}

    checksum_algorithms = ("md5", "sha1", "sha256")

    # Get all AIPS that the storage service knows about.
    aips = am.aips()
    for aip in aips:
        package_name = os.path.basename(aip.get("current_path")).replace(".7z", "")
        for algorithm in checksum_algorithms:
            # Store our manifest somewhere.
            relative_path = "{}/manifest-{}.txt".format(package_name, algorithm)
            save_path = "{}-manifest-{}.txt".format(package_name, algorithm)

            save_as_loc = os.path.join(temp_dir, save_path)

            print(relative_path)

            try:
                retrieve_file(am, aip.get("uuid"), save_as_loc, relative_path)
            except ExtractError:
                logger.info("No result for algorithm: %s", algorithm)
                continue

            # Our dictionary keys are checksums and all filename entries with
            # the same checksum are appended to create an array. If the array
            # at the end is greater than one, we have duplicate files.
            with open(save_as_loc, "r") as manifest_extract:
                for line in manifest_extract:
                    if line.strip() != "":
                        # Attach our transfer name so we know where our duplicates
                        # are. Turn into an array to create or append to our dict
                        # entry.
                        filename = line.split(" ", 1)[1].strip()
                        if not filter_aip_files(
                            filename, package_name, am.package_uuid
                        ):
                            line_pair = "{}:{}:{}".format(
                                line.strip(),
                                package_name.strip(),
                                am.package_uuid.strip(),
                            ).split(" ", 1)
                            manifest_data.setdefault(line_pair[0], [])
                            manifest_data[line_pair[0]].append(line_pair[1].strip())

    # Test duplicate response.
    retrieve_mets(am, manifest_data, temp_dir)

    # Once we have collected all our objects, output the duplicates as JSON.
    # output_duplicates(manifest_data)

    # Cleanup our temporary folder.
    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    sys.exit(main())
