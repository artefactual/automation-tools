#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Return all duplicates from the Archivematica Storage Service

Example usage:

$:~/git/archivematica/automation-tools$ python -m reports.duplicates.duplicates 2> /dev/null

Duplicate entries, per algorithm found, will be output to stdout, e.g.:

    {
        "manifest_data": {
            "078917a9ba3eb290ddb27f97d904cf6e24fec5f62a1986fdf760c07d6d4dd30e": [
                {
                    "date_modified": "2018-01-31",
                    "filepath": "data/objects/sci-fi.jpg",
                    "package_name": "1-588790bd-b9dd-4460-9705-d14f8700dba3",
                    "package_uuid": "588790bd-b9dd-4460-9705-d14f8700dba3"
                },
                {
                    "date_modified": "2018-01-31",
                    "filepath": "data/objects/sci-fi.jpg",
                    "package_name": "2-ba01e3f6-eb6b-4eb5-a8a8-c1ae10200b66",
                    "package_uuid": "ba01e3f6-eb6b-4eb5-a8a8-c1ae10200b66"
                }
            ],
            "233aa737752ffb64942ca18f03dd6d316957c5b7a0c439e07cdae9963794c315": [
                {
                    "date_modified": "2018-02-01",
                    "filepath": "data/objects/garage.jpg",
                    "package_name": "1-588790bd-b9dd-4460-9705-d14f8700dba3",
                    "package_uuid": "588790bd-b9dd-4460-9705-d14f8700dba3"
                },
                {
                    "date_modified": "2018-02-01",
                    "filepath": "data/objects/garage.jpg",
                    "package_name": "2-ba01e3f6-eb6b-4eb5-a8a8-c1ae10200b66",
                    "package_uuid": "ba01e3f6-eb6b-4eb5-a8a8-c1ae10200b66"
                }
            ]
        },
        "packages": {
            "588790bd-b9dd-4460-9705-d14f8700dba3": "1-588790bd-b9dd-4460-9705-d14f8700dba3",
            "ba01e3f6-eb6b-4eb5-a8a8-c1ae10200b66": "2-ba01e3f6-eb6b-4eb5-a8a8-c1ae10200b66"
        }
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

from . import loggingconfig

from .appconfig import AppConfig
from .parsemets import read_premis_data

from pandas import DataFrame

logging_dir = os.path.dirname(os.path.abspath(__file__))


logger = logging.getLogger("duplicates")
logger.disabled = False


class ExtractError(Exception):
    """Custom exception for handling extract errors."""


def json_pretty_print(json_string):
    """Pretty print a JSON string."""
    return json.dumps(json_string, sort_keys=True, indent=4)


def retrieve_file(am, package_uuid, save_as_loc, relative_path):
    """Helper function to retrieve our files from the Storage Service."""
    am.package_uuid = package_uuid
    am.saveas_filename = save_as_loc
    am.relative_path = relative_path
    # We can read the response headers if we like.
    resp = am.extract_file()
    if isinstance(resp, int) or resp is None:
        raise ExtractError("Unable to retrieve file from the Storage Service")
    return resp


def filter_aip_files(filepath, package_uuid):
    """Don't return AIP special files as duplicates."""
    filepath = filepath.strip()
    uuid_replace = "{{package-uuid}}"
    transfer_files = [
        ["data/logs/transfers/", "/logs/filenameCleanup.log"],
        ["data/objects/metadata/transfers/", "/directory_tree.txt"],
        ["data/logs/transfers/", "/logs/fileFormatIdentification.log"],
        ["data/objects/submissionDocumentation/", "/METS.xml"],
    ]
    aip_files = [
        "data/logs/filenameCleanup.log",
        "data/README.html",
        "data/logs/fileFormatIdentification.log",
        "data/METS.{{package-uuid}}.xml",
    ]
    for file_ in transfer_files:
        if file_[0] in filepath and file_[1] in filepath:
            logger.info("Filtering: %s", filepath)
            return True
    for file_ in aip_files:
        if file_.replace(uuid_replace, package_uuid) == filepath:
            logger.info("Filtering: %s", filepath)
            return True
        if file_ == filepath:
            logger.info("Filtering: %s", filepath)
            return True
    return False


def augment_data(package_uuid, duplicate_report, date_info):
    manifest_data = duplicate_report.get("manifest_data", {})
    for key, value in manifest_data.items():
        for package in value:
            if package_uuid != package.get("package_uuid", ""):
                continue
            for dates in date_info:
                path_ = package.get("filepath", "").strip(os.path.join("data", ""))
                if path_ == dates.get("filepath", ""):
                    package["date_modified"] = dates.get("date_modified", "")


def read_mets(mets_loc):
    """test..."""
    return read_premis_data(mets_loc)


def retrieve_mets(am, duplicate_report, temp_dir):
    """Retrieve METS from our packages with duplicate files and retrieve useful
    information.
    """
    for key, value in duplicate_report.get("packages", {}).items():
        """do nothing"""
        package_uuid = key
        package_name = value
        mets = "{}/data/METS.{}.xml".format(package_name, package_uuid)
        save_as_loc = os.path.join(temp_dir, mets.replace("/", "-"))
        if not os.path.exists(save_as_loc):
            try:
                retrieve_file(am, package_uuid, save_as_loc, mets)
                data = read_mets(save_as_loc)
                augment_data(package_uuid, duplicate_report, data)
            except ExtractError as err:
                logger.info(err)
                continue


def filter_duplicates(duplicate_report):
    """Filter our report for packages containing duplicates only."""
    dupes = dict(duplicate_report.get("manifest_data", {}))
    packages = {}
    for key, values in dupes.items():
        if len(values) > 1:
            for entry in values:
                packages[entry.get("package_uuid")] = entry.get("package_name")
        else:
            try:
                duplicate_report.get("manifest_data", {}).pop(key)
                logger.info("Popped checksum: %s", key)
            except (AttributeError, KeyError):
                raise ExtractError("Error filtering report for duplicates")
    duplicate_report["packages"] = packages
    return duplicate_report


def csv_out(duplicate_report, filename):
    """Output a CSV using Pandas and a bit of magic."""
    dupes = duplicate_report.get("manifest_data", {})
    cols = 0
    arr = [
        "file_path",
        "date_modified",
        "base_name",
        "dir_name",
        "package_name",
        "package_uuid",
    ]
    rows = []
    headers = None
    for key, value in dupes.items():
        cols = max(cols, len(value))
    # Create headers for our spreadsheet.
    headers = arr * cols
    for i in range(len(headers)):
        headers[i] = "{}_{}".format(headers[i], str(i).zfill(2))
    # Make sure that checksum is the first and only non-duplicated value.
    headers = ["Checksum"] + headers
    for key, value in dupes.items():
        records = []
        for prop in value:
            record = []
            record.append(prop.get("filepath", "NaN"))
            record.append(prop.get("date_modified", "NaN"))
            record.append(prop.get("basename", "NaN"))
            record.append(prop.get("dirname", "NaN"))
            record.append(prop.get("package_name", "NaN"))
            record.append(prop.get("package_uuid", "NaN"))
            records = records + record
        # Fill blank spaces in row. Might also be possible as a Pandas series.
        space = cols * len(arr) - len(records)
        if space:
            filler = ["NaN"] * space
            records = records + filler
        # Create a checksum entry for our spreadsheet.
        records = [key] + records
        # Create a dict from two lists.
        dictionary = dict(zip(headers, records))
        rows.append(dictionary)
    df = DataFrame(columns=headers)
    for entry in rows:
        df = df.append(entry, ignore_index=True)
        # Sort the columns in alphabetical order to pair similar headers.
        cols = sorted(df.columns.tolist())
        cols_no_suffix = [x.rsplit("_", 1)[0] for x in cols]
        df = df[cols]
    df.to_csv(filename, index=None, header=cols_no_suffix, encoding="utf8")


def output_report(duplicate_report):
    """Provide mechanisms to output different serializations."""
    with open("aipstore-duplicates.json", "w") as json_file:
        json_file.write(json_pretty_print(duplicate_report))
    print(json_pretty_print(duplicate_report))
    csv_out(duplicate_report, "aipstore-duplicates.csv")


def main():
    """Script's primary entry-point."""
    temp_dir = mkdtemp()
    loggingconfig.setup("INFO", os.path.join(logging_dir, "report.log"))
    am = AppConfig().get_am_client()
    # Maintain state of all values across the aipstore.
    duplicate_report = {}
    manifest_data = {}
    # Checksum algorithms to test for.
    checksum_algorithms = ("md5", "sha1", "sha256")
    # Get all AIPS that the storage service knows about.
    aips = am.aips()
    for aip in aips:
        package_name = os.path.basename(aip.get("current_path")).replace(".7z", "")
        package_uuid = aip.get("uuid")
        for algorithm in checksum_algorithms:
            # Store our manifest somewhere.
            relative_path = "{}/manifest-{}.txt".format(package_name, algorithm)
            save_path = "{}-manifest-{}.txt".format(package_name, algorithm)
            save_as_loc = os.path.join(temp_dir, save_path)
            try:
                retrieve_file(am, package_uuid, save_as_loc, relative_path)
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
                        checksum, filepath = line.split(" ", 1)
                        if not filter_aip_files(filepath, package_uuid):
                            entry = {}
                            filepath = filepath.strip()
                            entry["package_uuid"] = am.package_uuid.strip()
                            entry["package_name"] = package_name.strip()
                            entry["filepath"] = filepath
                            entry["basename"] = os.path.basename(filepath)
                            entry["dirname"] = os.path.dirname(filepath)
                            manifest_data.setdefault(checksum.strip(), [])
                            manifest_data[checksum].append(entry)
            duplicate_report["manifest_data"] = manifest_data
    duplicate_report = filter_duplicates(duplicate_report)
    retrieve_mets(am, duplicate_report, temp_dir)
    # Save to JSON and CSV.
    output_report(duplicate_report)
    # Cleanup our temporary folder.
    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    sys.exit(main())
