#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Return all duplicates from the Archivematica Storage Service.

The script utilizes the AM Client module. The fulcrum is the extract_file
endpoint and the bag manifest. An example use, if we call it via the API is:

    http -v --pretty=format \
        GET "http://127.0.0.1:62081/api/v2/file/<package_uuid>/extract_file/?relative_path_to_file=<transfer_name>-<package_uuid>/manifest-sha256.txt" \
        Authorization:"ApiKey test:test" | less

"""
from __future__ import print_function, unicode_literals

import logging
import os
import shutil
import sys
from tempfile import mkdtemp

try:
    from .digital_object import DigitalObject
    from . import hashutils
    from . import loggingconfig
    from .appconfig import AppConfig
    from .parsemets import read_premis_data
    from . import utils
except ValueError:
    from digital_object import DigitalObject
    import hashutils
    import loggingconfig
    from appconfig import AppConfig
    from parsemets import read_premis_data
    import utils


logging_dir = os.path.dirname(os.path.abspath(__file__))


logger = logging.getLogger("duplicates")
logger.disabled = False


MANIFEST_DATA = "manifest_data"


class ExtractError(Exception):
    """Custom exception for handling extract errors."""


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
    """do something."""
    manifest_data = duplicate_report.get(MANIFEST_DATA, {})
    for _, value in manifest_data.items():
        for package in value:
            if package_uuid != package.package_uuid:
                continue
            for dates in date_info:
                path_ = package.filepath.replace(os.path.join("data", ""), "")
                if path_ == dates.get("filepath", ""):
                    package.date_modified = dates.get("date_modified", "")
                    break


def read_mets(mets_loc):
    """test..."""
    return read_premis_data(mets_loc)


def retrieve_mets(am, duplicate_report, temp_dir):
    """Retrieve METS from our packages with duplicate files and retrieve useful
    information.
    """
    for key, value in duplicate_report.get("packages", {}).items():
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


def create_packages_section(duplicate_report):
    """Create a packages section to our report to make it easier to use the
    output.
    """
    packages = {}
    for _, values in duplicate_report.get(MANIFEST_DATA, {}).items():
        for entry in values:
            packages[entry.package_uuid] = entry.package_name
    duplicate_report["packages"] = packages
    return duplicate_report


def output_report(_):
    """Output some sort of duplicates report if desired."""
    print("Outputting report: We still need to implement this")


def retrieve_aip_index():
    """Script's primary entry-point."""
    temp_dir = mkdtemp()
    am = AppConfig().get_am_client()
    # Maintain state of all values across the aipstore.
    duplicate_report = {}
    manifest_data = {}
    # Get all AIPS that the storage service knows about.
    aips = am.aips()
    if not aips:
        sys.exit("Nothing to do: There are no AIPs")
    for aip in aips:
        package_name = os.path.basename(aip.get("current_path"))
        for ext in utils.EXTS:
            # TODO: make this more accurate...
            package_name = package_name.replace(ext, "")
        package_uuid = aip.get("uuid")
        hashes = hashutils.Hashes()
        for algorithm in hashes.checksum_algorithms:
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
                            filepath = filepath.strip()
                            obj = DigitalObject()
                            obj.package_uuid = am.package_uuid.strip()
                            obj.package_name = package_name.strip()
                            obj.filepath = filepath
                            obj.set_basename(filepath)
                            obj.set_dirname(filepath)
                            obj.hashes = {checksum.strip(): algorithm}
                            manifest_data.setdefault(checksum.strip(), [])
                            manifest_data[checksum].append(obj)
            duplicate_report[MANIFEST_DATA] = manifest_data

    # Add packages to report to make it easier to retrieve METS.
    create_packages_section(duplicate_report)

    # Retrieve METS and augment the data with date_modified information.
    retrieve_mets(am, duplicate_report, temp_dir)

    # Cleanup our temporary folder.
    shutil.rmtree(temp_dir)

    # Return our complete AIP manifest to the caller
    return duplicate_report


def main():
    """Script's primary entry-point."""
    report = retrieve_aip_index()
    output_report(report)


if __name__ == "__main__":
    loggingconfig.setup("INFO", os.path.join(logging_dir, "report.log"))
    sys.exit(main())
