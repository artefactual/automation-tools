#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Script to create candidate transfers in Archivematica (in a storage
location titled 'Automated candidate transfers').

Candidate transfers are generated from CSV files that require three fields:

   * "keep",
   * "path",
   * "hash",
   * "in_transfer_name",

Which will be used to determine what to move to a new transfer source (the
'Automated candidate transfers' folder).

* If keep is populated with any data, it is marked to be kept.

* "in_transfer_name" + "path" are combined to create the path where the data
  is currently.

* The data is then moved to an objects folder in a new transfer, and optionally
  a manifest created describing that transfer.
"""

from __future__ import print_function, unicode_literals

import argparse
import datetime
import errno
import logging
import os
import shutil
import sys

try:
    from .appconfig import AppConfig
    from . import loggingconfig
except (ValueError, ImportError):
    from appconfig import AppConfig
    import loggingconfig

import pandas


logging_dir = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger("candidates")

location_exists = False
transfer_name = None
candidate_list = []


class CreateCandidateError(Exception):
    """Custom exception for handling extract errors."""


def setup():
    """Capture any setup work this script needs to do."""
    now = datetime.datetime.now()
    now = now.strftime("%Y%m%d%H%M%S")
    number = len(os.listdir(AppConfig().default_path)) + 1
    number = "%03d" % number
    agent = AppConfig().candidate_agent
    global transfer_name
    transfer_name = "{}_{}_{}_candidate_transfer".format(now, agent, number).upper()


def _get_spaces(locations):
    """TODO: replace with a specific AMClient call to retrieve Spaces in
    the Storage Service. The call in AMClient doesn't exist yet.
    """
    all_spaces = []
    for location in locations.get("objects"):
        all_spaces.append(location.get("space"))
    return list(set(all_spaces))


def create_location(am, pipeline):
    """Create a location to move our candidate transfers to. If everything
    works as anticipated here, and the creation is either created, or already
    exists, we return the path we will use as confirmation to the caller.
    """
    candidates_location_desc = AppConfig().candidate_location
    relative_path = AppConfig().default_path
    space = AppConfig().default_space
    locations = am.list_storage_locations()
    if isinstance(locations, int):
        raise CreateCandidateError("Error returned from AMClient: {}".format(locations))
    for location in locations.get("objects"):
        if (
            location.get("description") == candidates_location_desc
            and location.get("relative_path") == relative_path
        ):
            global location_exists
            location_exists = True
    if not location_exists:
        space_uri = "/api/v2/space/{}/".format(space)
        if space_uri not in _get_spaces(locations):
            raise CreateCandidateError("Space hasn't been created to create location")
        logger.info("Creating location for candidate transfers")
        am.location_purpose = "TS"
        am.location_description = candidates_location_desc
        am.pipeline_uuids = pipeline
        am.space_uuid = AppConfig().default_space
        am.space_relative_path = relative_path
        am.default = False
        location = am.create_location()
        if isinstance(location, int):
            raise CreateCandidateError(
                "Error returned from AMClient: {}".format(location)
            )
        if "relative_path" not in location:
            raise CreateCandidateError("Problem creating location: {}".format(location))
    try:
        os.mkdir(relative_path)
    except OSError as err:
        if err.errno != errno.EEXIST:
            raise CreateCandidateError(
                "Permission denied, you need to create the directory manually via the console: {}".format(
                    err
                )
            )
        logger.info("Directory exists: %s", err)
    return relative_path


def _grab_hash_and_algorithm(hash_):
    """Return a checksum file associated with a particular hashing algorithm.
    """
    hash_, algorithm = hash_.split(" ")
    algorithm = algorithm.replace("(", "").replace(")", "")
    algorithm = "checksum.{}".format(algorithm)
    return hash_, algorithm


def make_metadata(metadata_location, path_in_transfer, hash_):
    """Append checksums to the checksum files in our metadata folder."""
    try:
        os.makedirs(metadata_location)
    except OSError as err:
        if err.errno != errno.EEXIST:
            pass
    hash_, algorithm = _grab_hash_and_algorithm(hash_)
    metadata_file = os.path.join(metadata_location, algorithm)
    with open(metadata_file, "a+") as metadata:
        line = "{}   {}\n".format(hash_, path_in_transfer)
        metadata.write(line)


def move_to_location():
    """Move our files to a new candidate transfer location."""
    seen = []
    am = AppConfig().get_am_client()
    # Retrieve pipeline.
    pipeline_res = am.get_pipelines()
    pipeline = pipeline_res.get("objects", {})[0].get("uuid")
    # Retrieve space.
    # TODO: develop code to create a space in amclient (2 hours?)
    # Create location.
    candidate_location = create_location(am, pipeline=pipeline)
    for candidate in candidate_list:
        """
           {'path': '/home/ross-spencer/.../StructMapTransferSamples/UnicodeEncodedExample/objects/Página_06.jpg',
            'hash': '040b356585b96f4903aeb9e5287e4560 (md5)',
            'in_transfer_name': 'StructMapTransferSamples'
           }
        """
        transfer = candidate.get("in_transfer_name")
        copy_path = candidate.get("path")
        path_in_transfer, filename = os.path.split(copy_path.split(transfer, 1)[1])
        path_in_transfer = path_in_transfer.strip(os.path.sep)
        new_path = os.path.join(
            candidate_location, transfer_name, "objects", path_in_transfer
        )
        try:
            os.makedirs(new_path)
        except OSError as err:
            if err.errno != errno.EEXIST:
                pass
        # metadata_location = os.path.join(candidate_location, transfer_name, "metadata")
        transfer_location = os.path.join(new_path, filename)
        path_in_transfer = os.path.join(path_in_transfer, filename)
        if transfer_location not in seen:
            seen.append(shutil.copyfile(copy_path, transfer_location))

            # FIXME: this outputs too many lines, or we don't copy enough files...
            # make_metadata(metadata_location, path_in_transfer, candidate.get("hash"))

        else:
            logger.warning("Item already seen in move: %s", transfer_location)


def process_csv(csv_file):
    """Read the given CSV file and extract the """
    # keep_list = None
    data = pandas.read_csv(csv_file, header=0)
    files_to_keep = ~data["keep"].isna()
    if not files_to_keep.any():
        return
    # Convert to list of dicts with info we need to keep.
    global candidate_list
    candidate_list = candidate_list + data[files_to_keep][
        ["path", "hash", "in_transfer_name"]
    ].to_dict("records")


def main(csv_list):
    """Process the csv list as much as is required."""
    setup()
    for csv_file in csv_list:
        process_csv(csv_file)
    move_to_location()


if __name__ == "__main__":
    """Primary entry point for this script."""
    loggingconfig.setup("INFO", os.path.join(logging_dir, "report.log"))
    if len(sys.argv) <= 1:
        sys.exit("Nothing to do")
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="append")
    args = parser.parse_args()
    for csv_ in args.csv:
        if not os.path.exists(csv_):
            sys.exit("CSV {} doesn'exist".format(csv_))
    sys.exit(main(args.csv))
