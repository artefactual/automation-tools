#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals

import copy
import logging
import os
import sys

try:
    from .appconfig import AppConfig
    from .digital_object import DigitalObject
    from . import duplicates
    from . import loggingconfig
    from .serialize_to_csv import CSVOut
    from . import utils
except (ValueError, ImportError):
    from appconfig import AppConfig
    from digital_object import DigitalObject
    import duplicates
    import loggingconfig
    from serialize_to_csv import CSVOut
    import utils

logging_dir = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger("accruals")
logger.disabled = False

# Location purpose = Transfer Source (TS)
location_purpose = "TS"
default_location = AppConfig().accruals_transfer_source


# Do something with this...
DOCKER = True

# Store our appraisal paths.
accrual_paths = []


def create_manifest(aip_index, accrual_objs):
    """do something."""
    dupes = []
    near_matches = []
    non_matches = []
    aip_obj_hashes = aip_index.get(duplicates.MANIFEST_DATA)
    for accrual_obj in accrual_objs:
        for accrual_hash in accrual_obj.hashes:
            if accrual_hash in aip_obj_hashes.keys():
                for _, aip_items in aip_obj_hashes.items():
                    for aip_item in aip_items:
                        if accrual_obj == aip_item:
                            accrual_obj.flag = True
                            cp = copy.copy(accrual_obj)
                            cp.package_name = aip_item.package_name
                            dupes.append(cp)
                        else:
                            diff = accrual_obj % aip_item
                            if (
                                diff == "No matching components"
                                or "checksum match" not in diff
                            ):
                                """Don't output."""
                                continue
                            accrual_obj.flag = True
                            cp1 = copy.copy(accrual_obj)
                            cp2 = copy.copy(aip_item)
                            near_matches.append([cp1, cp2])
                # Only need one hash to match then break.
                # May also be redundant as we only have one hash from the
                # bag manifests...
                break
    for accrual_obj in accrual_objs:
        if accrual_obj.flag is False:
            cp = copy.copy(accrual_obj)
            if cp not in non_matches:
                non_matches.append(cp)
    return dupes, near_matches, non_matches


def create_comparison_obj(transfer_path):
    """Do something."""
    transfer_arr = []
    for root, dirs, files in os.walk(transfer_path, topdown=True):
        for name in files:
            file_ = os.path.join(root, name)
            if os.path.isfile(file_):
                transfer_arr.append(DigitalObject(file_, transfer_path))
    return transfer_arr


def stat_transfers(accruals_path, all_transfers):
    """Retrieve all transfer paths and make a request to generate statistics
    about all the objects in that transfer path.
    """
    aip_index = duplicates.retrieve_aip_index()
    dupe_reports = []
    near_reports = []
    no_match_reports = []
    transfers = []
    for transfer in all_transfers:
        transfer_home = os.path.join(accruals_path, transfer)
        if DOCKER:
            transfer_home = utils.get_docker_path(transfer_home)
        objs = create_comparison_obj(transfer_home)
        transfers.append(objs)
        match_manifest, near_manifest, no_match_manifest = create_manifest(
            aip_index, objs
        )
        if match_manifest:
            dupe_reports.append({transfer: match_manifest})
        if near_manifest:
            near_reports.append({transfer: near_manifest})
        if no_match_manifest:
            no_match_reports.append({transfer: no_match_manifest})
    CSVOut.output_reports(
        aip_index, transfers, dupe_reports, near_reports, no_match_reports
    )


def main(location=default_location):
    """Primary entry point for this script."""

    am = AppConfig().get_am_client()
    sources = am.list_storage_locations()

    accruals = False
    for source in sources.get("objects"):
        if (
            source.get("purpose") == location_purpose
            and source.get("description") == location
        ):
            """do something."""
            am.transfer_source = source.get("uuid")
            am.transfer_path = source.get("path")
            accruals = True
    if not accruals:
        logger.info("Exiting. No transfer source: {}".format(location))
        sys.exit()

    # All transfer directories. Assumption is the same as Archivematica that
    # each transfer is organized into a single directory at this level.
    all_transfers = am.transferables().get("directories")
    stat_transfers(am.transfer_path, all_transfers)


if __name__ == "__main__":
    loggingconfig.setup("INFO", os.path.join(logging_dir, "report.log"))
    source = default_location
    try:
        source = sys.argv[1:][0]
        logger.error("Attempting to find transfers at: %s", source)
    except IndexError:
        pass
    sys.exit(main(source))
