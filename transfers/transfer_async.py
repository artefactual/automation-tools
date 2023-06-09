#!/usr/bin/env python
"""
Automate Transfers.

Helper script to automate running transfers through Archivematica.

Similar to ``transfers.transfer`` but using the new `/api/v2beta` API when
possible.
"""
import base64
import os
import sys

import requests

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfers import transfer
from transfers.loggingconfig import set_log_level
from transfers import models
from transfers.transferargs import get_parser
from transfers.transfer import (
    LOGGER,
    get_next_transfer,
    get_accession_id,
    get_setting,
    main,
)
from os import fsdecode, fsencode


class DashboardAPIError(Exception):
    """Dashboard API error."""


def _api_create_package(
    am_url,
    am_user,
    am_api_key,
    name,
    package_type,
    accession,
    ts_location_uuid,
    ts_path,
    config_file,
):
    url = am_url + "/api/v2beta/package/"
    headers = {"Authorization": f"ApiKey {am_user}:{am_api_key}"}
    data = {
        "name": fsdecode(name),
        "type": package_type,
        "accession": accession,
        "path": fsdecode(base64.b64encode(fsencode(ts_location_uuid) + b":" + ts_path)),
        "processing_config": get_setting(config_file, "processingconfig", "default"),
    }
    LOGGER.debug("URL: %s; Headers: %s, Data: %s", url, headers, data)
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    LOGGER.debug("Response: %s", response)
    resp_json = response.json()
    error = resp_json.get("error")
    if error:
        raise DashboardAPIError(error)
    return resp_json


def _start_transfer(
    ss_url,
    ss_user,
    ss_api_key,
    ts_location_uuid,
    ts_path,
    depth,
    am_url,
    am_user,
    am_api_key,
    transfer_type,
    see_files,
    config_file,
):
    """
    Start a new transfer.

    This is an early implementation and an alternative to
    ``transfer.start_transfer`` that uses the new ``/api/v2beta/package`` API.
    Because transfers start immediately we don't run the scripts inside the
    pre-transfer directory like ``transfer.start_transfer`` does.

    :param ss_url: URL of the Storage Sevice to query
    :param ss_user: User on the Storage Service for authentication
    :param ss_api_key: API key for user on the Storage Service for
                       authentication
    :param ts_location_uuid: UUID of the transfer source Location
    :param ts_path: Relative path inside the Location to work with.
    :param depth: Depth relative to ts_path to create a transfer from. Should
                  be 1 or greater.
    :param am_url: URL of Archivematica pipeline to start transfer on
    :param am_user: User on Archivematica for authentication
    :param am_api_key: API key for user on Archivematica for authentication
    :param transfer_type: Transfer type to use in Archivematica
    :param bool see_files: If true, start transfers from files as well as
                           directories
    :param config_file: Path to Automation Tools configuration file
    :returns: Tuple of Transfer information about the new transfer or None on
              error.
    """
    # Start new transfer
    processed = models.get_processed_transfer_paths()
    target = get_next_transfer(
        ss_url=ss_url,
        ss_user=ss_user,
        ss_api_key=ss_api_key,
        ts_location_uuid=ts_location_uuid,
        path_prefix=ts_path,
        depth=depth,
        processed=processed,
        see_files=see_files,
    )
    if not target:
        # Report the location UUID.
        LOGGER.info(
            "All potential transfers in Location ID: %s have been created. Exiting",
            ts_location_uuid,
        )
        return None
    LOGGER.info("Starting with %s", target)
    # Get accession ID
    accession = get_accession_id(target)
    LOGGER.info("Accession ID: %s", accession)

    try:
        result = _api_create_package(
            am_url,
            am_user,
            am_api_key,
            os.path.basename(target),
            transfer_type,
            accession,
            ts_location_uuid,
            target,
            config_file,
        )
    except (requests.exceptions.HTTPError, ValueError, DashboardAPIError) as err:
        LOGGER.error("Unable to start transfer: %s", err)
        models.transfer_failed_to_start(target)
        return None

    LOGGER.info("Package created: %s", result["id"])
    new_transfer = models.add_new_transfer(uuid=result["id"], path=target)
    LOGGER.info("New transfer: %s", new_transfer)

    return new_transfer


if __name__ == "__main__":
    parser = get_parser(__doc__)
    args = parser.parse_args()

    # Override ``start_transfer`` function.
    transfer.start_transfer = _start_transfer

    sys.exit(
        main(
            am_user=args.user,
            am_api_key=args.api_key,
            ss_user=args.ss_user,
            ss_api_key=args.ss_api_key,
            ts_uuid=args.transfer_source,
            ts_path=args.transfer_path,
            depth=args.depth,
            am_url=args.am_url,
            ss_url=args.ss_url,
            transfer_type=args.transfer_type,
            see_files=args.files,
            hide_on_complete=args.hide,
            log_level=set_log_level(args.log_level, args.quiet, args.verbose),
            config_file=args.config_file,
        )
    )
