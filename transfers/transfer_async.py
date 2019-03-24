#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Automate Transfers.

Helper script to automate running transfers through Archivematica.

Similar to ``transfers.transfer`` but using the new `/api/v2beta` API when
possible.
"""

from __future__ import print_function, unicode_literals

import base64
import os
import sys

import requests

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfers import transfer
from transfers.loggingconfig import set_log_level
from transfers.models import Unit
from transfers.transferargs import get_parser
from transfers.transfer import LOGGER, get_next_transfer, get_accession_id, main
from transfers.utils import fsencode


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
):
    url = am_url + "/api/v2beta/package/"
    headers = {"Authorization": "ApiKey {}:{}".format(am_user, am_api_key)}
    data = {
        "name": name,
        "type": package_type,
        "accession": accession,
        "path": base64.b64encode(fsencode(ts_location_uuid) + b":" + ts_path),
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
    session,
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
    :param bool see_files: If true, start transfers from files as well as
                           directories
    :param session: SQLAlchemy session with the DB
    :returns: Tuple of Transfer information about the new transfer or None on
              error.
    """
    # Start new transfer
    completed = {x[0] for x in session.query(Unit.path).all()}
    target = get_next_transfer(
        ss_url,
        ss_user,
        ss_api_key,
        ts_location_uuid,
        ts_path,
        depth,
        completed,
        see_files,
    )
    if not target:
        LOGGER.warning(
            "All potential transfers in %s have been created. Exiting", ts_path
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
        )
    except (requests.exceptions.HTTPError, ValueError, DashboardAPIError) as err:
        LOGGER.error("Unable to start transfer: %s", err)
        new_transfer = Unit(
            path=target, unit_type="transfer", status="FAILED", current=False
        )
        session.add(new_transfer)
        return None

    LOGGER.info("Package created: %s", result["id"])
    new_transfer = Unit(
        uuid=result["id"], path=target, unit_type="transfer", current=True
    )
    LOGGER.info("New transfer: %s", new_transfer)
    session.add(new_transfer)

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
        )
    )
