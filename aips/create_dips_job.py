#!/usr/bin/env python
"""
Create DIPs from an SS location

Get all AIPs from an existing SS instance, filtering them by location,
creating DIPs using the `create_dip` script and keeping track of them
in an SQLite database.

Optionally, uploads those DIPs to AtoM or the Storage Service using
the scripts from `dips` and deletes the local copy.
"""

import argparse
import logging.config  # Has to be imported separately
import os
import sys

import amclient
from sqlalchemy import exc

from aips import create_dip
from aips import models
from dips import atom_upload
from dips import storage_service_upload

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("dip_workflow")


def setup_logger(log_file, log_level="INFO"):
    """Configures the logger to output to console and log file"""
    if not log_file:
        log_file = os.path.join(THIS_DIR, "dip_workflow.log")

    CONFIG = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "default": {
                "format": "%(levelname)-8s  %(asctime)s  %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            }
        },
        "handlers": {
            "console": {"class": "logging.StreamHandler", "formatter": "default"},
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "default",
                "filename": log_file,
                "backupCount": 2,
                "maxBytes": 10 * 1024,
            },
        },
        "loggers": {
            "dip_workflow": {"level": log_level, "handlers": ["console", "file"]}
        },
    }

    logging.config.dictConfig(CONFIG)


def main(
    ss_url,
    ss_user,
    ss_api_key,
    location_uuid,
    origin_pipeline_uuid,
    tmp_dir,
    output_dir,
    database_file,
    delete_local_copy,
    upload_type,
    pipeline_uuid,
    cp_location_uuid,
    ds_location_uuid,
    shared_directory,
    atom_url,
    atom_email,
    atom_password,
    atom_slug,
    rsync_target,
):
    LOGGER.info("Processing AIPs in SS location: %s", location_uuid)

    # Idempotently create database and Aip table and create session
    try:
        session = models.init(database_file)
    except OSError:
        LOGGER.error("Could not create database in: %s", database_file)
        return 1

    # Get UPLOADED and VERIFIED AIPs from the SS
    try:
        am_client = amclient.AMClient(
            ss_url=ss_url, ss_user_name=ss_user, ss_api_key=ss_api_key
        )
        # There is an issue in the SS API that avoids
        # filtering the results by location. See:
        # https://github.com/artefactual/archivematica-storage-service/issues/298
        aips = am_client.aips({"status__in": "UPLOADED,VERIFIED"})
    except Exception as e:
        LOGGER.error(e)
        return 2

    # Get only AIPs from the specified location and origin pipeline
    aip_uuids = filter_aips(aips, location_uuid, origin_pipeline_uuid)

    # Create DIPs for those AIPs
    for uuid in aip_uuids:
        try:
            # To avoid race conditions while checking for an existing AIP
            # and saving it, create the row directly and check for an
            # integrity error exception (the uuid is a unique column)
            db_aip = models.Aip(uuid=uuid)
            session.add(db_aip)
            session.commit()
        except exc.IntegrityError:
            session.rollback()
            LOGGER.debug("Skipping AIP (already processed/processing): %s", uuid)
            continue

        mets_type = "atom"
        if upload_type == "ss-upload":
            mets_type = "storage-service"

        dip_path = create_dip.main(
            ss_url=ss_url,
            ss_user=ss_user,
            ss_api_key=ss_api_key,
            aip_uuid=uuid,
            tmp_dir=tmp_dir,
            output_dir=output_dir,
            mets_type=mets_type,
        )

        # Do not try upload on creation error
        if isinstance(dip_path, int):
            LOGGER.error("Could not create DIP from AIP: %s", uuid)
            continue

        if upload_type == "ss-upload":
            storage_service_upload.main(
                ss_url=ss_url,
                ss_user=ss_user,
                ss_api_key=ss_api_key,
                pipeline_uuid=pipeline_uuid,
                cp_location_uuid=cp_location_uuid,
                ds_location_uuid=ds_location_uuid,
                shared_directory=shared_directory,
                dip_path=dip_path,
                aip_uuid=uuid,
                delete_local_copy=delete_local_copy,
            )
        elif upload_type == "atom-upload":
            atom_upload.main(
                atom_url=atom_url,
                atom_email=atom_email,
                atom_password=atom_password,
                atom_slug=atom_slug,
                rsync_target=rsync_target,
                dip_path=dip_path,
                delete_local_copy=delete_local_copy,
            )

    LOGGER.info("All AIPs have been processed")


def filter_aips(aips, location_uuid, origin_pipeline_uuid):
    """
    Filters a list of AIPs based on a location UUID.

    :param list aips: list of AIPs from the results of an SS response
    :param str location_uuid: UUID from the SS location
    :param str origin_pipeline_uuid: UUID from the origin pipeline
    :returns: list of UUIDs from the AIPs in that location
    """
    location = f"/api/v2/location/{location_uuid}/"
    filtered_aips = []

    for aip in aips:
        if "uuid" not in aip:
            LOGGER.warning("Skipping AIP (missing UUID in SS response)")
            continue
        if "current_location" not in aip:
            LOGGER.debug("Skipping AIP (missing location): %s", aip["uuid"])
            continue
        if aip["current_location"] != location:
            LOGGER.debug("Skipping AIP (different location): %s", aip["uuid"])
            continue
        if origin_pipeline_uuid:
            pipeline = f"/api/v2/pipeline/{origin_pipeline_uuid}/"
            if "origin_pipeline" not in aip:
                LOGGER.debug("Skipping AIP (missing pipeline): %s", aip["uuid"])
                continue
            if aip["origin_pipeline"] != pipeline:
                LOGGER.debug("Skipping AIP (different pipeline): %s", aip["uuid"])
                continue
        filtered_aips.append(aip["uuid"])

    return filtered_aips


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--ss-url",
        metavar="URL",
        help="Storage Service URL. Default: http://127.0.0.1:8000",
        default="http://127.0.0.1:8000",
    )
    parser.add_argument(
        "--ss-user",
        metavar="USERNAME",
        required=True,
        help="Username of the Storage Service user to authenticate as.",
    )
    parser.add_argument(
        "--ss-api-key",
        metavar="KEY",
        required=True,
        help="API key of the Storage Service user.",
    )
    parser.add_argument(
        "--location-uuid",
        metavar="UUID",
        required=True,
        help="UUID of an AIP Storage location in the Storage Service.",
    )
    parser.add_argument(
        "--origin-pipeline-uuid",
        metavar="UUID",
        help="Optionally, filter AIPs by origin pipeline.",
        default=None,
    )
    parser.add_argument(
        "--database-file",
        metavar="PATH",
        required=True,
        help="Absolute path to an SQLite database file.",
    )
    parser.add_argument(
        "--tmp-dir",
        metavar="PATH",
        help="Absolute path to the directory used for temporary files. Default: /tmp.",
        default="/tmp",
    )
    parser.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Absolute path to the directory used to place the final DIP. Default: /tmp.",
        default="/tmp",
    )

    # Logging
    parser.add_argument(
        "--log-file", metavar="FILE", help="Location of log file", default=None
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase the debugging output.",
    )
    parser.add_argument(
        "--quiet", "-q", action="count", default=0, help="Decrease the debugging output"
    )
    parser.add_argument(
        "--log-level",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        default=None,
        help="Set the debugging output level. This will override -q and -v",
    )

    # Delete argument can't be set in the two subparsers bellow with the same name
    parser.add_argument(
        "--delete-local-copy",
        action="store_true",
        help="Deletes the local DIPs after upload if any of the upload arguments is used.",
    )

    # Create optional upload type subparsers
    subparsers = parser.add_subparsers(
        dest="upload_type",
        title="Upload options",
        description="The following arguments allow to upload the DIP after creation:",
        help="Leave empty to keep the DIP in the output path.",
    )

    # Storage Service upload subparser with extra SS required arguments
    parser_ss = subparsers.add_parser(
        "ss-upload",
        help="Storage Service upload. Check 'create_dips_job ss-upload -h'.",
    )
    parser_ss.add_argument(
        "--pipeline-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the Archivemativa pipeline in the Storage Service",
    )
    parser_ss.add_argument(
        "--cp-location-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the pipeline's Currently Processing location in the Storage Service",
    )
    parser_ss.add_argument(
        "--ds-location-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the pipeline's DIP storage location in the Storage Service",
    )
    parser_ss.add_argument(
        "--shared-directory",
        metavar="PATH",
        help="Absolute path to the pipeline's shared directory.",
        default="/var/archivematica/sharedDirectory/",
    )

    # AtoM upload subparser with AtoM required arguments
    parser_atom = subparsers.add_parser(
        "atom-upload", help="AtoM upload. Check 'create_dips_job atom-upload -h'."
    )
    parser_atom.add_argument(
        "--atom-url", metavar="URL", required=True, help="AtoM instance URL."
    )
    parser_atom.add_argument(
        "--atom-email",
        metavar="EMAIL",
        required=True,
        help="Email of the AtoM user to authenticate as.",
    )
    parser_atom.add_argument(
        "--atom-password",
        metavar="PASSWORD",
        required=True,
        help="Password of the AtoM user.",
    )
    parser_atom.add_argument(
        "--atom-slug",
        metavar="SLUG",
        required=True,
        help="AtoM archival description slug to target the upload.",
    )
    parser_atom.add_argument(
        "--rsync-target",
        metavar="HOST:PATH",
        required=True,
        help="Destination value passed to Rsync.",
    )

    args = parser.parse_args()

    log_levels = {2: "ERROR", 1: "WARNING", 0: "INFO", -1: "DEBUG"}
    if args.log_level is None:
        level = args.quiet - args.verbose
        level = max(level, -1)  # No smaller than -1
        level = min(level, 2)  # No larger than 2
        log_level = log_levels[level]
    else:
        log_level = args.log_level

    setup_logger(args.log_file, log_level)

    # Transform args Namespace to dict to be able to use get()
    # as some of the args defined in subsets may be missing.
    args_dict = vars(args)

    sys.exit(
        main(
            ss_url=args_dict.get("ss_url"),
            ss_user=args_dict.get("ss_user"),
            ss_api_key=args_dict.get("ss_api_key"),
            location_uuid=args_dict.get("location_uuid"),
            origin_pipeline_uuid=args_dict.get("origin_pipeline_uuid"),
            tmp_dir=args_dict.get("tmp_dir"),
            output_dir=args_dict.get("output_dir"),
            database_file=args_dict.get("database_file"),
            delete_local_copy=args_dict.get("delete_local_copy"),
            upload_type=args_dict.get("upload_type"),
            pipeline_uuid=args_dict.get("pipeline_uuid"),
            cp_location_uuid=args_dict.get("cp_location_uuid"),
            ds_location_uuid=args_dict.get("ds_location_uuid"),
            shared_directory=args_dict.get("shared_directory"),
            atom_url=args_dict.get("atom_url"),
            atom_email=args_dict.get("atom_email"),
            atom_password=args_dict.get("atom_password"),
            atom_slug=args_dict.get("atom_slug"),
            rsync_target=args_dict.get("rsync_target"),
        )
    )
