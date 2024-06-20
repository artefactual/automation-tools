#!/usr/bin/env python
"""
Uploads a DIP to an Storage Service instance.

Uploads a local DIP to a DIP storage location in an SS instance.
Requires access to a pipeline's currently processing location path (the
shared path), to move the DIP folder in there and send a requests to the
Storage Service to process that DIP and create a relationship with the
AIP from where it was created.
"""

import argparse
import logging.config  # Has to be imported separately
import os
import shutil
import sys
import uuid

import requests

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
    pipeline_uuid,
    cp_location_uuid,
    ds_location_uuid,
    shared_directory,
    dip_path,
    aip_uuid,
    delete_local_copy,
):
    # Move DIP to the currently processing location path, do not use any of the
    # existing watched directories as that may trigger other workflows.
    at_dips_dir = os.path.join(
        shared_directory, "watchedDirectories", "automationToolsDIPs"
    )
    if not os.path.exists(at_dips_dir):
        os.makedirs(at_dips_dir)
    upload_dir_name = os.path.basename(dip_path)
    upload_dip_dir = os.path.join(at_dips_dir, upload_dir_name)

    # Stop if the DIP already exists in the shared directory
    if os.path.exists(upload_dip_dir):
        LOGGER.error("A directory already exists for the DIP in: %s" % upload_dip_dir)
        return 1

    try:
        shutil.copytree(dip_path, upload_dip_dir)
    except (OSError, shutil.Error) as e:
        LOGGER.warning("Could not move DIP to currently processing path: %s", e)
        return 2

    # Build DIP data for SS request
    size = 0
    for dirpath, _, filenames in os.walk(upload_dip_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            size += os.path.getsize(file_path)
    dip_data = {
        "uuid": str(uuid.uuid4()),  # new UUID
        "origin_pipeline": "/api/v2/pipeline/%s/" % pipeline_uuid,
        "origin_location": "/api/v2/location/%s/" % cp_location_uuid,
        "origin_path": "watchedDirectories/automationToolsDIPs/%s/" % upload_dir_name,
        "current_location": "/api/v2/location/%s/" % ds_location_uuid,
        "current_path": upload_dir_name,
        "package_type": "DIP",
        "aip_subtype": "Archival Information Package",  # same as in AM
        "size": size,
        "related_package_uuid": aip_uuid,
        "events": [],
        "agents": [],
    }
    # TODO: Move this to amclient.
    LOGGER.info("Storing DIP in Storage Service.")
    url = "%s/api/v2/file/" % ss_url
    headers = {"Authorization": f"ApiKey {ss_user}:{ss_api_key}"}
    response = requests.post(url, headers=headers, json=dip_data, timeout=86400)
    result = 0
    if response.status_code != requests.codes.created:
        LOGGER.error("Could not store DIP in Storage Service: %s", response.text)
        result = 3
    else:
        LOGGER.info("DIP stored.")
        ret = response.json()
        if "uuid" in ret:
            LOGGER.info("Storage Service DIP UUID: %s" % ret["uuid"])
        else:
            LOGGER.warning("Storage Service didn't return the DIP UUID")

    # Finally remove the DIP from the currently processing location
    LOGGER.info("Removing duplicates.")
    try:
        shutil.rmtree(upload_dip_dir)
    except (OSError, shutil.Error) as e:
        LOGGER.warning("Duplicates removal failed: %s", e)

    # And remove the local copy if requested
    if delete_local_copy:
        LOGGER.info("Deleting local DIP.")
        try:
            shutil.rmtree(dip_path)
        except (OSError, shutil.Error) as e:
            LOGGER.warning("DIP removal failed: %s", e)

    return result


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
        "--pipeline-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the Archivemativa pipeline in the Storage Service",
    )
    parser.add_argument(
        "--cp-location-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the pipeline's Currently Processing location in the Storage Service",
    )
    parser.add_argument(
        "--ds-location-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the pipeline's DIP storage location in the Storage Service",
    )
    parser.add_argument(
        "--shared-directory",
        metavar="PATH",
        help="Absolute path to the pipeline's shared directory.",
        default="/var/archivematica/sharedDirectory/",
    )
    parser.add_argument(
        "--dip-path",
        metavar="PATH",
        required=True,
        help="Absolute path to the DIP to upload.",
    )
    parser.add_argument(
        "--aip-uuid", metavar="UUID", required=True, help="UUID of the related AIP"
    )
    parser.add_argument(
        "--delete-local-copy",
        action="store_true",
        help="Deletes the local DIP after upload.",
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

    sys.exit(
        main(
            ss_url=args.ss_url,
            ss_user=args.ss_user,
            ss_api_key=args.ss_api_key,
            pipeline_uuid=args.pipeline_uuid,
            cp_location_uuid=args.cp_location_uuid,
            ds_location_uuid=args.ds_location_uuid,
            shared_directory=args.shared_directory,
            dip_path=args.dip_path,
            aip_uuid=args.aip_uuid,
            delete_local_copy=args.delete_local_copy,
        )
    )
