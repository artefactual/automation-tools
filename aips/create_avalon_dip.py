#!/usr/bin/env python
"""
Create Avalon DIP

This script creates a DIP suitable for deposit into an Avalon Media System dropbox.
1. Downloads an AIP from the Storage Service,
2. Creates a DIP with only assets included,
3. Finds and updates root-level CSV with UUID fields,
4. Moves DIP to folder with transfer name in specified output directory.
"""

import argparse
import csv
import logging
import logging.config  # Has to be imported separately
import os
import sys
import subprocess
import shutil

import amclient
import metsrw

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("create_avalon_dip")


def setup_logger(log_file, log_level="INFO"):
    """Configures the logger to output to console and log file"""
    if not log_file:
        log_file = os.path.join(THIS_DIR, "create_avalon_dip.log")

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
            "create_avalon_dip": {"level": log_level, "handlers": ["console", "file"]}
        },
    }

    logging.config.dictConfig(CONFIG)


def main(ss_url, ss_user, ss_api_key, aip_uuid, tmp_dir, output_dir):
    LOGGER.info("Starting DIP creation from AIP: %s", aip_uuid)

    if not os.path.isdir(tmp_dir):
        LOGGER.error("%s is not a valid temporary directory", tmp_dir)
        return 1

    if not os.path.isdir(output_dir):
        LOGGER.error("%s is not a valid output directory", output_dir)
        return 2

    # Create empty workspace directory
    tmp_dir = os.path.join(tmp_dir, aip_uuid)
    if os.path.exists(tmp_dir):
        LOGGER.warning("Workspace directory already exists, overwriting")
        shutil.rmtree(tmp_dir)
    try:
        os.makedirs(tmp_dir)
    except OSError:
        LOGGER.error("Could not create workspace directory: %s", tmp_dir)
        return 3

    LOGGER.info("Downloading AIP from Storage Service")

    am_client = amclient.AMClient(
        aip_uuid=aip_uuid,
        ss_url=ss_url,
        ss_user_name=ss_user,
        ss_api_key=ss_api_key,
        directory=tmp_dir,
    )

    aip_file = am_client.download_aip()

    if not aip_file:
        LOGGER.error("Unable to download AIP")
        return 4

    LOGGER.info("Extracting AIP")
    aip_dir = extract_aip(aip_file, aip_uuid, tmp_dir)

    if not aip_dir:
        return 5

    LOGGER.info("Creating DIP")
    dip_dir = create_avalon_dip(aip_dir, aip_uuid, output_dir)

    if not dip_dir:
        LOGGER.error("Unable to create DIP")
        return 6

    # Remove workspace directory
    shutil.rmtree(tmp_dir)

    LOGGER.info("DIP created in: %s", dip_dir)

    # Move into Collection folder


def extract_aip(aip_file, aip_uuid, tmp_dir):
    """
    Extracts an AIP to a folder.

    :param str aip_file: absolute path to an AIP
    :param str aip_uuid: UUID from the AIP
    :param str tmp_dir: absolute path to a directory to place the extracted AIP
    :returns: absolute path to the extracted AIP folder
    """
    command = ["7z", "x", "-bd", "-y", "-o{0}".format(tmp_dir), aip_file]
    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        LOGGER.error("Could not extract AIP, error: %s", e.output)
        return

    # Remove extracted file to avoid multiple entries with the same UUID
    try:
        os.remove(aip_file)
    except OSError:
        pass

    # Find extracted entry. Assuming it contains the AIP UUID
    for entry in os.listdir(tmp_dir):
        if aip_uuid in entry:
            extracted_entry = os.path.join(tmp_dir, entry)

    if not extracted_entry:
        LOGGER.error("Can not find extracted AIP by UUID")
        return

    # Return folder path if it's a directory
    if os.path.isdir(extracted_entry):
        return extracted_entry

    # Re-try extraction if it's not a directory
    return extract_aip(extracted_entry, aip_uuid, tmp_dir)


def create_avalon_dip(aip_dir, aip_uuid, output_dir):
    """
    Creates a DIP from an uncompressed AIP.

    :param str aip_dir: absolute path to an uncompressed AIP
    :param str aip_uuid: UUID from the AIP
    :param str output_dir: absolute path to a directory to place the DIP
    :returns: absolute path to the created DIP folder
    """
    aip_name = os.path.basename(aip_dir)[:-37]
    dip_dir = os.path.join(output_dir, "{}/{}".format(aip_name, aip_uuid))

    if os.path.exists(dip_dir):
        LOGGER.warning("DIP folder already exists, overwriting")
        shutil.rmtree(dip_dir)
    os.makedirs(dip_dir)

    LOGGER.info("Moving METS file")
    aip_mets_file = "{}/data/METS.{}.xml".format(aip_dir, aip_uuid)

    mets = metsrw.METSDocument.fromfile(aip_mets_file)
    namespaces = metsrw.utils.NAMESPACES.copy()
    premis_map = metsrw.plugins.premisrw.utils.PREMIS_VERSIONS_MAP
    fsentries = mets.all_files()
    for fsentry in fsentries:
        if fsentry.use != "original" or not fsentry.path or not fsentry.file_uuid:
            continue

        LOGGER.info("Moving file: %s", fsentry.path)
        aip_file_path = os.path.join(os.path.join(aip_dir, "data"), fsentry.path)
        if not os.path.exists(aip_file_path):
            LOGGER.warning("Could not find file in AIP")
            continue

        if not len(fsentry.amdsecs):
            LOGGER.warning("Missing amdSec in METS file")
            continue

        amdsec = fsentry.amdsecs[0]
        for item in amdsec.subsections:
            if item.subsection == "techMD":
                techmd = item
        if not techmd:
            LOGGER.warning("techMD section could not be found")
            continue

        if techmd.contents.mdtype != "PREMIS:OBJECT":
            LOGGER.warning("premis:object could not be found")
            continue

        premis = techmd.contents.document

        # Update PREMIS namespace based on version attribute
        premis_version = premis.get("version", "2.2")
        try:
            namespaces["premis"] = premis_map[premis_version]["namespaces"]["premis"]
        except KeyError:
            LOGGER.warning(
                "Could not update namespace for PREMIS version: %s" % premis_version
            )
        original_name = premis.findtext("premis:originalName", namespaces=namespaces)

        if not original_name:
            LOGGER.warning("premis:originalName could not be found")
            continue

        string_start = "%transferDirectory%objects/"
        if original_name[:27] != string_start:
            LOGGER.warning("premis:originalName not starting with %s", string_start)
            continue

        # Move original file with original name
        dip_file_path = os.path.join(dip_dir, original_name[27:])
        dip_dir_path = os.path.dirname(dip_file_path)
        if not os.path.exists(dip_dir_path):
            os.makedirs(dip_dir_path)

        shutil.move(aip_file_path, dip_file_path)

    # Update Manifest file with UUIDs
    files = os.listdir(dip_dir)
    paths = [fn for fn in files if fn.endswith(".csv")]
    csv_path = ""
    if len(paths) and not len(paths) > 1:
        csv_path = dip_dir + "/" + paths[0]
        tmp_csv_path = dip_dir + "/tmp.csv"
        with open(csv_path, "r") as csv_input, open((tmp_csv_path), "w") as csv_output:
            reader = csv.reader(csv_input)
            writer = csv.writer(csv_output, lineterminator="\n")

            all = []
            # Skip row one, Add to row two
            first_row = next(reader)
            row = next(reader)
            row.append("Other Identifier")
            row.append("Other Identifier Label")
            all.append(first_row)
            all.append(row)

            for row in reader:
                row.append(aip_uuid)
                row.append("other")
                all.append(row)

            writer.writerows(all)
        os.rename(tmp_csv_path, csv_path)

    if not csv_path:
        LOGGER.error("Manifest file could not be found")

    return dip_dir


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
        "--aip-uuid",
        metavar="UUID",
        required=True,
        help="UUID of the AIP in the Storage Service",
    )
    parser.add_argument(
        "--tmp-dir",
        metavar="PATH",
        help="Absolute path to the directory used for temporary files. Default: /tmp",
        default="/tmp",
    )
    parser.add_argument(
        "--output-dir",
        metavar="PATH",
        help="Absolute path to the Avalon dropbox directory. Default: /tmp",
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
            aip_uuid=args.aip_uuid,
            tmp_dir=args.tmp_dir,
            output_dir=args.output_dir,
        )
    )
