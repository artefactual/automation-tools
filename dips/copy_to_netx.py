#!/usr/bin/env python
"""
Copies a DIP to NetX.

Copies a local DIP to NetX, providing a CSV list of each object.
"""

import argparse
import csv
import logging
import logging.config  # Has to be imported separately
import lxml.etree
import os
import shutil
import tempfile
import sys


THIS_DIR = os.path.abspath(os.path.dirname(__file__))
LOGGER = logging.getLogger("copy_to_netx")


def setup_logger(log_file, log_level="INFO"):
    """Configures the logger to output to console and log file"""
    if not log_file:
        log_file = os.path.join(THIS_DIR, "copy_to_netx.log")

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
            "copy_to_netx": {"level": log_level, "handlers": ["console", "file"]}
        },
    }

    logging.config.dictConfig(CONFIG)


def uuid_from_dip_path(dip_path):
    return dip_path[-36:]


def mets_filename_for_dip(dip_path):
    return "METS.%s.xml" % uuid_from_dip_path(dip_path)


def change_premis_namespace_to_v2(namespaces):
    namespaces_old_premis = namespaces.copy()
    namespaces_old_premis["premis"] = "info:lc/xmlns/premis-v2"

    return namespaces_old_premis


def parse_component_id_from_mets(mets_data, namespaces):
    """Parse component ID from METS file

    If a netx.json file is added to the metadata folder of a transfer, in the
    below format, then the value will end up in the METS file and will be used
    as a component ID.

    [{"component.identifier": "someidentifier"}]
    """
    path_to_component_id = "/mets:mets/mets:amdSec/mets:techMD/mets:mdWrap/mets:xmlData/premis:object/premis:objectCharacteristics/premis:objectCharacteristicsExtension/fits:fits/fits:toolOutput/fits:tool/exiftool/Componentidentifier"

    identifier = mets_data.xpath(path_to_component_id, namespaces=namespaces)

    if len(identifier):
        return identifier[0].text

    # Try again using V2 PREMIS namespace
    identifier = mets_data.xpath(
        path_to_component_id, namespaces=change_premis_namespace_to_v2(namespaces)
    )

    if len(identifier):
        return identifier[0].text


def parse_object_id_from_mets(mets_data, namespaces):
    """Parse object ID from METS file

    If an accession number is specified when starting a transfer then the
    value will end up in the METS file.

    This logic should be made more robust in the future.

    """
    path_to_accession_number = "/mets:mets/mets:amdSec/mets:techMD/mets:mdWrap/mets:xmlData/premis:object/premis:objectCharacteristics/premis:objectCharacteristicsExtension/fits:fits/fits:toolOutput/fits:tool/exiftool/MetsMetsHdrAltRecordID"

    accession_number = mets_data.xpath(path_to_accession_number, namespaces=namespaces)

    if len(accession_number):
        return accession_number[0].text

    # Try again, using V2 PREMIS namespace, to find it as parsed from JSON
    accession_number = mets_data.xpath(
        path_to_accession_number, namespaces=change_premis_namespace_to_v2(namespaces)
    )

    if len(accession_number):
        return accession_number[0].text


def write_csv_and_copy_objects(
    netx_csv_directory, netx_objects_directory, dip_path, object_id, component_id
):
    """Cycle through objects, copying to destination path and and writing CSV file of files/attributes"""
    csv_filepath = os.path.join(netx_csv_directory, "metadata.csv")
    needs_header = False if os.path.isfile(csv_filepath) else True

    with open(csv_filepath, "a") as csv_file:
        writer = csv.writer(
            csv_file, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
        )

        if needs_header:
            # Write header CSV row
            writer.writerow(["filename", "ObjectID", "Component Number", "dip_uuid"])

        objects_path = os.path.join(dip_path, "objects")
        sip_uuid = uuid_from_dip_path(dip_path)

        for object_file in os.listdir(objects_path):
            # Copy file to NetX directory
            shutil.copyfile(
                os.path.join(objects_path, object_file),
                os.path.join(netx_objects_directory, object_file),
            )

            # Write CSV row
            writer.writerow([object_file, object_id, component_id, sip_uuid])


def main(
    working_directory,
    dip_path,
    netx_csv_directory,
    netx_objects_directory,
    object_id,
    component_id,
    delete_local_copy,
):
    # Make sure NetX directories exist
    if not os.path.exists(netx_csv_directory):
        LOGGER.error(
            "The specified NetX CSV directory doesn't exist: %s" % netx_csv_directory
        )
        return 1

    if not os.path.exists(netx_objects_directory):
        LOGGER.error(
            "The specified NetX objects directory doesn't exist: %s"
            % netx_objects_directory
        )
        return 1

    # Copy DIP to the currently processing location path, do not use any of the
    # existing watched directories as that may trigger other workflows.
    at_dips_dir = os.path.join(working_directory, "automationToolsCopyToNetX")
    if not os.path.exists(at_dips_dir):
        os.makedirs(at_dips_dir)
    upload_dir_name = os.path.basename(dip_path)
    upload_dip_dir = os.path.join(at_dips_dir, upload_dir_name)

    # Stop if the DIP already exists in the working directory
    if os.path.exists(upload_dip_dir):
        LOGGER.error("A directory already exists for the DIP in: %s" % upload_dip_dir)
        return 1

    try:
        shutil.copytree(dip_path, upload_dip_dir)
    except (OSError, shutil.Error) as e:
        LOGGER.warning("Could not move DIP to currently processing path: %s", e)
        return 2

    # Attempt to read component and object IDs from metadata if not specified
    mets_filename = mets_filename_for_dip(dip_path)
    mets_data = lxml.etree.parse(os.path.join(dip_path, mets_filename))

    namespaces = {
        "mets": "http://www.loc.gov/METS/",
        "premis": "http://www.loc.gov/premis/v3",
        "dcterms": "http://purl.org/dc/terms/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "fits": "http://hul.harvard.edu/ois/xml/ns/fits/fits_output",
    }

    if component_id is None:
        component_id = parse_component_id_from_mets(mets_data, namespaces)
        LOGGER.info("Parsed component ID '%s' from METS" % component_id)

    if object_id is None:
        object_id = parse_object_id_from_mets(mets_data, namespaces)
        LOGGER.info("Parsed object ID '%s' from METS" % object_id)

    write_csv_and_copy_objects(
        netx_csv_directory, netx_objects_directory, dip_path, object_id, component_id
    )

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

    return 0


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dip-path",
        metavar="PATH",
        required=True,
        help="Absolute path to the DIP to upload.",
    )
    parser.add_argument(
        "--netx-csv-directory",
        metavar="PATH",
        required=True,
        help="Absolute path to the NetX CSV directory.",
    )
    parser.add_argument(
        "--netx-objects-directory",
        metavar="PATH",
        required=True,
        help="Absolute path to the NetX objects directory.",
    )
    parser.add_argument("--object-id", metavar="ID", help="Object ID to note in CSV.")
    parser.add_argument(
        "--component-id", metavar="ID", help="Component ID to note in CSV."
    )
    parser.add_argument(
        "--working-directory",
        metavar="PATH",
        help="Absolute path in which a temporary working directory will be created.",
        default=tempfile.gettempdir(),
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
            working_directory=args.working_directory,
            dip_path=args.dip_path,
            netx_csv_directory=args.netx_csv_directory,
            netx_objects_directory=args.netx_objects_directory,
            object_id=args.object_id,
            component_id=args.component_id,
            delete_local_copy=args.delete_local_copy,
        )
    )
