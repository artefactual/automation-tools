#!/usr/bin/env python
"""
Create DIP from AIP

Downloads an AIP from the Storage Service and creates a DIP. Unlike DIPs created
in Archivematica, the ones created with this script will include only the original
files from the transfer and they will maintain the directories, filenames and last
modified date from those files. They will be placed in a single ZIP file under the
objects directory which will also include a copy of the submissionDocumentation
folder (if present in the AIP) and the AIP METS file. Another METS file will be
generated alongside the objects folder containing only a reference to the ZIP file
(without AMD or DMD sections).
"""
import argparse
import csv
import logging.config  # Has to be imported separately
import os
import shutil
import subprocess
import sys
import uuid

import amclient
import metsrw

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
    aip_uuid,
    tmp_dir,
    output_dir,
    mets_type="atom",
    dip_type="zipped-objects",
):
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
    dip_dir = create_dip(aip_dir, aip_uuid, output_dir, mets_type, dip_type)

    if not dip_dir:
        LOGGER.error("Unable to create DIP")
        return 6

    # Remove workspace directory
    shutil.rmtree(tmp_dir)

    LOGGER.info("DIP created in: %s", dip_dir)

    return dip_dir


def extract_aip(aip_file, aip_uuid, tmp_dir):
    """
    Extracts an AIP to a folder.

    :param str aip_file: absolute path to an AIP
    :param str aip_uuid: UUID from the AIP
    :param str tmp_dir: absolute path to a directory to place the extracted AIP
    :returns: absolute path to the extracted AIP folder
    """
    command = ["7z", "x", "-bd", "-y", f"-o{tmp_dir}", aip_file]
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


def create_dip(aip_dir, aip_uuid, output_dir, mets_type, dip_type):
    """
    Creates a DIP from an uncompressed AIP.

    :param str aip_dir: absolute path to an uncompressed AIP
    :param str aip_uuid: UUID from the AIP
    :param str output_dir: absolute path to a directory to place the DIP
    :param str mets_type: type of METS to generate within DIP
    :param str dip_type: type of DIP to generate
    :returns: absolute path to the created DIP folder
    """
    aip_dir_name = os.path.basename(aip_dir)
    aip_name = aip_dir_name[:-37]

    if dip_type == "avalon-manifest":
        dip_dir = os.path.join(output_dir, aip_name, aip_uuid)
        to_zip_dir = dip_dir
    else:
        dip_dir = os.path.join(output_dir, aip_dir_name)
        objects_dir = os.path.join(dip_dir, "objects")
        to_zip_dir = os.path.join(objects_dir, aip_name)

    if os.path.exists(dip_dir):
        LOGGER.warning("DIP folder already exists, overwriting")
        shutil.rmtree(dip_dir)

    os.makedirs(to_zip_dir)

    if dip_type != "avalon-manifest":
        move_sub_doc(aip_dir, to_zip_dir)

    LOGGER.info("Moving METS file")
    aip_mets_file = f"{aip_dir}/data/METS.{aip_uuid}.xml"
    if not os.path.exists(aip_mets_file):
        LOGGER.error("Could not find AIP METS file")
        return
    to_zip_mets_file = f"{to_zip_dir}/METS.{aip_uuid}.xml"
    shutil.move(aip_mets_file, to_zip_mets_file)

    mets = metsrw.METSDocument.fromfile(to_zip_mets_file)
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
        update_premis_ns(premis, namespaces, premis_map)

        original_name = get_premis_original_name(premis, namespaces)
        if not original_name:
            LOGGER.warning("Could not get original file name from premis:originalName")
            continue

        original_relpath = get_original_relpath(original_name)
        if not original_relpath:
            continue

        # Move original file with original file name and create parent folders
        dip_file_path = os.path.join(to_zip_dir, original_relpath)
        dip_dir_path = os.path.dirname(dip_file_path)
        if not os.path.exists(dip_dir_path):
            os.makedirs(dip_dir_path)

        shutil.move(aip_file_path, dip_file_path)

        if dip_type != "avalon-manifest":
            try:
                set_fslastmodified(premis, namespaces, dip_file_path)
            except Exception:
                LOGGER.warning("fits/fileinfo/fslastmodified not found")

    # Modify or copy METS file for DIP based on mets_type argument
    dip_mets_file = os.path.join(dip_dir, f"METS.{aip_uuid}.xml")

    if dip_type == "avalon-manifest":
        # Update Manifest file with UUIDs
        update_avalon_manifest(dip_dir, aip_uuid)
        os.remove(to_zip_mets_file)
    else:
        if mets_type == "atom":
            create_dip_mets(aip_dir, aip_name, fsentries, mets, dip_mets_file)
        elif mets_type == "storage-service":
            copy_aip_mets(to_zip_mets_file, dip_mets_file)
        compress_zip_folder(to_zip_dir)
        shutil.rmtree(to_zip_dir)

    return dip_dir


def create_dip_mets(aip_dir, aip_name, fsentries, mets, dip_mets_file):
    """Creates DIP METS file for AtoM/default upload."""

    LOGGER.info("Creating DIP METS file for AtoM/default upload.")
    objects_entry = None
    for fsentry in fsentries:
        # Do not delete AIP entry
        if (
            fsentry.label == os.path.basename(aip_dir)
            and fsentry.type.lower() == "directory"
        ):
            continue

        # Do not delete objects entry and save it for parenting
        if fsentry.label == "objects" and fsentry.type.lower() == "directory":
            objects_entry = fsentry
            continue

        # Delete all the others
        mets.remove_entry(fsentry)

    if not objects_entry:
        LOGGER.error("Could not find objects entry in METS file")
        return

    # Create new entry for ZIP file
    entry = metsrw.FSEntry(
        label=f"{aip_name}.zip",
        path=f"objects/{aip_name}.zip",
        file_uuid=str(uuid.uuid4()),
    )

    # Add new entry to objects directory
    objects_entry.add_child(entry)

    # Create DIP METS file
    try:
        mets.write(dip_mets_file, fully_qualified=True, pretty_print=True)
    except Exception:
        LOGGER.error("Could not create DIP METS file")
        return


def copy_aip_mets(to_zip_mets_file, dip_mets_file):
    """Copies AIP's METS file."""

    LOGGER.info("Copying AIP's METS file.")
    try:
        shutil.copy(to_zip_mets_file, dip_mets_file)
    except Exception:
        LOGGER.error("Could not create DIP METS file")
        return


def compress_zip_folder(to_zip_dir):
    """Compresses to_zip_dir inside the DIP objects folder"""

    LOGGER.info("Compressing ZIP folder inside objects")
    command = ["7z", "a", "-tzip", f"{to_zip_dir}.zip", to_zip_dir]
    try:
        subprocess.check_output(command, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        LOGGER.error("Could not compress ZIP folder, error: %s", e.output)
        return


def move_sub_doc(aip_dir, to_zip_dir):
    """Moves submissionDocumentation folder"""

    LOGGER.info("Moving submissionDocumentation folder")
    aip_sub_doc = f"{aip_dir}/data/objects/submissionDocumentation"
    if os.path.exists(aip_sub_doc):
        to_zip_sub_doc = os.path.join(to_zip_dir, "submissionDocumentation")
        shutil.move(aip_sub_doc, to_zip_sub_doc)
    else:
        LOGGER.warning("submissionDocumentation folder not found")


def set_fslastmodified(premis, namespaces, dip_file_path):
    """Obtain and set the fslastmodified date to the moved files"""

    fslastmodified = premis.findtext(
        "premis:objectCharacteristics/premis:objectCharacteristicsExtension/fits:fits/fits:fileinfo/fits:fslastmodified",
        namespaces=namespaces,
    )
    if not fslastmodified:
        LOGGER.warning("fits/fileinfo/fslastmodified not found")

    # Convert from miliseconds to seconds
    timestamp = int(fslastmodified) // 1000
    os.utime(dip_file_path, (timestamp, timestamp))


def update_avalon_manifest(dip_dir, aip_uuid):
    """Update Avalon Manifest CSV with AIP UUID"""

    files = os.listdir(dip_dir)
    paths = [fn for fn in files if fn.endswith(".csv")]
    csv_path = ""
    if len(paths) == 1:
        csv_path = os.path.join(dip_dir, paths[0])
        tmp_csv_path = os.path.join(dip_dir, "tmp.csv")
        with open(csv_path) as csv_input, open((tmp_csv_path), "w") as csv_output:
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
        shutil.move(tmp_csv_path, csv_path)

    if not csv_path:
        LOGGER.error("Manifest file could not be found!")


def update_premis_ns(premis, namespaces, premis_map):
    """Update PREMIS namespace based on version attribute"""
    premis_version = premis.get("version", "2.2")
    try:
        namespaces["premis"] = premis_map[premis_version]["namespaces"]["premis"]
    except KeyError:
        LOGGER.warning(
            "Could not update namespace for PREMIS version: %s" % premis_version
        )


def get_premis_original_name(premis, namespaces):
    """Get the original file name from a premis:originalName"""

    original_name = premis.findtext("premis:originalName", namespaces=namespaces)
    if not original_name:
        LOGGER.warning("premis:originalName could not be found")
        return None

    return original_name


def get_original_relpath(original_name):
    """Get the relative file path from a premis:originalName"""

    path_prefixes = ["%transferDirectory%objects/", "%transferDirectory%data/"]
    for prefix in path_prefixes:
        if original_name.startswith(prefix):
            return original_name[len(prefix) :]

    LOGGER.warning(
        '"%s" has an invalid path prefix, it must be one of ("%s")',
        original_name,
        '", "'.join(path_prefixes),
    )


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
        help="Absolute path to the directory used to place the final DIP. Default: /tmp",
        default="/tmp",
    )
    parser.add_argument(
        "--mets-type",
        choices=["atom", "storage-service"],
        default="atom",
        help="Generate METS file for AtoM upload or use the AIP's METS file for Storage Service upload. Default: atom.",
    )
    parser.add_argument(
        "--dip-type",
        choices=["avalon-manifest", "zipped-objects"],
        default="zipped-objects",
        help="Structure DIP for specific systems. Default: zipped-objects.",
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

    ret = main(
        ss_url=args.ss_url,
        ss_user=args.ss_user,
        ss_api_key=args.ss_api_key,
        aip_uuid=args.aip_uuid,
        tmp_dir=args.tmp_dir,
        output_dir=args.output_dir,
        mets_type=args.mets_type,
        dip_type=args.dip_type,
    )

    # The main function returns the DIP's path on success
    # or an int higher than 0 if it fails. The scrip will
    # always exit with an int, 0 on success.
    if not isinstance(ret, int):
        ret = 0

    sys.exit(ret)
