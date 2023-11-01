#!/usr/bin/env python
"""
Automate Transfers.

Helper script to automate running transfers through Archivematica.
"""
import ast
import atexit
import base64
import configparser
import logging
import os
import shutil
import subprocess
import sys
import time
from os import fsdecode
from os import fsencode

import requests
from amclient import AMClient
from sqlalchemy.orm.exc import NoResultFound

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfers import defaults, errors, loggingconfig, models, utils
from transfers.transferargs import get_parser


# Directory for various processing decisions, below.
THIS_DIR = os.path.abspath(os.path.dirname(__file__))

# Setup module level logging.
LOGGER = logging.getLogger("transfers")


def setup_automation_execution(pid_file):
    """Setup procedures for transfer.py."""
    atexit.register(manage_automation_execution, pid_file)


def manage_automation_execution(pid_file):
    """Cleanup procedures for transfer.py."""
    LOGGER.info("Running post-execution clean-up. Exiting script")
    os.remove(pid_file)
    models.cleanup_session()


def create_db_session(config_file):
    """Create and return a database session."""
    models.init_session(
        get_setting(config_file, "databasefile", os.path.join(THIS_DIR, "transfers.db"))
    )
    return models.Session()


def get_setting(config_file, setting, default=None):
    """Get an option value from the configuration file."""
    config = configparser.ConfigParser()
    section = "transfers"
    try:
        config.read(config_file)
        cfg = config.get(section, setting)
        LOGGER.info("Configuration values read for %s: %s", setting, cfg)
        return cfg
    except configparser.NoOptionError:
        LOGGER.warning("No option provided for %s", setting)
        return default
    except configparser.NoSectionError:
        LOGGER.warning("No section: %s in %s", section, config.sections())
        return default


def get_status(
    am_url,
    am_user,
    am_api_key,
    ss_url,
    ss_user,
    ss_api_key,
    unit_uuid,
    unit_type,
    transfer_delete_path,
    hide_on_complete=False,
    delete_on_complete=False,
    
):
    """
    Get status of the SIP or Transfer with unit_uuid.

    :param str unit_uuid: UUID of the unit to query for.
    :param str unit_type: 'ingest' or 'transfer'
    :param bool hide_on_complete: Hide the unit in the dashboard if COMPLETE
    :returns: Dict with status of the unit from Archivematica or None.
    """
    # Get status
    url = f"{am_url}/api/{unit_type}/status/{unit_uuid}/"
    params = {"username": am_user, "api_key": am_api_key}
    unit_info = utils._call_url_json(url, params)
    if isinstance(unit_info, int):
        if errors.error_lookup(unit_info) is not None:
            return errors.error_lookup(unit_info)
    # If complete, hide in dashboard
    if hide_on_complete and unit_info and unit_info.get("status") == "COMPLETE":
        LOGGER.info("Hiding %s %s in dashboard", unit_type, unit_uuid)
        url = f"{am_url}/api/{unit_type}/{unit_uuid}/delete/"
        LOGGER.debug("Method: DELETE; URL: %s; params: %s;", url, params)
        response = requests.delete(url, params=params)
        LOGGER.debug("Response: %s", response)
    # If Transfer is complete, get the SIP's status
    if (
        unit_info
        and unit_type == "transfer"
        and unit_info.get("status") == "COMPLETE"
        and unit_info.get("sip_uuid") != "BACKLOG"
    ):
        LOGGER.info(
            "%s is a complete transfer, fetching SIP %s status.",
            unit_uuid,
            unit_info.get("sip_uuid"),
        )
        # Update DB to refer to this one
        unit = models.retrieve_unit_by_type_and_uuid(
            uuid=unit_uuid, unit_type=unit_type
        )
        models.update_unit_type_and_uuid(
            unit=unit, unit_type="ingest", uuid=unit_info.get("sip_uuid")
        )
        # Get SIP status
        url = "{}/api/ingest/status/{}/".format(am_url, unit_info.get("sip_uuid"))
        unit_info = utils._call_url_json(url, params)
        if isinstance(unit_info, int):
            if errors.error_lookup(unit_info) is not None:
                return errors.error_lookup(unit_info)
        # If complete, hide in dashboard
        if hide_on_complete and unit_info and unit_info.get("status") == "COMPLETE":
            LOGGER.info("Hiding SIP %s in dashboard", unit.uuid)
            url = f"{am_url}/api/ingest/{unit.uuid}/delete/"
            LOGGER.debug("Method: DELETE; URL: %s; params: %s;", url, params)
            response = requests.delete(url, params=params)
            LOGGER.debug("Response: %s", response)
        # If complete and SIP status is 'UPLOADED', delete transfer source
        # files
        if delete_on_complete and unit_info and unit_info.get("status") == "COMPLETE":
            am = AMClient(
                ss_url=ss_url,
                ss_user_name=ss_user,
                ss_api_key=ss_api_key,
                package_uuid=unit.uuid,
            )
            response = am.get_package_details()
            if response.get("status") == "UPLOADED":
                LOGGER.info(
                    "Deleting source files for SIP %s from watched " "directory",
                    unit.uuid,
                )
                try:
                    # Use the transfer_delete_path provided by user ex: /transferSource/
                    deletePath = transfer_delete_path + unit.path.decode("UTF-8")
                    shutil.rmtree(deletePath)
                    LOGGER.info("Source files deleted for SIP %s " "deleted", unit.uuid)
                except OSError as e:
                    LOGGER.warning(
                        "Error deleting source files: %s. If "
                        "running this module remotely the "
                        "script might not have access to the "
                        "transfer source",
                        e,
                    )
    return unit_info


def get_accession_id(dirname):
    """
    Call get-accession-number and return literal_eval stdout as accession ID.

    get-accession-number should be in the same directory as transfer.py. Its
    only output to stdout should be the accession number surrounded by
    quotes.  Eg. "accession number"

    :param str dirname: Directory name of folder to become transfer
    :returns: accession number or None.
    """
    script_path = os.path.join(THIS_DIR, "get-accession-number")
    try:
        p = subprocess.Popen(
            [script_path, dirname],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as err:
        LOGGER.warning("Error: %s when trying to run %s", err, script_path)
        return None
    output, err = p.communicate()
    if p.returncode != 0:
        LOGGER.error(
            "Error running %s %s: RC: %s; stdout: %s; stderr: %s",
            script_path,
            dirname,
            p.returncode,
            output,
            err,
        )
        return None
    output = fsdecode(output)
    try:
        return ast.literal_eval(output)
    except (ValueError, SyntaxError) as err:
        LOGGER.warning(
            "Unable to parse output from %s. Output: %s. %s", script_path, output, err
        )
        return None


def run_pre_transfer_scripts(config_file, transfer_path, transfer_type):
    """Wrapper for the run_scripts function. Pre-transfer functions want to
    modify the transfer itself, therefore the transfer path sent by the
    calling function function should at least be a valid one. If run_scripts
    results in an OSError exception then the calling function should take
    responsibility for working with that.
    """
    if not os.path.exists(transfer_path):
        LOGGER.error("Invalid transfer path for the pre-transfer scripts to work with")
    else:
        run_scripts("pre-transfer", config_file, transfer_path, transfer_type)


def run_scripts(directory, config_file, *args):
    """
    Run all executable scripts in directory relative to this file.

    :param str directory: Dir in the same folder as this file to run scripts
    :param args: All other parameters will be passed to called scripts.
    :return: None
    """
    directory = os.path.join(THIS_DIR, directory)
    if not os.path.isdir(directory):
        LOGGER.warning("%s is not a directory. No scripts to run.", directory)
        return
    script_args = list(args)
    LOGGER.debug("script_args: %s", script_args)
    script_extensions = get_setting(config_file, "scriptextensions", "").split(":")
    LOGGER.debug("script_extensions: %s", script_extensions)
    for script in sorted(os.listdir(directory)):
        LOGGER.debug("Script: %s", script)
        script_path = os.path.realpath(os.path.join(directory, script))
        if not os.path.isfile(script_path):
            LOGGER.info("%s is not a file, skipping", script_path)
            continue
        if not os.access(script_path, os.X_OK):
            LOGGER.info("%s is not executable, skipping", script_path)
            continue
        script_name, script_ext = os.path.splitext(script)
        if script_extensions and script_ext not in script_extensions:
            LOGGER.info(
                "'%s' for '%s' not in configured list of script file "
                "extensions, skipping",
                script_ext,
                script_path,
            )
            continue
        LOGGER.info('Running %s "%s"', script_path, '" "'.join(args))
        p = subprocess.Popen(
            [script_path] + script_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = p.communicate()
        LOGGER.info("Return code: %s", p.returncode)
        LOGGER.info("stdout: %s", stdout)
        if stderr:
            LOGGER.warning("stderr: %s", stderr)


def get_next_transfer(
    ss_url,
    ss_user,
    ss_api_key,
    ts_location_uuid,
    path_prefix,
    depth,
    processed,
    see_files,
):
    """
    Helper to find the first directory that doesn't have an associated
    transfer.

    :param ss_url:           URL of the Storage Service to query
    :param ss_user:          User on the Storage Service for authentication
    :param ss_api_key:       API key for user on the Storage Service for
                             authentication
    :param ts_location_uuid: UUID of the transfer source Location
    :param path_prefix:      Relative path inside the Location to work with.
    :param depth:            Depth relative to path_prefix to create a transfer
                             from. Should be 1 or greater.
    :param set processed:    Set of the paths of processed by the automation
                             tools in the database. Ideally, relative to the
                             same transfer source location, including the same
                             path_prefix, and at the same depth. Paths include
                             those currently processing and completed.
    :param bool see_files:   Return files as well as folders to become
                             transfers.
    :returns:                Path relative to TS Location of the new transfer.
    """
    # Get sorted list from source directory.
    url = ss_url + "/api/v2/location/" + ts_location_uuid + "/browse/"
    params = {"username": ss_user, "api_key": ss_api_key}
    if path_prefix:
        params["path"] = base64.b64encode(path_prefix)
    browse_info = utils._call_url_json(url, params)
    if isinstance(browse_info, int):
        if errors.error_lookup(browse_info) is not None:
            LOGGER.error(
                "Error when browsing location: %s", errors.error_lookup(browse_info)
            )
            return None
    if browse_info is None:
        return None
    if see_files:
        entries = browse_info["entries"]
    else:
        entries = browse_info["directories"]
    entries = [base64.b64decode(e.encode("utf8")) for e in entries]
    LOGGER.debug("Entries: %s", entries)
    LOGGER.info("Total files or folders in transfer source location: %s", len(entries))
    entries = [os.path.join(path_prefix, e) for e in entries]
    # If at the correct depth, check if any of these have not been made into
    # transfers yet
    if depth <= 1:
        # Find the directories that are not already in the DB using sets
        entries = set(entries) - processed
        LOGGER.debug("New transfer candidates: %s", entries)
        LOGGER.info("Unprocessed entries to choose from: %s", len(entries))
        # Sort, take the first
        entries = sorted(list(entries))
        if not entries:
            LOGGER.info("All potential transfers in %s have been created.", path_prefix)
            return None
        target = entries[0]
        return target
    else:  # if depth > 1
        # Recurse on each directory
        for entry in entries:
            LOGGER.debug("New path: %s", entry)
            target = get_next_transfer(
                ss_url=ss_url,
                ss_user=ss_user,
                ss_api_key=ss_api_key,
                ts_location_uuid=ts_location_uuid,
                path_prefix=entry,
                depth=depth - 1,
                processed=processed,
                see_files=see_files,
            )
            if target:
                return target
    return None


def call_start_transfer_endpoint(
    am_url, am_user, am_api_key, target, transfer_type, accession, ts_location_uuid
):
    """Make the call to the start_transfer endpoint and return the unapproved
    directory name, and current (absolute path), of the transfer as a tuple.
    """
    url = f"{am_url}/api/transfer/start_transfer/"
    params = {"username": am_user, "api_key": am_api_key}
    target_name = os.path.basename(target)
    data = {
        "name": target_name,
        "type": transfer_type,
        "accession": accession,
        "paths[]": [base64.b64encode(fsencode(ts_location_uuid) + b":" + target)],
        "row_ids[]": [""],
    }
    LOGGER.debug("URL: %s; Params: %s; Data: %s", url, params, data)
    response = requests.post(url, params=params, data=data)
    LOGGER.debug("Response: %s", response)
    try:
        resp_json = response.json()
        # Retrieve transfer_name, and the absolute path to the transfer for the
        # calling function.
        transfer_abs_path = resp_json.get("path")
        return os.path.basename(transfer_abs_path.strip(os.sep)), transfer_abs_path
    except ValueError:
        LOGGER.error(
            "Could not parse JSON from response Response: %s: %s, %s",
            response.status_code,
            response.reason,
            response.headers,
        )
        # Debug log, rather than Warning as the response from the server is
        # likely to be HTML and likely to be too verbose to be useful.
        LOGGER.debug("Could not parse JSON from response: %s", response.text)
        return None, None
    if not response.ok or resp_json.get("error"):
        LOGGER.error("Unable to start transfer.")
        LOGGER.error("Response: %s", resp_json)
        return None, None


def start_transfer(
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
    Starts a new transfer.

    :param ss_url: URL of the Storage Service to query
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
    # Retrieve the next transfer to process.
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
            "All potential transfers in Location ID: %s have been created. " "Exiting",
            ts_location_uuid,
        )
        return None
    LOGGER.info("Starting with %s", target)
    # Get accession ID
    accession = get_accession_id(target)
    LOGGER.info("Accession ID: %s", accession)
    # Call the start transfer endpoint.
    # Retrieve the directory name for Archivematica.
    transfer_name, transfer_abs_path = call_start_transfer_endpoint(
        am_url=am_url,
        am_user=am_user,
        am_api_key=am_api_key,
        target=target,
        transfer_type=transfer_type,
        accession=accession,
        ts_location_uuid=ts_location_uuid,
    )
    if not transfer_name:
        LOGGER.info("Cannot begin transfer with target name: %s", target)
        models.transfer_failed_to_start(target)
        return None
    # Run all pre-transfer scripts on the unapproved transfer directory.
    LOGGER.info("Attempting to run pre-transfer scripts on: %s", transfer_name)
    try:
        run_pre_transfer_scripts(
            config_file=config_file,
            transfer_path=transfer_abs_path,
            transfer_type=transfer_type,
        )
    except OSError as err:
        LOGGER.error("Failed to run pre-transfer scripts: %s", err)
        return None
    # Approve transfer.
    LOGGER.info("Ready to approve transfer")
    retry_count = 3
    for i in range(retry_count):
        result = approve_transfer(transfer_name, am_url, am_api_key, am_user)
        # Mark as started
        if result:
            LOGGER.info("Approved %s", result)
            # Store the absolute path to help users to determine what type
            # the transfer is, and where something it is.
            new_transfer = models.add_new_transfer(uuid=result, path=target)
            LOGGER.info("New transfer: %s", new_transfer)
            break
        LOGGER.info("Failed transfer approval, try %s of %s", i + 1, retry_count)
    else:
        new_transfer = models.failed_to_approve(path=target)
        LOGGER.warning("Transfer not approved: %s", transfer_name)
        return None
    # Start transfer completed successfully.
    LOGGER.info("Finished %s", target)
    return new_transfer


def approve_transfer(dirname, url, am_api_key, am_user):
    """
    Approve transfer with dirname.

    :returns: UUID of the approved transfer or None.
    """
    LOGGER.info("Approving %s", dirname)
    time.sleep(6)
    am = AMClient(am_url=url, am_user_name=am_user, am_api_key=am_api_key)
    try:
        # Find the waiting transfers available to be approved via the am client
        # interface.
        waiting_transfers = am.unapproved_transfers()["results"]
    except (KeyError, TypeError):
        LOGGER.error(
            "Request to unapproved transfers did not return the "
            "expected response, see the request log"
        )
        return None
    if not waiting_transfers:
        LOGGER.warning("There are no waiting transfers.")
        return None
    res = list(
        filter(
            lambda waiting: fsencode(waiting["directory"]) == fsencode(dirname),
            waiting_transfers,
        )
    )
    if not res:
        LOGGER.warning(
            "Requested directory %s not found in the waiting " "transfers list", dirname
        )
        return None
    LOGGER.info("Found waiting transfer: %s", res[0]["directory"])
    # We can reuse the existing AM Client but we didn't know all the kwargs
    # at the outset so we need to set its attributes here.
    am.transfer_type = res[0]["type"]
    am.transfer_directory = dirname
    # Approve the transfer and return the UUID of the transfer approved.
    approved = am.approve_transfer()
    if isinstance(approved, int):
        if errors.error_lookup(approved) is not None:
            LOGGER.error("Error approving transfer: %s", errors.error_lookup(approved))
            return None
    # Get will return None, or the UUID.
    return approved.get("uuid")


def main(
    am_user,
    am_api_key,
    ss_user,
    ss_api_key,
    ts_uuid,
    ts_path,
    depth,
    am_url,
    ss_url,
    transfer_type,
    see_files,
    transfer_delete_path,
    hide_on_complete=False,
    delete_on_complete=False,
    config_file=None,
    log_level="INFO",
):
    """Primary entry point for the automation tools script."""
    loggingconfig.setup(
        log_level, get_setting(config_file, "logfile", defaults.TRANSFER_LOG_FILE)
    )

    LOGGER.info("Automation tools waking up")

    # Check for evidence that this is already running
    default_pidfile = os.path.join(THIS_DIR, "pid.lck")
    pid_file = get_setting(config_file, "pidfile", default_pidfile)
    try:
        # Open PID file only if it doesn't exist for read/write
        f = os.fdopen(os.open(pid_file, os.O_CREAT | os.O_EXCL | os.O_RDWR), "w")
    except OSError:
        LOGGER.error(
            "This script is already running. To override this "
            "behavior and start a new run, remove %s",
            pid_file,
        )
        return 0
    else:
        pid = os.getpid()
        f.write(str(pid))
        f.close()

    # Create a database session to work with.
    create_db_session(config_file)

    # Create the callback to automatically remove pid.lck on script completion.
    setup_automation_execution(pid_file=pid_file)

    # Check status of last unit
    current_unit = None
    try:
        current_unit = models.get_current_unit()
        unit_uuid = current_unit.uuid
        unit_type = current_unit.unit_type
    except NoResultFound:
        # Assume a new run if no result can be found in the database.
        LOGGER.debug("No current unit", exc_info=True)
        unit_uuid = unit_type = ""
        LOGGER.info("Current unit: unknown.  Assuming new run.")
        status = "UNKNOWN"
    else:
        LOGGER.info("Current unit: %s", current_unit)
        # Get status
        status_info = get_status(
            am_url,
            am_user,
            am_api_key,
            ss_url,
            ss_user,
            ss_api_key,
            unit_uuid,
            unit_type,
            transfer_delete_path,
            hide_on_complete,
            delete_on_complete,
            
        )
        LOGGER.info("Status info: %s", status_info)
        if not status_info:
            LOGGER.error("Could not fetch status for %s. Exiting.", unit_uuid)
            return 1
        try:
            status = status_info.get("status")
            models.update_unit_status(current_unit, status)
        except AttributeError as err:
            LOGGER.error(
                "Cannot read response from server for %s: %s", current_unit, err
            )
            return None

    # If processing, exit
    if status == "PROCESSING":
        LOGGER.info("Current transfer still processing, nothing to do.")
        return 0

    # If waiting on input, send email, exit
    elif status == "USER_INPUT":
        LOGGER.info("Waiting on user input, running scripts in user-input directory.")
        microservice = status_info.get("microservice", "")
        run_scripts(
            "user-input",
            config_file,
            microservice,  # Current microservice name
            # String True or False if this is the first time at this prompt
            str(microservice != current_unit.microservice),
            status_info["path"],  # Absolute path
            status_info["uuid"],  # SIP/Transfer UUID
            status_info["name"],  # SIP/Transfer name
            status_info["type"],  # SIP or transfer
        )
        models.update_unit_microservice(current_unit, microservice)
        return 0

    # If failed, rejected, completed etc, start new transfer
    if current_unit:
        models.update_unit_current(current_unit, False)
    new_transfer = start_transfer(
        ss_url,
        ss_user,
        ss_api_key,
        ts_uuid,
        ts_path,
        depth,
        am_url,
        am_user,
        am_api_key,
        transfer_type,
        see_files,
        config_file,
    )
    return 0 if new_transfer else 1


if __name__ == "__main__":
    parser = get_parser(__doc__)
    args = parser.parse_args()

    log_level = loggingconfig.set_log_level(args.log_level, args.quiet, args.verbose)

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
            transfer_delete_path=args.transfer_delete_path,
            hide_on_complete=args.hide,
            delete_on_complete=args.delete_on_complete,
            config_file=args.config_file,
            log_level=log_level,
        )
    )
