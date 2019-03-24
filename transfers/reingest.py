#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Reingest AIPs Automatically.

Created for CCArch to reingest compressed AIPs using an alternative
Archivematica ProcessingMCP.xml file.

A work in progress, with some improvements that can be made to long-running
processes like this over time.
"""
from __future__ import print_function

import argparse
import atexit
import json
import logging
import os
import time
import sys

from amclient import AMClient
from six import string_types, text_type

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transfers import errors, loggingconfig
from transfers import reingestmodel as reingestunit

LOGGER = logging.getLogger('transfers')

# Used early in the script to indicate failure with non-zero status, i.e.
# something went wrong.
ERR_PROCESSING = 1

# A sleep value to realistically allow for reingest to start and approval of
# reingest to happen.
LATENCY = 0.8

# If the process is running already we don't want to atexit to execute with
# its default registered behavior. Override here.
OVERRIDE_ATEXIT = False


def setup_reingest(config):
    """Setup the script for reingest."""
    with open(config) as json_data_file:
        data = json.load(json_data_file)
        # The function atexit will ensure the PID is removed if one is created.
        # We need to make sure that config is loaded first for this to happen.
        atexit.register(manage_process, data, remove=True)
        return data


def setup_amclient(amclient):
    """Setup the AM Client Class.

    Setup the amclient class that we're going to use. We set the attributes
    here to avoid a Python AttributeError when using dot syntax.
    We avoid having to use `setattr(...)` in the individual functions by doing
    this, thus enabling dot-notation when desired. Once amclient has been
    refactored so that Issue #71 is addressed we should be able to delete
    this block. The `setattr(...)` calls in this block are ordered
    alphabetically.
    """
    setattr(amclient, "aip_uuid", None)
    setattr(amclient, "package_uuid", None)
    setattr(amclient, "pipeline_uuid", None)
    setattr(amclient, "processing_config", None)
    setattr(amclient, "sip_uuid", None)
    setattr(amclient, "transfer_directory", None)
    setattr(amclient, "transfer_uuid", None)
    return amclient


def get_am_client(config):
    """Return an AM Client Object to work with throughout the rest
    of the script.
    """
    connection = config['connection']
    amclient = AMClient(
        ss_url=connection['ss_url'],
        ss_user_name=connection['ss_user_name'],
        ss_api_key=connection['ss_api_key'],
        am_url=connection['am_url'],
        am_user_name=connection['am_user_name'],
        am_api_key=connection['am_api_key'],
        output_mode=connection['output_mode'])
    return setup_amclient(amclient)


def pipeline_exists(amclient, pipeline_uuid):
    """Test whether a pipeline is known to the storage service."""
    try:
        pipelines = amclient.get_pipelines()["objects"]
        return next((pipeline for pipeline in pipelines
                     if pipeline["uuid"] == pipeline_uuid), False)
    except KeyError:
        return False


def processing_exists(amclient, processing_name):
    """Check to see if a processing configuration exists inside Archivematica
    so that it cab be called and used.
    """
    amclient.processing_config = processing_name
    resp = amclient.get_processing_config()
    if errors.error_lookup(resp) is resp:
        return True
    return False


def reingest_full_and_approve(amclient, pipeline_uuid, aip_uuid,
                              processing_config="default", latency=None,
                              approval_retries=2):
    """Reingest an archivematica AIP.

    This function will make three calls to the AM Client code:

        1) To initiate the reingest.
        2) To monitor the transfer status.
        3) To approve it in the transfer workflow.

    A latency argument makes up for the endpoint being neither particularly
    synchronous or asynchronous. We don't want to mindlessly poll
    the server to check that the AIP we want to reingest is in the pipeline.
    """

    # Initialize an AIPs reingest.
    amclient.pipeline_uuid = pipeline_uuid
    amclient.aip_uuid = aip_uuid
    amclient.processing_config = processing_config
    reingest_aip = amclient.reingest_aip()

    # If we successfully initialize the reingest we get a dict back that
    # we can work with to monitor state. If we do not get a dict back we need
    # to look at what went wrong.
    if not isinstance(reingest_aip, dict):
        LOGGER.error("Reingest failed with server error %s, returning.",
                     errors.error_lookup(reingest_aip))
        return False, "Error calling reingest_aip."

    reingest_uuid = reingest_aip["reingest_uuid"]
    LOGGER.info("Reingest UUID to work with %s", reingest_uuid)

    LOGGER.info("Checking status of %s once in transfer queue", reingest_uuid)

    # We need to make sure that the approval is synchronized with the request
    # to reingest. Occasionally a reingest will get stopped at approval when
    # it doesn't need to if we just wait a little longer or try again.
    for _ in range(approval_retries):
        transfer = None
        while not isinstance(transfer, dict):
            if latency:
                time.sleep(latency)   # ~latency between call and AM actioning.
            amclient.transfer_uuid = reingest_uuid
            transfer = amclient.get_transfer_status()

        LOGGER.info("Attempting to approve transfer following the "
                    "initialization of reingest.")

        if transfer.get("status") == "USER_INPUT":
            transfer_directory = transfer["directory"]
            LOGGER.info("Approving reingest automatically. Directory to "
                        "approve %s", transfer_directory)
            amclient.transfer_directory = transfer_directory
            message = amclient.approve_transfer()
            if message.get('error') is None:
                LOGGER.info("Approval successful, returning True")
                return True, message['uuid']
            return False, "Error approving transfer."

    return False, "Error retrieving transfer status."


def manage_process(config_file, remove=False):
    """Manage the reingest process using a process lock.

    If the PID.lck file exists then inform the user. We shouldn't try running
    this script if it is already running elsewhere. Manage_process is the
    default atexit behavior of this script so it will remove the process lock
    if it is part of regular script execution. If the script is already running
    then the default behavior is overridden and the script is allowed to exit
    without removing the LCK file.
    """
    pid_file = config_file['process']['pid']
    global OVERRIDE_ATEXIT
    if OVERRIDE_ATEXIT:
        return
    if remove and os.path.isfile(pid_file):
        LOGGER.info("Removing PID for current process.")
        os.remove(pid_file)
        return
    try:
        pidf = os.fdopen(
            os.open(pid_file, os.O_CREAT | os.O_EXCL | os.O_RDWR), 'w')
    except OSError:
        LOGGER.info('This script is already running. To override this '
                    'behavior and start a new run, remove %s', pid_file)
        # By default, do not remove the PID if the process is already running.
        OVERRIDE_ATEXIT = True
        sys.exit()
    pid = os.getpid()
    pidf.write(str(pid))
    pidf.close()


def db_has_aips(session):
    """Test whether the database has any existing content to be processed."""
    return bool(reingestunit.get_items(session))


def load_db(session, aiplist):
    """If we have AIPs to reingest, load the database."""
    if isinstance(aiplist, (string_types, text_type)):
        return False
    try:
        for aip in aiplist:
            reingestunit.insert_aip_row_for_reingest(session, aip)
        return True
    except TypeError:
        LOGGER.error("AIP list to load is not properly formed.")
        return False


def loadfromlist(listfile):
    """Load AIPs from a user provided file containing an array of AIP UUIDs."""
    userlist = None
    try:
        with open(listfile) as aip_list:
            # A rudimentary attempt at postel's law to allow for invalid JSON
            # if a user's array is written using incorrect quotation marks.
            userlist = json.loads(aip_list.read().replace("'", '"'))
            return userlist
    except IOError as err:
        LOGGER.error("Check existence of file: %s", err)
        sys.exit(ERR_PROCESSING)


def get_status(status):
    """Get status from a python dict.

    Archivematica will return a dict with a 'status' field for a number of
    processing queries when looking to see where a Transfer, or SIP, or AIP is
    in the workflow.
    """
    try:
        return status['status']
    except (KeyError, TypeError):
        return None


def update_reingest(session, amclient):
    """Set the status of the AIP to COMPLETE if the transfer and ingest process
    has completed.
    """
    for aip in reingestunit.get_items_in_progress(session):
        transfer_uuid = aip.transfer_uuid
        aip_uuid = aip.aip_uuid

        # A delta can be produced if we look at transfer status, ingest status,
        # and the package details. If transfer is complete, and ingest is
        # complete (and the SIP uuid can be found) and then the package is
        # described as being uploaded, then we have reingested the AIP and we
        # can set our process state to STATUS_COMPLETE.
        amclient.transfer_uuid = transfer_uuid
        amclient.sip_uuid = aip_uuid
        amclient.package_uuid = aip_uuid
        transfer_status = get_status(amclient.get_transfer_status())
        ingest_status = get_status(amclient.get_ingest_status())
        aip_status = get_status(amclient.get_package_details())
        if transfer_status == "COMPLETE" and ingest_status == "PROCESSING":
            LOGGER.info("AIP %s processing is now in ingest", aip_uuid)
        elif ingest_status == "COMPLETE" and aip_status == "UPLOADED":
            reingestunit.set_status_complete(session, aip.aip_uuid)


def start_reingest(session, amclient, pipeline_uuid, processing_config,
                   throttle, approval_retries=2):
    """Begin the reingest of an AIP.

    start_reingest is called after update_reingest where update_reingest is
    used to ensure that there is no existing work being done by this script
    that would mean using more resource on the pipeline than is configured.

    For example, if one reingest has completed, and we have one AIP still to
    process, update_reingest will make sure the first reingest is marked as
    COMPLETE, and start_reingest can be used to begin the next.

    The update -> start (flush -> begin) approach to running this script is
    useful when automating the reingest process where this script is called
    repeatedly via a cronjob.
    """
    new_aips = reingestunit.get_items_new(session)
    in_progress = reingestunit.get_items_in_progress(session)
    if not new_aips and not in_progress:
        # Return early, reingest complete
        return True
    pool = throttle - len(in_progress)
    if pool < 1:
        LOGGER.info("Pool is less than one, exiting, until next run")
        return False
    for index in range(min(pool, len(new_aips))):
        aip = new_aips[index].aip_uuid
        error, message = reingest_full_and_approve(
            amclient, pipeline_uuid, aip, processing_config, latency=LATENCY,
            approval_retries=approval_retries)
        if error is not False:
            reingestunit.set_status_in_progress(session, aip,
                                                transfer_uuid=message)
        else:
            LOGGER.error("Error initiating reingest %s, %s", aip, message)
            reingestunit.set_status_error(session, aip, message)
    return False


def get_completion_stats(session, all_items=False):
    """Output the database state.

    At the completion of bulk reingest it is a good idea to return a log of
    all the items that have been proccessed. We can do this here to stdout. The
    function also takes a parameter of 'all_items' if a user requests to see
    the state of the database at any point in time. The database model will
    output a list of AIPs and their current status, e.g. NEW, IN_PROGRESS, or
    COMPLETE. If the AIP has been reingested an approximate processing time
    will also be output.
    """
    if all_items:
        all_aips = reingestunit.get_items(session)
        for aip in all_aips:
            print(aip)
        return

    complete = reingestunit.get_items_complete(session)
    error = reingestunit.get_items_error(session)
    for comp in complete:
        print(comp)
    for err in error:
        print(err)


def main():
    """Primary entry point for the reingest script.

    Roughly speaking the following operations happen:
        * 1. Generate list of compressed AIPs from the system
        * 2. Optionally if we have a user generated list, compare,
        * 3. Exit if necessary
        * 4. If we progress we need the processing config to reingest with
        * 5. And we need to maintain state as we go, so setup a db, and store
             our AIP list
        * 6. From there, check the progress of the reingest, update if
             necessary, and then in-turn start and approve each subsequent
             AIPs reingest.
        * 7. On completion, output a log of results.
    """

    # Setup command line arguments.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=str,
                        help='REQUIRED: configure the script', required=True)
    parser.add_argument('--listcompressedaips', action='store_true',
                        help='list compressed AIPs in storage')
    parser.add_argument('--compareaiplist', type=str,
                        help='compare stored compressed aips with an existing'
                             ' list')
    parser.add_argument('--processfromlist', type=str,
                        help='reingest from a list of UUIDs')
    parser.add_argument('--processfromstorage', action="store_true",
                        help='reingest compressed AIPs from the '
                             'Storage Service')
    parser.add_argument('--dbstatus', action="store_true",
                        help='output log from the database')
    parser.add_argument('--logging', type=str, nargs="?",
                        help='logging level, INFO, DEBUG, WARNING, ERROR')

    if not len(sys.argv) > 2:
        parser.print_help()
        parser.exit()

    args = parser.parse_args()

    # Retrieve configuration details from our JSON file.
    config = setup_reingest(args.config)

    logging_path = config['logging']['path']
    logging_default = config['logging']['default'].upper()
    if args.logging is None or args.logging not in ["INFO",
                                                    "DEBUG",
                                                    "WARNING",
                                                    "ERROR"]:

        loggingconfig.setup(logging_default, logging_path)
    else:
        loggingconfig.setup(args.logging, logging_path)

    # Create an AM Client instance to work with.
    amclient = get_am_client(config)

    # Perform some early checks to make sure this process will work. Check now,
    # exit early.
    pipeline_uuid = config['reingest']['pipeline']

    if not pipeline_exists(amclient, pipeline_uuid):
        LOGGER.error("Pipeline does not exist to reingest on. Exiting.")
        sys.exit(ERR_PROCESSING)

    processing_config = config['reingest']['processing_config']
    if not processing_exists(amclient, processing_config):
        LOGGER.error("Processing config does not exist to reingest with. "
                     "Exiting.")
        sys.exit(ERR_PROCESSING)

    # Throttle  and approval retries are useful pieces of information to
    # grab early.
    throttle = config['reingest']['throttle']
    approval_retries = config['reingest']['approval_retries']
    LOGGER.info("Processing throttle set to %s, "
                "approval retries set to %s", throttle, approval_retries)

    # Once our preliminary checks are out of the way we can start to probe
    # the Storage Service a little more for matching AIPs.
    if args.listcompressedaips:
        aips = amclient.get_all_compressed_aips()
        LOGGER.info("%s Compressed AIPs in the Storage Service", len(aips.keys()))
        LOGGER.debug("Compressed AIPs list: %s", aips.keys())
        sys.exit()

    if args.compareaiplist:
        comparelist = loadfromlist(args.compareaiplist)
        if comparelist:
            aips = amclient.get_all_compressed_aips()
            list1 = comparelist  # user-list
            list2 = aips.keys()
            if set(list1) == set(list2):
                LOGGER.info("Both lists of compressed AIPs are identical. "
                            "Recommendation is to proceed with reingest")
            else:
                print("Difference in user set: %s",
                      list(set(list1) - set(list2)))
                print("Difference in Storage Service set: %s",
                      list(set(list2) - set(list1)))
        sys.exit()

    # At this stage we're going to start processing, so setup a PID to
    # work with.
    manage_process(config)
    dbpath = config['database']['path']
    reingestunit.init(dbpath)
    session = reingestunit.Session()

    # To generate a log of everything in the database we need to get hold of
    # a database session here.
    if args.dbstatus:
        if os.path.exists(dbpath):
            get_completion_stats(session, all_items=True)
            sys.exit()
    load_items = db_has_aips(session)
    if load_items:
        LOGGER.info("Database already contains AIPs, ignoring LOAD")
    elif not load_items and args.processfromlist:
        LOGGER.info("Reingesting from user list of AIPs")
        aiplist = loadfromlist(args.processfromlist)
        if not load_db(session, aiplist):
            sys.exit(ERR_PROCESSING)
    elif not load_items and args.processfromstorage:
        LOGGER.info("Reingesting from Storage Service list of AIPs")
        aips = amclient.get_all_compressed_aips()
        if not load_db(session, list(aips.keys())):
            sys.exit(ERR_PROCESSING)

    # Check for existing transfers in the pipeline matching our AIPs and update
    # their status. This will free up our ability to start new reingests. Even
    # if there are zero in the pipeline we can call it fairly inexpensively
    # here first so that start_reingest doesn't have to be called within
    # itself.
    update_reingest(session=session, amclient=amclient)

    # Start as many ingests from the pool as we can per throttle.
    complete = start_reingest(session=session, amclient=amclient,
                              pipeline_uuid=pipeline_uuid,
                              processing_config=processing_config,
                              throttle=throttle,
                              approval_retries=approval_retries)

    # If there are no new AIPs and none in progress, then complete this work
    # by outputting some information about the process.
    if complete:
        get_completion_stats(session=session)


if __name__ == '__main__':
    main()
