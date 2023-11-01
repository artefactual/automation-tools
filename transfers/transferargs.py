"""Command-line argument parser for automated transfer scripts."""
import argparse
from os import fsencode

from transfers.defaults import DEF_AM_URL
from transfers.defaults import DEF_SS_URL


def get_parser(doc):
    """Parser comand-line arguments for automated transfer scripts."""
    # Variable for conformance to flake8 line lenght below.
    rawformatter = argparse.RawDescriptionHelpFormatter

    parser = argparse.ArgumentParser(description=doc, formatter_class=rawformatter)
    parser.add_argument(
        "-u",
        "--user",
        metavar="USERNAME",
        required=True,
        help="Username of the Archivematica dashboard user " "to authenticate as.",
    )
    parser.add_argument(
        "-k",
        "--api-key",
        metavar="KEY",
        required=True,
        help="API key of the Archivematica " "dashboard user.",
    )
    parser.add_argument(
        "--ss-user",
        metavar="USERNAME",
        required=True,
        help="Username of the Storage Service user to " "authenticate as.",
    )
    parser.add_argument(
        "--ss-api-key",
        metavar="KEY",
        required=True,
        help="API key of the Storage Service " "user.",
    )
    parser.add_argument(
        "-t",
        "--transfer-source",
        metavar="UUID",
        required=True,
        help="Transfer Source Location UUID to fetch transfers from.",
    )
    parser.add_argument(
        # default=b'' to convert to bytes from unicode str provided by
        # command line.
        "--transfer-path",
        metavar="PATH",
        help="Relative path within the " 'Transfer Source. Default: ""',
        type=fsencode,
        default=b"",
    )
    parser.add_argument(
        "--depth",
        "-d",
        help="Depth to create the transfers from relative "
        "to the transfer source location and path. "
        "Default of 1 creates transfers from the "
        "children of transfer-path.",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--am-url",
        "-a",
        metavar="URL",
        help="Archivematica URL. Default: %s" % DEF_AM_URL,
        default="%s" % DEF_AM_URL,
    )
    parser.add_argument(
        "--ss-url",
        "-s",
        metavar="URL",
        help="Storage Service URL. Default: %s" % DEF_SS_URL,
        default="%s" % DEF_SS_URL,
    )
    parser.add_argument(
        "--transfer-type",
        metavar="TYPE",
        help="Type of transfer to start. "
        "One of: 'standard' "
        "(default), 'unzipped bag', "
        "'zipped bag', 'dspace'.",
        default="standard",
        choices=["standard", "unzipped bag", "zipped bag", "dspace"],
    )
    parser.add_argument(
        "--files",
        action="store_true",
        help="If set, start transfers from files as well as " "folders.",
    )
    parser.add_argument(
        "--hide",
        action="store_true",
        help="If set, hide the Transfers and SIPs in the "
        "dashboard once they complete.",
    )
    parser.add_argument(
        "--delete-on-complete",
        action="store_true",
        help="If set, delete transfer source files after "
        "ingest successfully completes.",
    )
    
    parser.add_argument(
        "--transfer_delete_path",
        metavar="PATH",
        help="Plain text path to Transfer Source",
        type=str,
        default=None,
    )
    parser.add_argument(
        "-c",
        "--config-file",
        metavar="FILE",
        help="Configuration file(log/db/PID files)",
        default=None,
    )

    # Logging
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
        help="Set the debugging output level. This will " "override -q and -v",
    )

    return parser
