#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Archivematica Client Argument Parser

import sys
import argparse
from collections import namedtuple

from six import binary_type, text_type

# AM Client Module Configuration.

# Allow execution as an executable and the script to be run at package level
# by ensuring that it can see itself.
sys.path.append('../')

from transfers import defaults

try:
    from os import fsencode
except ImportError:
    def fsencode(filename):
        """Cribbed & modified from Python3's OS module to support Python2."""
        encoding = sys.getfilesystemencoding()
        if isinstance(filename, binary_type):
            return filename
        elif isinstance(filename, text_type):
            return filename.encode(encoding)
        else:
            raise TypeError("expect bytes or str, not %s" %
                            type(filename).__name__)

# Reusable argument constants (for CLI).
Arg = namedtuple('Arg', ['name', 'help', 'type'])
AIP_UUID = Arg(
    name='aip_uuid',
    help='UUID of the target AIP',
    type=None)
AM_API_KEY = Arg(
    name='am_api_key',
    help='Archivematica API key',
    type=None)
DIP_UUID = Arg(
    name='dip_uuid',
    help='UUID of the target DIP',
    type=None)
SS_API_KEY = Arg(
    name='ss_api_key',
    help='Storage Service API key',
    type=None)
TRANSFER_SOURCE = Arg(
    name='transfer_source',
    help='Transfer source UUID',
    type=None)

# Reusable option constants (for CLI).
Opt = namedtuple('Opt', ['name', 'metavar', 'help', 'default', 'type'])
AM_URL = Opt(
    name='am-url',
    metavar='URL',
    help='Archivematica URL. Default: {0}'.format(defaults.DEF_AM_URL),
    default=defaults.DEF_AM_URL,
    type=None)
AM_USER_NAME = Opt(
    name='am-user-name',
    metavar='USERNAME',
    help='Archivematica username. Default: {0}'.format(defaults.DEF_USER_NAME),
    default=defaults.DEF_USER_NAME,
    type=None)
DIRECTORY = Opt(
    name='directory',
    metavar='DIR',
    help='Directory path to save the DIP in',
    default=None,
    type=None)
OUTPUT_MODE = Opt(
    name='output-mode',
    metavar='MODE',
    help='How to print output, JSON (default) or Python',
    default='json',
    type=None)
SS_URL = Opt(
    name='ss-url',
    metavar='URL',
    help='Storage Service URL. Default: {0}'.format(defaults.DEF_SS_URL),
    default=defaults.DEF_SS_URL,
    type=None)
SS_USER_NAME = Opt(
    name='ss-user-name',
    metavar='USERNAME',
    help='Storage Service username. Default: {0}'.format(
        defaults.DEF_USER_NAME),
    default=defaults.DEF_USER_NAME,
    type=None)
TRANSFER_PATH = Opt(
    name='transfer-path',
    metavar='PATH',
    help='Relative path within the Transfer Source. Default: ""',
    default=b'',
    type=fsencode)


# Sub-command configuration: give them a name, help text, a tuple of ``Arg``
# instances and a tuple of ``Opts`` instances.
SubCommand = namedtuple('SubCommand', ['name', 'help', 'args', 'opts'])
SUBCOMMANDS = (
    SubCommand(
        name='close-completed-transfers',
        help='Close all completed transfers.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='close-completed-ingests',
        help='Close all completed ingests.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='completed-transfers',
        help='Print all completed transfers.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='completed-ingests',
        help='Print all completed ingests.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='unapproved-transfers',
        help='Print all unapproved transfers.',
        args=(AM_API_KEY,),
        opts=(AM_USER_NAME, AM_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='transferables',
        help='Print all transferable entities in the Storage Service.',
        args=(SS_API_KEY, TRANSFER_SOURCE),
        opts=(SS_USER_NAME, SS_URL, TRANSFER_PATH, OUTPUT_MODE)
    ),
    SubCommand(
        name='aips',
        help='Print all AIPs in the Storage Service.',
        args=(SS_API_KEY,),
        opts=(SS_USER_NAME, SS_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='dips',
        help='Print all DIPs in the Storage Service.',
        args=(SS_API_KEY,),
        opts=(SS_USER_NAME, SS_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='aips2dips',
        help='Print all AIPs in the Storage Service along with their \
              corresponding DIPs.',
        args=(SS_API_KEY,),
        opts=(SS_USER_NAME, SS_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='aip2dips',
        help=('Print the AIP with AIP_UUID along with its corresponding '
              'DIP(s).'),
        args=(AIP_UUID, SS_API_KEY),
        opts=(SS_USER_NAME, SS_URL, OUTPUT_MODE)
    ),
    SubCommand(
        name='download-dip',
        help='Download the DIP with DIP_UUID.',
        args=(DIP_UUID, SS_API_KEY),
        opts=(SS_USER_NAME, SS_URL, DIRECTORY, OUTPUT_MODE)
    )
)


def get_parser():
    """Parse arguments according to the ``SUBCOMMANDS`` configuration. Return
    an argparse ``Namespace`` instance representing the parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description='Archivematica Client',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--log-file', metavar='FILE', help='logfile',
        default=defaults.AMCLIENT_LOG_FILE)
    parser.add_argument(
        '--log-level', choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
        default=defaults.DEFAULT_LOG_LEVEL,
        help='Set the debugging output level.')
    subparsers = parser.add_subparsers(help='sub-command help',
                                       dest='subcommand', metavar="<command>")
    for subcommand in SUBCOMMANDS:
        subparser = subparsers.add_parser(subcommand.name,
                                          help=subcommand.help)
        for arg in subcommand.args:
            subparser.add_argument(
                arg.name, help=arg.help, type=arg.type)
        for opt in subcommand.opts:
            subparser.add_argument(
                '--' + opt.name, metavar=opt.metavar, help=opt.help,
                default=opt.default, type=opt.type)
    return parser
