# -*- coding: utf-8 -*-

# Defaults and constants for amclient.py and transfers.py

import os
import re

DEF_AM_URL = 'http://127.0.0.1:62080'
DEF_SS_URL = 'http://127.0.0.1:62081'
DEF_USER_NAME = 'test'

UUID_PATT = re.compile(
    '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')

UNDECODABLE = 'UNABLE TO DECODE'
UNDEC_MSG = ('Unable to decode a transfer source component; giving up and'
             ' returning {0}'.format(UNDECODABLE))

# Default logging for thee module.
THIS_DIR = os.path.abspath(os.path.dirname(__file__))

# Global for logfile if not set.
TRANSFER_LOG_FILE = os.path.join(THIS_DIR, 'automate-transfer.log')

# Default log level.
DEFAULT_LOG_LEVEL = "INFO"

# Default Processing Configuration
DEFAULT_PROCESSING_CONFIG = "default"

# Default reingest type
DEFAULT_REINGEST_TYPE = "full"

# Default transfer type
DEFAULT_TRANSFER_TYPE = "standard"
