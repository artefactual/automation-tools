Automation Tools

The Automation Tools project is a set of python scripts, that are designed to automate the processing of Transfers in an Archivematica pipeline.  
It is used to prepare transfers, move them into the pipelines processing location, and take actions when user input is required.  Only one transfer is sent to the pipeline at a time, the scripts wait until the current Transfer either fails, is rejected or is stored as an AIP before automatically starting the next available Transfer. 

The code is available from http://github.com/artefactual/automation-tools .

The code is deployed to /usr/lib/archivematica/automation-tools.

It is turned off and on through a crontab entry:

example cron entry:
*/5 * * * * /etc/archivematica/automation-tools/transfer-script.sh

The cron entry executes the transfer-script.sh script.  
This script can be modified, to adjust how the automation tools work.  The full set of parameters that can be changed are:


-u USERNAME --user USERNAME     Username of the dashboard user to authenticate as
-k KEY --api-key KEY            API key of the dashboard user
-t UUID --transfer-source UUID  Transfer Source Location UUID to fetch transfers from
--transfer-path PATH            Relative path within the Transfer Source [default: ]
-d DEPTH --depth DEPTH          Depth to create the transfers from relative to the transfer source location and path. Default creates transfers from the children of transfer-path. [default: 1]
-a URL --am-url URL             Archivematica URL [default: http://127.0.0.1]
-s URL --ss-url URL             Storage Service URL [default: http://127.0.0.1:8000]
--transfer-type TYPE            Type of transfer to start. Unimplemented. [default: standard]
--files                         Start transfers from files as well as folders. Unimplemeted. [default: False]

production version of file:

#!/bin/bash
cd /usr/lib/archivematica/automation-tools/transfers/
/usr/share/python/automation-tools/bin/python transfer.py --user <add user>  --api-key <add api key> --transfer-source <add transfer source location uuid> --depth 2

tree output of /usr/lib/archivematica/automation-tools/
.
├── COPYRIGHT
├── fixtures
│   └── vcr_cassettes
│       ├── get_status_ingest.yaml
│       ├── get_status_not_json.yaml
│       ├── get_status_no_unit.yaml
│       ├── get_status_transfer_to_ingest.yaml
│       └── get_status_transfer.yaml
├── LICENSE
├── requirements
│   ├── base.txt
│   ├── local.txt
│   └── production.txt
├── requirements.txt
├── tests
│   ├── __init__.py
│   └── test_transfers.py
├── TRADEMARK
└── transfers
    ├── get-accession-number
    ├── __init__.py
    ├── models.py
    ├── pre-transfer
    │   ├── add_metadata_netx.py
    │   ├── add_metadata.py
    │   ├── default_config.py
    │   └── defaultProcessingMCP.xml
    ├── transfer.py
    ├── transfers.db
    └── user-input
        └── send_email.py


The transfers.db file is a sqlite database, that contains a record of all the Transfers that have been processed.  In a testing environment, deleting this file will cause the tools to re-process any and all folders found in the Transfer Source Location. 

There are 3 directories, within the automation tools installation, that are checked for scripts.  All scripts found in the transfers/pre-transfer folder are executed when a Transfer is first copied from the specified Transfer Source Location, to the Archivematica pipeline.  Currently, there are 3 python scripts that are run:
-add_metadata.py creates a metadata.json file, by parsing data out of the transfer folder name.  This ends up as dublin core in a dmdSec of the final METS file.
-add_metadata_netx.py creates a next.json file, that is put in the transfers metadata folder, and used for copying dips to Netx.
-default_config.py copies the included defaultProcessingMCP.xml into the Transfer.  This file overrides any configuration set in the Archivematica dashboard, so that user choices are guaranteed and avoided as desired.

Scripts in the transfers/ folder are executed next.  Currently there is only one:
get-accession-number which finds the TMS Object ID from the Transfer folder name. This number is POSTed to the Archivematica REST API when the Transfer is created.

Scripts in the user-input folder are run whenever there is a transfer or sip that is waiting at a user input prompt.
-send_email.py can be edited to change the email addresses it sends notices to, or to change the notification message.
This script is configured to only send emails the first time that the automation tools become aware that a Transfer is waiting for user input.  It also ignores any user inputs other than 'Approve Normalization'.

Any new scripts added to these directories will automatically be run alongside the existing scripts.

Logs are written to /var/log/archivematica/ in production, and into /usr/lib/archivematica/automation-tools in dev.
The logging level can be adjusted, by modifying the transfers/transfer.py file, there is a section like this that can be modified:

 'loggers': {
        'transfer': {
            'level': 'DEBUG',  # One of INFO, DEBUG, WARNING, ERROR, CRITICAL
