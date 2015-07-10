Automation Tools
================

The Automation Tools project is a set of python scripts, that are designed to automate the processing of transfers in an Archivematica pipeline.

Currently, the only automation tool is automate transfers.  It is used to prepare transfers, move them into the pipelines processing location, and take actions when user input is required.  Only one transfer is sent to the pipeline at a time, the scripts wait until the current transfer is resolved (failed, rejected or stored as an AIP) before automatically starting the next available transfer. 

The code is available on [Github](http://github.com/artefactual/automation-tools).

The code is deployed to `/usr/lib/archivematica/automation-tools`.

Deployment
----------

Suggested deployment is to use cron to run a shell script that runs the automate transfer tool. Example shell script:

    #!/bin/bash
    cd /usr/lib/archivematica/automation-tools/transfers/
    /usr/share/python/automation-tools/bin/python transfer.py --user <add user>  --api-key <add api key> --transfer-source <add transfer source location uuid> --depth 2

This script is run through a crontab entry. Example:

    */5 * * * * /etc/archivematica/automation-tools/transfer-script.sh

The cron entry executes the `transfer-script.sh` script. This should be run as the same user as Archivematica is run as (likely the `archivematica` user.)

When running, automate transfers stores its working state in transfers.db, a sqlite database.  It contains a record of all the transfers that have been processed.  In a testing environment, deleting this file will cause the tools to re-process any and all folders found in the Transfer Source Location. 

Configuration
-------------

This script can be modified, to adjust how automate transfers works.  The full set of parameters that can be changed are:

* `-u USERNAME, --user USERNAME` [REQUIRED]: Username of the dashboard user to authenticate as.
* `-k KEY, --api-key KEY` [REQUIRED]: API key of the dashboard user.
* `-t UUID, --transfer-source UUID`: [REQUIRED] Transfer Source Location UUID to fetch transfers from.
* `--transfer-path PATH`: Relative path within the Transfer Source. Default: ""
* `--depth DEPTH, -d DEPTH`: Depth to create the transfers from relative to the transfer source location and path. Default of 1 creates transfers from the children of transfer-path.
* `--am-url URL, -a URL`:Archivematica URL. Default: http://127.0.0.1
* `--ss-url URL, -s URL`: Storage Service URL. Default: http://127.0.0.1:8000
* `--transfer-type TYPE`: Type of transfer to start. One of: 'standard' (default), 'unzipped bag', 'zipped bag', 'dspace'.
* `--files`: If set, start transfers from files as well as folders.
* `--hide`: If set, hides the Transfer and SIP once completed.

Hooks
-----

During processing, automate transfers will run scripts from several places to customize behaviour. These scripts can be in any language. If they are written in Python, we recommend making them source compatible with python 2 or 3.

There are three places hooks can be used to change the automate tools behaviour.

* `transfers/get-accession-number` (script)
* `transfers/pre-transfer` (directory)
* `transfers/user-input` (directory)

Any new scripts added to these directories will automatically be run alongside the existing scripts.

There are also several scripts provided for common use cases and examples of processing that can be done.
These are found in the `examples` directory sorted by their usecase and can be copied or symlinked to the appropriate directory for automation-tools to run them.
If you write a script that might be useful for others, please make a pull request!

### get-accession-id

* _Name:_ `get-accession-id`
* _Location:_ Same directory as transfers.py
* _Parameters:_ [`path`]
* _Return Code:_ 0
* _Output:_ Quoted value of the accession number (e.g. `"ID 42"`)

`get-accession-number` is run to customize the accession number of the created transfer. Its single parameter is the path relative to the transfer source location.  Note that no files are locally available when `get-accession-id` is run. It should print to standard output the quoted value of the accession number (e.g. `"ID42"`), `None`, or no output. If the return code is not 0, all output is ignored. This is POSTed to the Archivematica REST API when the transfer is created.

### pre-transfer hooks

* _Parameters:_ [`absolute path`, `transfer type`]

All executable files found in `pre-transfer` are executed in alphabetical order when a transfer is first copied from the specified Transfer Source Location to the Archivematica pipeline. The return code and output of these scripts is not evaluated.

All scripts are passed the same two parameters:

* `absolute path` is the absolute path on disk of the transfer
* `transfer type` is transfer type, the same as the parameter passed to the script. One of 'standard', 'unzipped bag', 'zipped bag', 'dspace'.

There are some sample scripts in the pre-transfers directory that may be useful, or models for your own scripts.

* `00_file_to_folder.py`: If the transfer is a single file (eg a zipped bag or DSpace transfer), it moves it into an identically named folder. This is not required for processing, but allows other pre-transfer scripts to run.
* `add_metadata.py`: Creates a metadata.json file, by parsing data out of the transfer folder name.  This ends up as Dublin Dore in a dmdSec of the final METS file.
* `archivesspace_ids.py`: Creates an archivesspaceids.csv by parsing ArchivesSpace reference IDs from filenames.  This will automate the matching GUI if a DIP is uploaded to ArchivesSpace.
* `default_config.py`: Copies the included `defaultProcessingMCP.xml` into the transfer directory. This file overrides any configuration set in the Archivematica dashboard, so that user choices are guaranteed and avoided as desired.

### user-input

* _Parameters:_ [`microservice name`, `first time at wait point`, `absolute path` , `unit UUID`, `unit name`, `unit type`]

All executable files in the `user-input folder` are executing in alphabetical order whenever there is a transfer or SIP that is waiting at a user input prompt. The return code and output of these scripts is not evaluated.

All scripts are passed the same set of parameters.

* `microservice name` is the name of the microservice awaiting user input. E.g. Approve Normalization
* `first time at wait point` is the string "True" if this is the first time the script is being run at this wait point, "False" if not. This is useful for only notifying the user once.
* `absolute path` is the absolute path on disk of the transfer
* `unit UUID` is the SIP or transfer's UUID
* `unit name` is the name of the SIP or transfer, not including the UUID.
* `unit type` is either "SIP" or "transfer"

There are some sample scripts in the pre-transfers directory that may be useful, or models for your own scripts.

* `send_email.py`: Emails the first time a transfer is waitintg for input at Approve Normalization.  It can be edited to change the email addresses it sends notices to, or to change the notification message.

Logs
----

Logs are written to the same directory as the `transfers.py` script. The logging level can be adjusted, by modifying the transfers/transfer.py file. Find the following section and changed `'INFO'` to one of `'INFO'`, `'DEBUG'`, `'WARNING'`, `'ERROR'` or `'CRITICAL'`.

    'loggers': {
        'transfer': {
            'level': 'INFO',  # One of INFO, DEBUG, WARNING, ERROR, CRITICAL
            'handlers': ['console', 'file'],
        },
    },

Related Projects
----------------

* [automation-audit](https://github.com/finoradin/automation-audit): an interface for auditing and managing Archivematica's automation-tools.
