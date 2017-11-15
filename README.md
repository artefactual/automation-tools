[![Travis CI](https://travis-ci.org/artefactual/automation-tools.svg?branch=master)](https://travis-ci.org/artefactual/automation-tools)

Automation Tools
================

The Automation Tools project is a set of python scripts, that are designed to automate the processing of transfers in an Archivematica pipeline.

<!-- doctoc: https://www.npmjs.com/package/doctoc -->
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Requirements](#requirements)
- [Installation](#installation)
- [Automated transfers](#automated-transfers)
  - [Configuration](#configuration)
    - [Parameters](#parameters)
    - [Getting Correct UUIDs and Setting Processing Rules](#getting-correct-uuids-and-setting-processing-rules)
    - [Getting API keys](#getting-api-keys)
  - [Hooks](#hooks)
    - [get-accession-id](#get-accession-id)
    - [pre-transfer hooks](#pre-transfer-hooks)
    - [user-input](#user-input)
  - [Logs](#logs)
  - [Multiple automated transfer instances](#multiple-automated-transfer-instances)
- [DIP creation](#dip-creation)
  - [Configuration](#configuration-1)
    - [Parameters](#parameters-1)
    - [Getting Storage Service API key](#getting-storage-service-api-key)
- [DIP upload to AtoM](#dip-upload-to-atom)
  - [Configuration](#configuration-2)
    - [Parameters](#parameters-2)
- [Archivematica Client](#archivematica-client)
  - [Subcommands and arguments](#subcommands-and-arguments)
- [Related Projects](#related-projects)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

Requirements
------------

* 7z (only for DIP creation script)

Installation
------------

* Checkout or link the code in this repo to `/usr/lib/archivematica/automation-tools`
* Create virtualenv `/usr/share/python/automation-tools` and pip install requirements there
* Create directories `/var/log/archivematica/automation-tools` and `/var/archivematica/automation-tools` owned by user `archivematica`, for log/database/PID files.
* Create directory `/etc/archivematica/automation-tools` and add configuration files there. Files in the `etc/` directory of this repository can be used as an example (also see below for more about configuration)

Automated transfers
-------------------

`transfers/transfer.py` is used to prepare transfers, move them into the pipelines processing location, and take actions when user input is required.
Only one transfer is sent to the pipeline at a time, the scripts wait until the current transfer is resolved (failed, rejected or stored as an AIP) before automatically starting the next available transfer.

### Configuration

Suggested deployment is to use cron to run a shell script that runs the automate transfer tool. Example shell script (for example in `/etc/archivematica/automation-tools/transfer-script.sh`):

```
#!/bin/bash
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m transfers.transfer --user <user> --api-key <apikey> --ss-user <user> --ss-api-key <apikey> --transfer-source <transfer_source_uuid> --config-file <config_file>
```

(Note that the script calls the transfers script as a module using python's `-m` flag, this is required due to the use of relative imports in the code)

The script can be run from a shell window like:

```
user@host:/etc/archivematica/automation-tools$ sudo -u archivematica ./transfer-script.sh
```

It is suggested to run the script through a crontab entry for user archivematica (to avoid the need to repeatedly invoke it manually):

```
*/5 * * * * /etc/archivematica/automation-tools/transfer-script.sh
```

When running, automated transfers stores its working state in a sqlite database.  It contains a record of all the transfers that have been processed.  In a testing environment, deleting this file will cause the tools to re-process any and all folders found in the Transfer Source Location.

#### Parameters

The `transfers.py` script can be modified to adjust how automated transfers work.  The full set of parameters that can be changed are:

* `-u USERNAME, --user USERNAME` [REQUIRED]: Username of the Archivematica dashboard user to authenticate as.
* `-k KEY, --api-key KEY` [REQUIRED]: API key of the Archivematica dashboard user.
* `--ss-user USERNAME` [REQUIRED]: Username of the Storage Service user to authenticate as. Storage Service 0.8 and up requires this; earlier versions will ignore any value provided.
* `--ss-api-key KEY` [REQUIRED]: API key of the Storage Service user. Storage Service 0.8 and up requires this; earlier versions will ignore any value provided.
* `-t UUID, --transfer-source UUID`: [REQUIRED] Transfer Source Location UUID to fetch transfers from. Check the next section for more details on this field.
* `--transfer-path PATH`: Relative path within the Transfer Source. Default: ""
* `--depth DEPTH, -d DEPTH`: Depth to create the transfers from relative to the transfer source location and path. Default of 1 creates transfers from the children of transfer-path.
* `--am-url URL, -a URL`:Archivematica URL. Default: http://127.0.0.1
* `--ss-url URL, -s URL`: Storage Service URL. Default: http://127.0.0.1:8000
* `--transfer-type TYPE`: Type of transfer to start. One of: 'standard' (default), 'unzipped bag', 'zipped bag', 'dspace'.
* `--files`: If set, start transfers from files as well as folders.
* `--hide`: If set, hides the Transfer and SIP once completed.
* `-c FILE, --config-file FILE`: config file containing file paths for log/database/PID files. Default: log/database/PID files stored in the same directory as the script (not recommended for production)
* `-v, --verbose`: Increase the debugging output. Can be specified multiple times, e.g. `-vv`
* `-q, --quiet`: Decrease the debugging output. Can be specified multiple times, e.g. `-qq`
* `--log-level`: Set the level for debugging output. One of: 'ERROR', 'WARNING', 'INFO', 'DEBUG'. This will override `-q` and `-v`

#### Getting Correct UUIDs and Setting Processing Rules

The easiest way to configure the tasks that automation-tools will run is by using the dashboard:

1. Go to Administration > Processing Configuration and choose the options you wish to use.

2. Save the configuration on the form.

3. Copy the processing configuration file from `/var/archivematica/sharedDirectory/sharedMicroServiceTasksConfigs/processingMCPConfigs/defaultProcessingMCP.xml` on the Archivematica host machine to the `transfers/` directory of your automation-tools installation location.

The automation-tools command-line also relies on installation-specific UUIDs. To obtain the transfer source UUID for script invocation, visit the 'Transfer Source' tab in the Archivematica Storage Space web dashboard. If a row is marked as a transfer souce its UUID value will be valid as a transfer source argument.

#### Getting API keys

To get the Archivematica API key, log in to Archivematica as the user you wish to authenticate as.
From the dashboard, click your username in the top right corner, then select 'Your profile'.
The API key will be displayed at the bottom of the page.

To get the Storage Service API key, log in to the Storage Service as the user you wish to authenticate as.
From the dashboard, go to Administration > Users and select 'Edit' for the user you want the key for.
The API key will be displayed at the bottom of the page.
Storage Service versions earlier than 0.8.x do not require an API key, and will not provide one.
In that case, fill in `--ss-user` and `--ss-api-key` with stub data, since those parameters are required by automated transfers.

### Hooks

During processing, automate transfers will run scripts from several places to customize behaviour. These scripts can be in any language. If they are written in Python, we recommend making them source compatible with python 2 or 3.

There are three places hooks can be used to change the automate tools behaviour.

* `transfers/get-accession-number` (script)
* `transfers/pre-transfer` (directory)
* `transfers/user-input` (directory)

Any new scripts added to these directories will automatically be run alongside the existing scripts.

There are also several scripts provided for common use cases and examples of processing that can be done.
These are found in the `examples` directory sorted by their usecase and can be copied or symlinked to the appropriate directory for automation-tools to run them.
If you write a script that might be useful for others, please make a pull request!

#### get-accession-id

* _Name:_ `get-accession-id`
* _Location:_ Same directory as transfers.py
* _Parameters:_ [`path`]
* _Return Code:_ 0
* _Output:_ Quoted value of the accession number (e.g. `"ID 42"`)

`get-accession-number` is run to customize the accession number of the created transfer. Its single parameter is the path relative to the transfer source location.  Note that no files are locally available when `get-accession-id` is run. It should print to standard output the quoted value of the accession number (e.g. `"ID42"`), `None`, or no output. If the return code is not 0, all output is ignored. This is POSTed to the Archivematica REST API when the transfer is created.

#### pre-transfer hooks

* _Parameters:_ [`absolute path`, `transfer type`]

All executable files found in `pre-transfer` are executed in alphabetical order when a transfer is first copied from the specified Transfer Source Location to the Archivematica pipeline. The return code and output of these scripts is not evaluated.

All scripts are passed the same two parameters:

* `absolute path` is the absolute path on disk of the transfer
* `transfer type` is transfer type, the same as the parameter passed to the script. One of 'standard', 'unzipped bag', 'zipped bag', 'dspace'.

There are some sample scripts in the pre-transfers directory that may be useful, or models for your own scripts.

* `00_file_to_folder.py`: If the transfer is a single file (eg a zipped bag or DSpace transfer), it moves it into an identically named folder. This is not required for processing, but allows other pre-transfer scripts to run.
* `00_unbag.py`: Repackages a bag as a standard transfer, writing md5 hashes from bag manifest into metadata/checksum.md5 file. This enables use of scripts such as add_metadata.py with bags, which would otherwise cause failure at the bag validation job.
* `add_metadata.py`: Creates a metadata.json file, by parsing data out of the transfer folder name.  This ends up as Dublin Dore in a dmdSec of the final METS file.
* `archivesspace_ids.py`: Creates an archivesspaceids.csv by parsing ArchivesSpace reference IDs from filenames.  This will automate the matching GUI if a DIP is uploaded to ArchivesSpace.
* `default_config.py`: Copies the included `defaultProcessingMCP.xml` into the transfer directory. This file overrides any configuration set in the Archivematica dashboard, so that user choices are guaranteed and avoided as desired.

#### user-input

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

* `send_email.py`: Emails the first time a transfer is waiting for input at Approve Normalization.  It can be edited to change the email addresses it sends notices to, or to change the notification message.

### Logs

Logs are written to a directory specified in the config file (or `/var/log/archivematica/automation-tools/` by default). The logging level can be adjusted, by modifying the transfers/transfer.py file. Find the following section and changed `'INFO'` to one of `'INFO'`, `'DEBUG'`, `'WARNING'`, `'ERROR'` or `'CRITICAL'`.

    'loggers': {
        'transfer': {
            'level': 'INFO',  # One of INFO, DEBUG, WARNING, ERROR, CRITICAL
            'handlers': ['console', 'file'],
        },
    },

### Multiple automated transfer instances

You may need to set up multiple automated transfer instances, for example if required to ingest both standard transfers and bags. In cases where hooks are the same for both instances, it could be achieved by setting up different scripts, each one invoking the transfers.py script with the required parameters. Example:

```
# first script invokes like this (standard transfer):
/usr/share/python/automation-tools/bin/python -m transfers.transfer --user <user>  --api-key <apikey> --ss-user <user> --ss-api-key <apikey> --transfer-source <transfer_source_uuid_for_std_xfers> --config-file <config_file>

# second script invokes like this (unzipped bags):
/usr/share/python/automation-tools/bin/python -m transfers.transfer --user <user>  --api-key <apikey> --ss-user <user> --ss-api-key <apikey> --transfer-source <transfer_source_2_uuid_for_bags> --config-file <config_file_2> --transfer-type 'unzipped bag'
```

`<config_file_1>` and `<config_file_2>` should specify different file names for db/PID/log files. See transfers.conf and transfers-2.conf in etc/ for an example

In case different hooks are required for each instance, a possible approach is to checkout a new instance of the automation tools, for example in `/usr/lib/archivematica/automation-tools-2`

DIP creation
------------

`aips/create_dip.py` can be used to make a DIP from an AIP available in an Storage Service instance. Unlike DIPs created in Archivematica, the ones created with this script will include only the original files from the transfer and they will maintain the directories, filenames and last modified date from those files. They will be placed in a single ZIP file under the objects directory which will also include a copy of the submissionDocumentation folder (if present in the AIP) and the AIP METS file. Another METS file will be generated alongside the objects folder containing only a reference to the ZIP file (without AMD or DMD sections).

Although this script is part of the automation-tools it's not completely automated yet, so it needs to be executed once per AIP and it requires the AIP UUID. It also requires 7z installed and available to extract the AIPs downloaded from the Storage Service.

### Configuration

Suggested use of this script is by using the example shell script in the `etc` directory (`/etc/archivematica/automation-tools/create_dip_script.sh`):

```
#!/bin/bash
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m aips.create_dip \
  --ss-user <username> \
  --ss-api-key <api_key> \
  --aip-uuid <uuid> \
  --tmp-dir <path> \
  --output-dir <path> \
  --log-file <path>
```

(Note that the script calls the DIP creation script as a module using python's `-m` flag, this is required due to the use of relative imports in the code)

The script can be run from a shell window like:

```
user@host:/etc/archivematica/automation-tools$ sudo -u archivematica ./create_dip_script.sh
```

#### Parameters

The `aips/create_dip.py` accepts the following parameters:

* `--ss-url URL, -s URL`: Storage Service URL. Default: http://127.0.0.1:8000
* `--ss-user USERNAME` [REQUIRED]: Username of the Storage Service user to authenticate as. Storage Service 0.8 and up requires this; earlier versions will ignore any value provided.
* `--ss-api-key KEY` [REQUIRED]: API key of the Storage Service user. Storage Service 0.8 and up requires this; earlier versions will ignore any value provided.
* `--aip-uuid UUID` [REQUIRED]: AIP UUID in the Storage Service to create the DIP from.
* `--tmp-dir PATH`: Absolute path to a directory where the AIP will be downloaded and extracted. Default: "/tmp"
* `--output-dir PATH`: Absolute path to a directory where the DIP will be created. Default: "/tmp"
* `--log-file PATH`: Absolute path to a file to output the logs. Otherwise it will be created in the script directory.
* `-v, --verbose`: Increase the debugging output. Can be specified multiple times, e.g. `-vv`
* `-q, --quiet`: Decrease the debugging output. Can be specified multiple times, e.g. `-qq`
* `--log-level`: Set the level for debugging output. One of: 'ERROR', 'WARNING', 'INFO', 'DEBUG'. This will override `-q` and `-v`

#### Getting Storage Service API key

See [Getting API keys](#getting-api-keys)

DIP upload to AtoM
------------------

`dips/atom_upload.py` is available to upload a DIP folder from the local filesystem to an external AtoM instance. It requires a passwordless SSH connection to the AtoM host for the user running the script and the AtoM host has to be already added to list of known hosts. [More info](https://wiki.archivematica.org/Upload_DIP#Send_your_DIPs_using_rsync)

Although this script is part of the automation-tools it's not completely automated yet, so it needs to be executed once per DIP and it requires the DIP path and the AtoM target description slug.

### Configuration

Suggested use of this script is by using the example shell script in the `etc` directory (`/etc/archivematica/automation-tools/atom_upload_script.sh`):

```
#!/bin/bash
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m dips.atom_upload \
  --atom-url <url> \
  --atom-email <email> \
  --atom-password <password> \
  --atom-slug <slug> \
  --rsync-target <host:path> \
  --dip-path <path> \
  --log-file <path>
```

(Note that the script calls the upload to AtoM script as a module using python's `-m` flag, this is required due to the use of relative imports in the code)

The script can be run from a shell window like:

```
user@host:/etc/archivematica/automation-tools$ sudo -u archivematica ./atom_upload_script.sh
```

#### Parameters

The `dips/atom_upload.py` accepts the following parameters:

* `--atom-url URL`: AtoM URL. Default: http://192.168.168.193
* `--atom-email EMAIL` [REQUIRED]: Email of the AtoM user to authenticate as.
* `--atom-password PASSWORD` [REQUIRED]: Password of the AtoM user to authenticate as.
* `--atom-slug SLUG` [REQUIRED]: Slug of the AtoM archival description to target in the upload.
* `--rsync-target HOST:PATH`: Host and path to place the DIP folder with `rsync`. Default: 192.168.168.193:/tmp
* `--dip-path PATH` [REQUIRED]: Absolute path to a local DIP to upload.
* `--log-file PATH`: Absolute path to a file to output the logs. Otherwise it will be created in the script directory.
* `-v, --verbose`: Increase the debugging output. Can be specified multiple times, e.g. `-vv`
* `-q, --quiet`: Decrease the debugging output. Can be specified multiple times, e.g. `-qq`
* `--log-level`: Set the level for debugging output. One of: 'ERROR', 'WARNING', 'INFO', 'DEBUG'. This will override `-q` and `-v`

Archivematica Client
--------------------

The transfers/amclient.py script is a module and CLI that provides functionality for interacting with the various Archivematica APIs.

Basic usage: amclient.py <subcommand> [optional arguments] <positional argument(s)>
  E.g.: `amclient.py close-completed-transfers --am-user-name islandora 234deffdf89d887a7023546e6bc0031167cedf6`

### Subcommands and arguments

* close-completed-transfers
  * purpose: close all completed transfers (those not failed or rejected)
  * positional argument: am_api_key - API key for Archivematica dashboard user
  * optional arguments:
    * `--am-user-name <username>` - username for Archivematica dashboard user (default: test)
    * `--am-url <url>` - Archivematica URL (default: `http://127.0.0.1`)

* close-completed-ingests
  * purpose: close all completed ingests (those not failed or rejected)
  * positional argument: am_api_key - API key for Archivematica dashboard user
  * optional arguments:
    * `--am-user-name <username>` - username for Archivematica dashboard user (default: test)
    * `--am-url <url>` - Archivematica URL (default: `http://127.0.0.1`)

* completed-transfers
  * purpose: print all completed transfers
  * positional argument: am_api_key - API key for Archivematica dashboard user
  * optional arguments:
    * `--am-user-name <username>` - username for Archivematica dashboard user (default: test)
    * `--am-url <url>` - Archivematica URL (default: `http://127.0.0.1`)

* completed-ingests
  * purpose: print all completed ingests
  * positional argument: am_api_key - API key for Archivematica dashboard user
  * optional arguments:
    * `--am-user-name <username>` - username for Archivematica dashboard user (default: test)
    * `--am-url <url>` - Archivematica URL (default: `http://127.0.0.1`)

* unapproved-transfers
  * purpose: print all unapproved transfers
  * positional argument: am_api_key - API key for Archivematica dashboard user
  * optional arguments:
    * `--am-user-name <username>` - username for Archivematica dashboard user (default: test)
    * `--am-url <url>` - Archivematica URL (default: `http://127.0.0.1`)

* transferables
  * purpose: print all transferable entities in the Storage Service
  * positional arguments:
    * ss_api_key - Storage Service API key
    * transfer_source - transfer source UUID
  * optional arguments:
    * `--ss-user-name <username>` - Storage Service username (default: `test`)
    * `--ss-url <url>` - Storage Service URL (default: `http://127.0.0.1:8000`)
    * `--transfer-path <path>` - relative path within the Transfer Source (default: `""`)

* aips
  * purpose: print all AIPs in the Storage Service
  * positional argument: ss_api_key - Storage Service API key
  * optional arguments:
    * `--ss-user-name <username>` - Storage Service username (default: `test`)
    * `--ss-url <url>` - Storage Service URL (default: `http://127.0.0.1:8000`)

* dips
  * purpose: print all DIPs in the Storage Service
  * positional argument: ss_api_key - Storage Service API key
  * optional arguments:
    * `--ss-user-name <username>` - Storage Service username (default: `test`)
    * `--ss-url <url>` - Storage Service URL (default: `http://127.0.0.1:8000`)

* aips2dips
  * purpose: print all AIPs in the Storage Service along with their corresponding DIPs
  * positional argument: ss_api_key - Storage Service API key
  * optional arguments:
    * `--ss-user-name <username>` - Storage Service username (default: `test`)
    * `--ss-url <url>` - Storage Service URL (default: `http://127.0.0.1:8000`)

* aip2dips
  * purpose: print an AIP with AIP_UUID along with its corresponding DIPs
  * positional arguments:
    * aip_uuid - UUID of the target AIP
    * ss_api_key - Storage Service API key
  * optional arguments:
    * `--ss-user-name <username>` - Storage Service username (default: `test`)
    * `--ss-url <url>` - Storage Service URL (default: `http://127.0.0.1:8000`)

* download-dip
  * purpose: download a DIP with DIP_UUID
  * positional arguments:
    * dip_uuid - UUID of the target DIP
    * ss_api_key - Storage Service API key
  * optional arguments:
    * `--ss-user-name <username>` - Storage Service username (default: `test`)
    * `--ss-url <url>` - Storage Service URL (default: `http://127.0.0.1:8000`)
    * `--directory <dir>` - directory path in which to save the DIP

In addition, these optional arguments are available for all subcommands:
* `--help`, `--h` - show help message and exit
* `--output-mode <mode>` - how to print output, JSON (default) or Python

See notes above about finding the Archivematica and Storage Service API keys.

Related Projects
----------------

* [automation-audit](https://github.com/finoradin/automation-audit): an interface for auditing and managing Archivematica's automation-tools.
