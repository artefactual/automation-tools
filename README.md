[![Travis CI](https://travis-ci.org/artefactual/automation-tools.svg?branch=master)](https://travis-ci.org/artefactual/automation-tools)

Automation Tools
================

The Automation Tools project is a set of Python scripts, that are designed to
automate the processing of transfers in an Archivematica pipeline.

<!-- doctoc: https://www.npmjs.com/package/doctoc -->
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Requirements](#requirements)
- [Installation](#installation)
- [Automated transfers](#automated-transfers)
  - [Configuration](#configuration)
    - [Parameters](#parameters)
    - [Setting processing rules](#setting-processing-rules)
    - [Getting the transfer source UUID](#getting-the-transfer-source-uuid)
    - [Getting API keys](#getting-api-keys)
  - [Hooks](#hooks)
    - [get-accession-id](#get-accession-id)
    - [pre-transfer hooks](#pre-transfer-hooks)
    - [user-input](#user-input)
  - [Logs](#logs)
  - [Multiple automated transfer instances](#multiple-automated-transfer-instances)
  - [`transfer_async.py`](#transfer_asyncpy)
  - [Tips for ingesting DSpace exports](#tips-for-ingesting-dspace-exports)
- [DIP creation](#dip-creation)
  - [Configuration](#configuration-1)
    - [Parameters](#parameters-1)
    - [Getting Storage Service API key](#getting-storage-service-api-key)
- [DIP upload to AtoM](#dip-upload-to-atom)
  - [Configuration](#configuration-2)
    - [Parameters](#parameters-2)
- [Reingest](#reingest)
- [Related Projects](#related-projects)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

Requirements
------------

* 7z (only for DIP creation script)

Installation
------------

* Checkout or link the code in this repo to
`/usr/lib/archivematica/automation-tools`
* Create virtualenv `/usr/share/python/automation-tools` and pip install
requirements there
* Create directories `/var/log/archivematica/automation-tools` and
`/var/archivematica/automation-tools` owned by user `archivematica`, for
log/database/PID files.
* Create directory `/etc/archivematica/automation-tools` and add configuration
files there. Files in the `etc/` directory of this repository can be used as an
example (also see below for more about configuration)

Automated transfers
-------------------

`transfers/transfer.py` is used to prepare transfers, move them into the
pipelines processing location, and take actions when user input is required.
Only one transfer is sent to the pipeline at a time, the scripts wait until
the current transfer is resolved (failed, rejected or stored as an AIP) before
automatically starting the next available transfer.

### Configuration

Suggested deployment is to use cron to run a shell script that runs the
automate transfer tool. Example shell script (for example in
`/etc/archivematica/automation-tools/transfer-script.sh`):

```
#!/bin/bash
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m transfers.transfer \
  --user <user> \
  --api-key <apikey> \
  --ss-user <user> \
  --ss-api-key <apikey> \
  --transfer-source <transfer_source_uuid> \
  --config-file <config_file>
```

(Note that the script calls the transfers script as a module using Python's
`-m` flag, this is required due to the use of relative imports in the code)

The script can be run from a shell window like:

```
user@host:/etc/archivematica/automation-tools$ sudo -u archivematica ./transfer-script.sh
```

It is suggested to run the script through a crontab entry for user
archivematica (to avoid the need to repeatedly invoke it manually):

```
*/5 * * * * /etc/archivematica/automation-tools/transfer-script.sh
```

When running, automated transfers stores its working state in a sqlite
database.  It contains a record of all the transfers that have been processed.
In a testing environment, deleting this file will cause the tools to re-process
any and all folders found in the Transfer Source Location.

#### Parameters

The `transfers.py` script can be modified to adjust how automated transfers
work.  The full set of parameters that can be changed are:

* `-u USERNAME, --user USERNAME` [REQUIRED]: Username of the Archivematica
dashboard user to authenticate as.
* `-k KEY, --api-key KEY` [REQUIRED]: API key of the Archivematica dashboard
user.
* `--ss-user USERNAME` [REQUIRED]: Username of the Storage Service user to
authenticate as. Storage Service 0.8 and up requires this; earlier versions
will ignore any value provided.
* `--ss-api-key KEY` [REQUIRED]: API key of the Storage Service user. Storage
Service 0.8 and up requires this; earlier versions will ignore any value
provided.
* `-t UUID, --transfer-source UUID`: [REQUIRED] Transfer Source Location UUID
to fetch transfers from. Check the next section for more details on this field.
* `--transfer-path PATH`: Relative path within the Transfer Source. Default: ""
* `--depth DEPTH, -d DEPTH`: Depth to create the transfers from relative to the
transfer source location and path. Default of 1 creates transfers from the
children of transfer-path.
* `--am-url URL, -a URL`:Archivematica URL. Default: http://127.0.0.1
* `--ss-url URL, -s URL`: Storage Service URL. Default: http://127.0.0.1:8000
* `--transfer-type TYPE`: Type of transfer to start. One of: 'standard'
(default), 'unzipped bag', 'zipped bag', 'dspace'.
* `--files`: If set, start transfers from files as well as folders.
* `--hide`: If set, hides the Transfer and SIP once completed.
* `--delete-on-complete`: If set, delete transfer source files from watched
directory once completed.
* `-c FILE, --config-file FILE`: config file containing file paths for
log/database/PID files. Default: log/database/PID files stored in the same
directory as the script (not recommended for production)
* `-v, --verbose`: Increase the debugging output. Can be specified multiple
times, e.g. `-vv`
* `-q, --quiet`: Decrease the debugging output. Can be specified multiple
times, e.g. `-qq`
* `--log-level`: Set the level for debugging output. One of: 'ERROR',
'WARNING', 'INFO', 'DEBUG'. This will override `-q` and `-v`

The `--config-file` specified can also be used to define a list of file
extensions that script files should have for execution. By default there is no
limitation, but it may be useful to specify this, for example
`scriptextensions = .py:.sh`. Multiple extensions may be specified, using '`:`'
as a separator.

#### Setting processing rules

The easiest way to configure the tasks that automation-tools will run is by
using the dashboard:

1. Go to Administration > Processing Configuration and choose the options you
wish to use.

2. Save the configuration on the form.

3. Copy the processing configuration file from
`/var/archivematica/sharedDirectory/sharedMicroServiceTasksConfigs/
processingMCPConfigs/defaultProcessingMCP.xml`
on the Archivematica host machine to the `transfers/pre-transfer/` directory
of your automation-tools installation location (also ensure that the
`default_config.py` script exists in the same directory)

#### Getting the transfer source UUID

To obtain the transfer source UUID for script invocation, visit the 'Locations'
tab in the Archivematica Storage Service web UI.

#### Getting API keys

To get the Archivematica API key, log in to Archivematica as the user you wish
to authenticate as. From the dashboard, click your username in the top right
corner, then select 'Your profile'. The API key will be displayed at the bottom
of the page.

To get the Storage Service API key, log in to the Storage Service as the user
you wish to authenticate as. From the dashboard, go to *Administration > Users*
and select 'Edit' for the user you want the key for. The API key will be
displayed at the bottom of the page. Storage Service versions earlier than
`0.8.x` do not require an API key, and will not provide one. In that case, fill
in `--ss-user` and `--ss-api-key` with stub data, since those parameters are
required by automated transfers.

### Hooks

During processing, automate transfers will run scripts from several places to
customize behaviour. These scripts can be in any language. If they are written
in Python, we recommend making them source compatible with Python 2 or 3.

There are three places hooks can be used to change the automate tools
behaviour.

* `transfers/get-accession-number` (script)
* `transfers/pre-transfer` (directory)
* `transfers/user-input` (directory)

Any new scripts added to these directories will automatically be run alongside
the existing scripts.

There are also several scripts provided for common use cases and examples of
processing that can be done. These are found in the `examples` directory sorted
by their usecase and can be copied or symlinked to the appropriate directory
for automation-tools to run them.

If you write a script that might be useful for others, please make a pull
request!

#### get-accession-id

* _Name:_ `get-accession-id`
* _Location:_ Same directory as transfers.py
* _Parameters:_ [`path`]
* _Return Code:_ 0
* _Output:_ Quoted value of the accession number (e.g. `"ID 42"`)

`get-accession-number` is run to customize the accession number of the created
transfer. Its single parameter is the path relative to the transfer source
location.  Note that no files are locally available when `get-accession-id` is
run. It should print to standard output the quoted value of the accession
number (e.g. `"ID42"`), `None`, or no output. If the return code is not 0, all
output is ignored. This is POSTed to the Archivematica REST API when the
transfer is created.

#### pre-transfer hooks

* _Parameters:_ [`absolute path`, `transfer type`]

All executable files found in `pre-transfer` are executed in alphabetical order
when a transfer is first copied from the specified Transfer Source Location to
the Archivematica pipeline. The return code and output of these scripts is not
evaluated.

All scripts are passed the same two parameters:

* `absolute path` is the absolute path on disk of the transfer
* `transfer type` is transfer type, the same as the parameter passed to the
script. One of 'standard', 'unzipped bag', 'zipped bag', 'dspace'.

There are some sample scripts in the pre-transfers directory that may be
useful, or models for your own scripts.

* `00_unbag.py`: Repackages a bag as a standard transfer, writing md5 hashes
from bag manifest into metadata/checksum.md5 file. This enables use of scripts
such as add_metadata.py with bags, which would otherwise cause failure at the
bag validation job.
* `add_metadata.py`: Creates a metadata.json file, by parsing data out of the
transfer folder name.  This ends up as Dublin Dore in a dmdSec of the final
METS file.
* `add_metadata_dspace.py`: Creates a metadata.csv file, by parsing data out of
the dspace export name. This ends up as Dublin Dore in a dmdSec of the final
METS file.
* `archivesspace_ids.py`: Creates an archivesspaceids.csv by parsing
ArchivesSpace reference IDs from filenames.  This will automate the matching
GUI if a DIP is uploaded to ArchivesSpace.
* `default_config.py`: Copies the included `defaultProcessingMCP.xml` into the
transfer directory. This file overrides any configuration set in the
Archivematica dashboard, so that user choices are guaranteed and avoided as
desired.

#### user-input

* _Parameters:_ [`microservice name`, `first time at wait point`,
`absolute path` , `unit UUID`, `unit name`, `unit type`]

All executable files in the `user-input folder` are executing in alphabetical
order whenever there is a transfer or SIP that is waiting at a user input
prompt. The return code and output of these scripts is not evaluated.

All scripts are passed the same set of parameters.

* `microservice name` is the name of the microservice awaiting user input. E.g.
Approve Normalization
* `first time at wait point` is the string "True" if this is the first time the
script is being run at this wait point, "False" if not. This is useful for only
notifying the user once.
* `absolute path` is the absolute path on disk of the transfer
* `unit UUID` is the SIP or transfer's UUID
* `unit name` is the name of the SIP or transfer, not including the UUID.
* `unit type` is either "SIP" or "transfer"

There are some sample scripts in the pre-transfers directory that may be
useful, or models for your own scripts.

* `send_email.py`: Emails the first time a transfer is waiting for input at
Approve Normalization.  It can be edited to change the email addresses it sends
notices to, or to change the notification message.

### Logs

Logs are written to a directory specified in the config file (or
`/var/log/archivematica/automation-tools/` by default). The logging level can
be adjusted, by modifying the transfers/transfer.py file. Find the following
section and changed `'INFO'` to one of `'INFO'`, `'DEBUG'`, `'WARNING'`,
`'ERROR'` or `'CRITICAL'`.

    'loggers': {
        'transfer': {
            'level': 'INFO',  # One of INFO, DEBUG, WARNING, ERROR, CRITICAL
            'handlers': ['console', 'file'],
        },
    },

### Multiple automated transfer instances

You may need to set up multiple automated transfer instances, for example if
required to ingest both standard transfers and bags. In cases where hooks are
the same for both instances, it could be achieved by setting up different
scripts, each one invoking the transfers.py script with the required
parameters. Example:

```
# first script invokes like this (standard transfer):
/usr/share/python/automation-tools/bin/python -m transfers.transfer \
  --user <user>  \
  --api-key <apikey> \
  --ss-user <user> \
  --ss-api-key <apikey> \
  --transfer-source <transfer_source_uuid_for_std_xfers> \
  --config-file <config_file>

# second script invokes like this (unzipped bags):
/usr/share/python/automation-tools/bin/python -m transfers.transfer \
  --user <user> \
  --api-key <apikey> \
  --ss-user <user> \
  --ss-api-key <apikey> \
  --transfer-source <transfer_source_2_uuid_for_bags> \
  --config-file <config_file_2> \
  --transfer-type 'unzipped bag'
```

`<config_file_1>` and `<config_file_2>` should specify different file names for
db/PID/log files. See transfers.conf and transfers-2.conf in etc/ for an
example

In case different hooks are required for each instance, a possible approach is
to checkout a new instance of the automation tools, for example in
`/usr/lib/archivematica/automation-tools-2`

### `transfer_async.py`

This is a new work-in-progress entry point similar to `transfers.transfer` that
uses the new asynchronous endpoints of Archivematica being developed under the
`/api/v2beta` API. It takes the same arguments, e.g.:

```
#!/usr/bin/env bash

cd /usr/lib/archivematica/automation-tools/

/usr/share/python/automation-tools/bin/python -m transfers.transfer_async \
  --user <user> --api-key <apikey> \
  --ss-user <user> --ss-api-key <apikey> \
  --transfer-source <transfer_source_uuid> \
  --config-file <config_file>
```

### Tips for ingesting DSpace exports

* At the transfer source location, put the DSpace item to be ingested in a
subdirectory (e.g., `ITEM@123-4567/ITEM@123-4567.zip`)  . The scripts
[here](https://github.com/artefactual-labs/transfer-source-helpers/) can be
used for this purpose

* Use the add_metadata_dspace.py pre-transfer script (described in
[pre-transfer hooks](#pre-transfer-hooks))

* Create a base `defaultProcessingMCP.xml` as described in
[Getting Correct UUIDs and Setting Processing Rules][], then append the
following preconfigured choice to the xml:
```
    <!-- DSpace skips quarantine -->
    <preconfiguredChoice>
      <appliesTo>05f99ffd-abf2-4f5a-9ec8-f80a59967b89</appliesTo>
      <goToChain>d4404ab1-dc7f-4e9e-b1f8-aa861e766b8e</goToChain>
    </preconfiguredChoice>
```

* When invoking the transfers.py script, add the `--transfer-type dspace`
parameter, for example:
```
/usr/share/python/automation-tools/bin/python -m transfers.transfer \
  --transfer-type dspace \
  --user <user> \
  --api-key <apikey> \
  --ss-user <user> \
  --ss-api-key <apikey> \
  --transfer-source <transfer_source_uuid> \
  --config-file <config_file>
```

DIP creation
------------

`aips/create_dip.py` and `aips/create_dips_job.py` can be used to make DIPs
from AIPs available in a Storage Service instance. Unlike DIPs created in
Archivematica, the ones created with this script will include only the original
files from the transfer and they will maintain the directories, filenames and
last modified date from those files. They will be placed with a copy of the
submissionDocumentation folder (if present in the AIP) and the AIP METS file
in a single ZIP file under the objects directory. Another METS file will be
generated alongside the objects folder containing only a reference to the ZIP
file (without AMD or DMD sections).

While `aips/create_dip.py` only processes one AIP per execution,
`aips/create_dips_job.py` will process all AIPs in a given Storage Service
location, keeping track of them in an SQLite database. Both scripts require 7z
installed and available to extract the AIPs downloaded from the Storage
Service.

### Configuration

Suggested use of these scripts is by using the example shell scripts in the
`etc` directory:

* [create_dip_script.sh][]
* [create_dips_job_script.sh][]

(Note that the scripts call the DIP creation scripts as modules using Python's
`-m` flag; this is required due to the use of relative imports in the code)

The scripts can be run from a shell window like:

```
user@host:/etc/archivematica/automation-tools$ sudo -u archivematica ./create_dip_script.sh

user@host:/etc/archivematica/automation-tools$ sudo -u archivematica ./create_dips_job_script.sh
```

For `aips/create_dips_job.py`, it is suggested to run the script through a
crontab entry for user archivematica, to avoid the need to repeatedly invoke it
manually to keep processing the same location:

```
*/5 * * * * /etc/archivematica/automation-tools/create_dips_job_script.sh
```

To process multiple Storage Service locations, `create_dips_job_script.sh`
could be duplicated with a different set of parameters to call
`aips/create_dips_job.py` and be executed in the same manner.

#### Parameters

Both scripts have the following parameters in common:

* `--ss-url URL, -s URL`: Storage Service URL. Default: http://127.0.0.1:8000
* `--ss-user USERNAME` [REQUIRED]: Username of the Storage Service user to
authenticate as. Storage Service 0.8 and up requires this; earlier versions
will ignore any value provided.
* `--ss-api-key KEY` [REQUIRED]: API key of the Storage Service user. Storage
Service 0.8 and up requires this; earlier versions will ignore any value
provided.
* `--tmp-dir PATH`: Absolute path to a directory where the AIP(s) will be
downloaded and extracted. Default: "/tmp"
* `--output-dir PATH`: Absolute path to a directory where the DIP(s) will be
created. Default: "/tmp"
* `--log-file PATH`: Absolute path to a file to output the logs. Otherwise it
will be created in the script directory.
* `-v, --verbose`: Increase the debugging output. Can be specified multiple
times, e.g. `-vv`
* `-q, --quiet`: Decrease the debugging output. Can be specified multiple
times, e.g. `-qq`
* `--log-level`: Set the level for debugging output. One of: 'ERROR',
'WARNING', 'INFO', 'DEBUG'. This will override `-q` and `-v`

`aips/create_dip.py` requires the following extra parameter:

* `--aip-uuid UUID` [REQUIRED]: AIP UUID in the Storage Service to create the
DIP from.

`aips/create_dips_job.py` requires the following extra parameters:

* `--location-uuid UUID` [REQUIRED]: Storage Service location UUID to be
processed.
* `--database-file PATH` [REQUIRED]: Absolute path to an SQLite database file
to keep track of the processed AIPs.

#### Getting Storage Service API key

See [Getting API keys](#getting-api-keys)

DIP upload to AtoM
------------------

`dips/atom_upload.py` is available to upload a DIP folder from the local
filesystem to an external AtoM instance. It requires a passwordless SSH
connection to the AtoM host for the user running the script and the AtoM host
has to be already added to list of known hosts.
[More info][Send DIPS]

Although this script is part of the automation-tools it's not completely
automated yet, so it needs to be executed once per DIP and it requires the DIP
path and the AtoM target description slug.

### Configuration

Suggested use of this script is by using the example shell script in the `etc`
directory (`/etc/archivematica/automation-tools/atom_upload_script.sh`):

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

(Note that the script calls the upload to AtoM script as a module using
Python's `-m` flag, this is required due to the use of relative imports in
the code)

The script can be run from a shell window like:

```
user@host:/etc/archivematica/automation-tools$ sudo -u archivematica ./atom_upload_script.sh
```

#### Parameters

The `dips/atom_upload.py` accepts the following parameters:

* `--atom-url URL`: AtoM URL. Default: http://192.168.168.193
* `--atom-email EMAIL` [REQUIRED]: Email of the AtoM user to authenticate as.
* `--atom-password PASSWORD` [REQUIRED]: Password of the AtoM user to
authenticate as.
* `--atom-slug SLUG` [REQUIRED]: Slug of the AtoM archival description to
target in the upload.
* `--rsync-target HOST:PATH`: Host and path to place the DIP folder with
`rsync`. Default: 192.168.168.193:/tmp
* `--dip-path PATH` [REQUIRED]: Absolute path to a local DIP to upload.
* `--log-file PATH`: Absolute path to a file to output the logs. Otherwise it
will be created in the script directory.
* `-v, --verbose`: Increase the debugging output. Can be specified multiple
times, e.g. `-vv`
* `-q, --quiet`: Decrease the debugging output. Can be specified multiple
times, e.g. `-qq`
* `--log-level`: Set the level for debugging output. One of: 'ERROR',
'WARNING', 'INFO', 'DEBUG'. This will override `-q` and `-v`

Reingest
--------

The *transfers/reingest.py* script is a module and CLI (command line interface)
tool that provides functionality for bulk-reingest of compressed AIPs. Given a
user formatted list of AIP UUIDs it can complete the bulk-reingest of any AIP
listed.

An example list that could be stored in a text file might be:
```
   ["54369f6a-aa82-4b29-80c9-834d3625397d",
    "b18801dd-30ec-46ba-ac6b-4cb561585ac9",
    "b4d37c2c-df30-4a16-8f2f-4cb02a5d53cb"]
```

Alternatively the command can be configured using the `--processfromstorage`
switch to identify compressed archives in the storage service and the transfer
initiated that way.

Command line switches can be seen by running the script without any other
arguments:
```
$ python -m transfers.reingest
usage: reingest.py [-h] --config CONFIG [--listcompressedaips]
                   [--compareaiplist COMPAREAIPLIST]
                   [--processfromlist PROCESSFROMLIST] [--processfromstorage]
                   [--dbstatus] [--logging [LOGGING]]

Reingest AIPs Automatically. Created for CCArch to reingest compressed AIPs
using an alternative Archivematica ProcessingMCP.xml file. A work in progress,
with some improvements that can be made to long-running processes like this
over time.

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG       REQUIRED: configure the script
  --listcompressedaips  list compressed AIPs in storage
  --compareaiplist COMPAREAIPLIST
                        compare stored compressed aips with an existing list
  --processfromlist PROCESSFROMLIST
                        reingest from a list of UUIDs
  --processfromstorage  reingest compressed AIPs from the Storage Service
  --dbstatus            output log from the database
  --logging [LOGGING]   logging level, INFO, DEBUG, WARNING, ERROR
```

Configuration of reingest.py is done via *transfers/reingestconfig.json*. You
will need to configure API parameters via `reingestconfig.json`. See notes
above about finding the Archivematica and Storage Service API keys. An example
configuration with placeholder parameters is provided in
[reingestconfig.json](transfers/reingestconfig.json)

*Reingest.py* is best used via the shell script provided in the
[*transfers/examples/reingest*](transfers/examples/reingest) folder. As it is
designed for bulk-reingest, it is best used in conjunction with a cronfile, an
example of which is provided in the same folder.

State is recorded in an sqlite database. When the script is executed over and
over the state is updated or read. Progress is then stored in a separate log
file.

The log file *reingest.log* will either be under *transfers/reingest.log* or
somewhere else specified in your *reingestconfig.json* file.

We can review *reingest.log* to see where in the process the reingest is:

1. First time the code is run we can see reingest begin and its automatic
   approval in the pipeline:
```
$ ./run-reingest.sh

INFO      2018-10-25 23:59:19 reingest.py:422  Processing throttle set to 1, approval retries set to 2
INFO      2018-10-25 23:59:19 reingest.py:470  Reingesting from Storage Service list of AIPs
INFO      2018-10-25 23:59:19 reingest.py:147  Reingest UUID to work with 7b0bef8e-ad64-4f74-967e-258299f38272
INFO      2018-10-25 23:59:19 reingest.py:149  Checking status of 7b0bef8e-ad64-4f74-967e-258299f38272 once in transfer queue
INFO      2018-10-25 23:59:21 reingest.py:162  Attempting to approve transfer following the initialization of reingest.
INFO      2018-10-25 23:59:21 reingest.py:168  Approving reingest automatically. Directory to approve transfer-two
INFO      2018-10-25 23:59:21 reingest.py:172  Approval successful, returning True
INFO      2018-10-25 23:59:21 reingestmodel.py:146  Setting in progress 1dd914ad-520d-447f-8207-c49aaad5612d, transfer uuid 7b0bef8e-ad64-4f74-967e-258299f38272
INFO      2018-10-25 23:59:21 reingestmodel.py:119  setting status StatusEnum.STATUS_IN_PROGRESS for AIP 1dd914ad-520d-447f-8207-c49aaad5612d
INFO      2018-10-25 23:59:21 reingest.py:194  Removing PID for current process.
```

2. While the reingest progresses we can see that there isn't an ingest to
   monitor, and the status doesn't need updating.
```
$ ./run-reingest.sh

INFO      2018-10-25 23:59:39 reingest.py:422  Processing throttle set to 1, approval retries set to 2
INFO      2018-10-25 23:59:39 reingest.py:463  Database already contains AIPs, ignoring LOAD
WARNING   2018-10-25 23:59:39 utils.py:58   GET Request to http://127.0.0.1:62080/api/ingest/status/1dd914ad-520d-447f-8207-c49aaad5612d/ returned 400 BAD REQUEST
INFO      2018-10-25 23:59:39 reingest.py:304  Pool is less than one, exiting, until next run
INFO      2018-10-25 23:59:39 reingest.py:194  Removing PID for current process.
```

3. Once the ingest stage of the workflow has started, the state is updated.
```
$ ./run-reingest.sh

INFO      2018-10-26 00:00:03 reingest.py:422  Processing throttle set to 1, approval retries set to 2
INFO      2018-10-26 00:00:03 reingest.py:463  Database already contains AIPs, ignoring LOAD
INFO      2018-10-26 00:00:03 reingest.py:276  AIP 1dd914ad-520d-447f-8207-c49aaad5612d processing is now in ingest
INFO      2018-10-26 00:00:03 reingest.py:304  Pool is less than one, exiting, until next run
INFO      2018-10-26 00:00:03 reingest.py:194  Removing PID for current process.
```

4. Following completion of the first reingest, a second is started and we can
   see similar log entries to those in *1.*
```
$ ./run-reingest.sh

INFO      2018-10-26 00:00:26 reingest.py:422  Processing throttle set to 1, approval retries set to 2
INFO      2018-10-26 00:00:26 reingest.py:463  Database already contains AIPs, ignoring LOAD
INFO      2018-10-26 00:00:26 reingestmodel.py:119  setting status StatusEnum.STATUS_COMPLETE for AIP 1dd914ad-520d-447f-8207-c49aaad5612d
INFO      2018-10-26 00:00:26 reingestmodel.py:160  AIP 1dd914ad-520d-447f-8207-c49aaad5612d processed in -65 seconds
INFO      2018-10-26 00:00:27 reingest.py:147  Reingest UUID to work with 884f9d4d-a415-41ae-bd38-6958755e22ec
INFO      2018-10-26 00:00:27 reingest.py:149  Checking status of 884f9d4d-a415-41ae-bd38-6958755e22ec once in transfer queue
INFO      2018-10-26 00:00:28 reingest.py:162  Attempting to approve transfer following the initialization of reingest.
INFO      2018-10-26 00:00:28 reingest.py:168  Approving reingest automatically. Directory to approve transfer-one
INFO      2018-10-26 00:00:28 reingest.py:172  Approval successful, returning True
INFO      2018-10-26 00:00:28 reingestmodel.py:146  Setting in progress 2bfe450f-d295-4d21-b3f2-7ca43320043e, transfer uuid 884f9d4d-a415-41ae-bd38-6958755e22ec
INFO      2018-10-26 00:00:28 reingestmodel.py:119  setting status StatusEnum.STATUS_IN_PROGRESS for AIP 2bfe450f-d295-4d21-b3f2-7ca43320043e
INFO      2018-10-26 00:00:28 reingest.py:194  Removing PID for current process.
```

The database file *reingest.db* will either be under *transfers/reingest.db* or
somewhere else specified in your *reingestconfig.json* file.

The database can be read using tools capable of reading an sqlite database,
e.g. [sqlite3](https://packages.ubuntu.com/bionic/sqlite3). A snippet of the
database might look as follows:
```
aip_uuid                              transfer_uuid                         status           message     start_time                  end_time
------------------------------------  ------------------------------------  ---------------  ----------  --------------------------  --------------------------
1dd914ad-520d-447f-8207-c49aaad5612d  7b0bef8e-ad64-4f74-967e-258299f38272  STATUS_COMPLETE              2018-10-25 21:59:21.304078  2018-10-25 22:00:26.772603
2bfe450f-d295-4d21-b3f2-7ca43320043e  884f9d4d-a415-41ae-bd38-6958755e22ec  STATUS_IN_PROGR              2018-10-25 22:00:28.964030
3c5bf34c-1001-4ea3-8d59-a7989e6f73ca                                        STATUS_NEW
cca46884-eb6e-4a50-95b5-a99630e1bc4b                                        STATUS_NEW
```

Here we can see one transfer is complete (`STATUS_COMPLETE`). One is in
progress (`STATUS_IN_PROGRESS`). Two are new (`STATUS_NEW`) and are
yet to be processed.

Once all AIPs selected to be reingested are done, the status will read
`STATUS-COMPLETE`.

`STATUS_ERROR` may also be shown. Archivematica does not provide detailed error
information via its API as of yet, and so, if an error does occur, the user is
expected to find the AIP in the pipeline and look at which jobs may have
failed.

If the transfer or ingest has already been removed from the dashboard then the
information could be found by looking up the status of the microservices:

Via the user interface:

* *http://{archivematica-url}}/transfer/{UUID}/*
* *http://{archivematica-url}}/ingest/{UUID}/*

Via the API (users will need to filter on package UUID):

* *http://{archivematica-url}}/transfer/status/*
* *http://{archivematica-url}}/ingest/status/*

Related Projects
----------------

* [automation-audit](https://github.com/finoradin/automation-audit): an
interface for auditing and managing Archivematica's automation-tools.

[Getting Correct UUIDs and Setting Processing Rules]: #getting-correct-uuids-and-setting-processing-rules
[create_dip_script.sh]: https://github.com/artefactual/automation-tools/blob/master/etc/create_dip_script.sh
[create_dips_job_script.sh]: https://github.com/artefactual/automation-tools/blob/master/etc/create_dips_job_script.sh
[Send DIPS]: https://wiki.archivematica.org/Upload_DIP#Send_your_DIPs_using_rsync
