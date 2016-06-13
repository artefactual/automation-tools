#!/bin/bash
# transfer script example
# /etc/archivematica/automation-tools/transfer-script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m transfers.transfer --user <user> --api-key <apikey>  --ss-user <user> --ss-api-key <apikey> --transfer-source <transfer_source_uuid> --config-file <config_file>
