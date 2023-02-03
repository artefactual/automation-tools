#!/bin/bash
# transfer script example
# /etc/archivematica/automation-tools/transfer-script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/venv/bin/python -m transfers.transfer \
  --am-url <archivematica_url> \
  --ss-url <storage_service_url> \
  --user <archivematica_user> \
  --api-key <archivematica_api_key> \ 
  --ss-user <storage_service_user> \
  --ss-api-key <storage_service_api_key> \
  --transfer-source <transfer_source_uuid> \
  --config-file <config_file>
