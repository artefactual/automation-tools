#!/bin/bash
# storage_service_upload script example
# /etc/archivematica/automation-tools/storage_service_upload_script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/venv/bin/python -m dips.storage_service_upload \
  --ss-user <username> \
  --ss-api-key <api_key> \
  --pipeline-uuid <uuid> \
  --cp-location-uuid <uuid> \
  --ds-location-uuid <uuid> \
  --shared-directory <path> \
  --dip-path <path> \
  --aip-uuid <uuid> \
  --log-file <path>
