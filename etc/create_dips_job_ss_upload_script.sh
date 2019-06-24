#!/bin/bash
# create_dips_job script example with upload to Storage Service
# /etc/archivematica/automation-tools/create_dips_job_ss_upload_script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/venv/bin/python -m aips.create_dips_job \
  --ss-user <username> \
  --ss-api-key <api_key> \
  --location-uuid <uuid> \
  --database-file <path> \
  --tmp-dir <path> \
  --output-dir <path> \
  --log-file <path> \
  --delete-local-copy \
  ss-upload \
  --pipeline-uuid <uuid> \
  --cp-location-uuid <uuid> \
  --ds-location-uuid <uuid> \
  --shared-directory <path>
