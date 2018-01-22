#!/bin/bash
# create_dips_job script example
# /etc/archivematica/automation-tools/create_dips_job_script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m aips.create_dips_job \
  --ss-user <username> \
  --ss-api-key <api_key> \
  --location-uuid <uuid> \
  --database-file <path> \
  --tmp-dir <path> \
  --output-dir <path> \
  --log-file <path>
