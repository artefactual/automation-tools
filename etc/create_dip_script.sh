#!/bin/bash
# create_dip script example
# /etc/archivematica/automation-tools/create_dip_script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m aips.create_dip \
  --ss-user <username> \
  --ss-api-key <api_key> \
  --aip-uuid <uuid> \
  --tmp-dir <path> \
  --output-dir <path> \
  --log-file <path>
