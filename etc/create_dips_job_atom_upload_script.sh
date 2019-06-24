#!/bin/bash
# create_dips_job script example with upload to AtoM
# /etc/archivematica/automation-tools/create_dips_job_atom_upload_script.sh
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
  atom-upload \
  --atom-url <url> \
  --atom-email <email> \
  --atom-password <password> \
  --atom-slug <slug> \
  --rsync-target <host:path>
