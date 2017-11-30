#!/bin/bash
# atom_upload script example
# /etc/archivematica/automation-tools/atom_upload_script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m dips.atom_upload \
  --atom-url <url> \
  --atom-email <email> \
  --atom-password <password> \
  --atom-slug <slug> \
  --rsync-target <host:path> \
  --dip-path <path> \
  --log-file <path>
