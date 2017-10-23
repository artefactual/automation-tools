#!/bin/bash
# storage move package script example
# /etc/archivematica/automation-tools/storage-script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/bin/python -m storage.move_packages --config-file /etc/archivematica/automation-tools/storage.conf --ss-user USERNAME --ss-api-key KEY --from-location a13e466d-a144-430a-85b3-95e6aaa52f20  --to-location fbdf5325-c342-406a-ba66-3f4e3f73cf5f