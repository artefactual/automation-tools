#!/bin/bash
# Docker transfer script example.
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/venv/bin/python -m transfers.transfer \
  --am-url http://archivematica-dashboard:8000 \
  --ss-url http://archivematica-storage-service:8000 \
  --user test \
  --api-key test \
  --ss-user test \
  --ss-api-key test \
  --config-file /etc/archivematica/automation-tools/transfers.conf \
  --transfer-source $TRANSFER_SOURCE
