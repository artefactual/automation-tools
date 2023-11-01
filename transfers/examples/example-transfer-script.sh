#!/bin/bash
# transfer script example
# /etc/archivematica/automation-tools/transfer-script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/venv38/bin/python -m transfers.transfer \
  --am-url http://archmatica:80 \
  --ss-url http://archmatica:8000 \
  --user xxxxx \
  --api-key xxxxxx \
  --ss-user support \
  --ss-api-key xxxxxxx \
  --transfer-source xxxxxxxx \
  --delete-on-complete \
  --hide \
  --transfer_delete_path '/mnt/transferSource/BatchTransfer/' \
  --config-file transfers.conf \
  --transfer-type 'unzipped bag'
