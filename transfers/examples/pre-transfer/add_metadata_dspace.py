#!/usr/bin/env python

from __future__ import print_function

import csv
import os
import re
import sys

def main(transfer_path):
    """
    Add DSpace identifier to SIP metadata.

    Check for files in the transfer path. If there is exactly one, parse out the identifier.

    Example filenames -> ID:
    COLLECTION@2429-11123.zip -> 2429/11123
    COMMUNITY@2429-1036.zip -> 2429/1036
    ITEM@2429-100.zip -> 2429/100
    ITEM@2429-1005.zip -> 2429/1005
    ITEM@2429-10029.zip -> 2429/10029
    SITE@2429-0.zip -> 2429/0
    """
    files = os.listdir(transfer_path)
    if len(files) != 1:
        return 2
    basename = os.path.basename(files[0])
    regex = r'[\w]+@([\d]+)-([\d]+)\.zip$'
    match = re.search(regex, basename)
    if not match:
        return 1

    dc_id = '/'.join(match.groups())
    print('Identifier: ', dc_id, end='')
    header = ['parts', 'dc.identifier']
    data = ['objects', dc_id]
    metadata_path = os.path.join(transfer_path, 'metadata')
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    metadata_path = os.path.join(metadata_path, 'metadata.csv')
    with open(metadata_path, 'w') as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerow(data)
    return 0

if __name__ == '__main__':
    transfer_path = sys.argv[1]
    sys.exit(main(transfer_path))
