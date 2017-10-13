#!/usr/bin/env python

from __future__ import print_function

import csv
import os
import sys


def main(transfer_path):
    if not os.path.isdir(transfer_path):
        return 1
    transfer_path = transfer_path.rstrip('/')
    basename = os.path.basename(transfer_path)
    try:
        dc_id, _, _ = basename.split('---')
    except ValueError:
        print('Error splitting', basename)
        return 1
    print('Identifier: ', dc_id, end='')
    metadata = [
        ['parts', 'dc.identifier'],
        ['objects', dc_id]
    ]
    metadata_path = os.path.join(transfer_path, 'metadata')
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    metadata_path = os.path.join(metadata_path, 'metadata.csv')
    with open(metadata_path, 'w') as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(metadata)

    return 0


if __name__ == '__main__':
    transfer_path = sys.argv[1]
    sys.exit(main(transfer_path))
