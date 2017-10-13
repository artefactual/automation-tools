#!/usr/bin/env python

from __future__ import print_function

import csv
import errno
import os
import sys


def main(transfer_path):
    """
    Generate archivesspaceids.csv with reference IDs based on filenames.
    """
    data_path = transfer_path
    if os.path.exists(os.path.join(transfer_path, 'data')):  # Assume this is a bag
        data_path = os.path.join(transfer_path, 'data')

    archivesspaceids_path = os.path.join(data_path, 'metadata', 'archivesspaceids.csv')
    if os.path.exists(archivesspaceids_path):
        print(archivesspaceids_path, 'already exists, exiting')
        return

    as_ids = []

    for dirpath, _, filenames in os.walk(transfer_path):
        for filename in filenames:
            identifier = os.path.splitext(filename)[0]
            relative_path = os.path.join(dirpath, filename).replace(transfer_path, '')
            if not identifier or not relative_path:
                continue
            as_ids.append([relative_path, identifier])

    print(as_ids)
    # Write out CSV
    try:
        os.mkdir(os.path.join(data_path, 'metadata'))
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass  # Already exists
        else:
            raise
    with open(archivesspaceids_path, 'w') as f:
        writer = csv.writer(f)
        writer.writerows(as_ids)


if __name__ == '__main__':
    transfer_path = sys.argv[1]
    sys.exit(main(transfer_path))
