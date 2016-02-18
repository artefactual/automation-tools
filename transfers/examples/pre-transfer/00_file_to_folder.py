#!/usr/bin/env python

from __future__ import print_function, unicode_literals
import datetime
import os
import sys

def main(transfer_path):
    """
    If transfer_path is a file, move into a directory of the same name.
    """
    if os.path.isdir(transfer_path):
        return 1
    # Move file into temp dir
    transfer_dir, transfer_name = os.path.split(transfer_path)
    temp_dir = os.path.join(transfer_dir, 'temp-' + str(datetime.datetime.utcnow()))
    os.mkdir(temp_dir)
    os.rename(transfer_path, os.path.join(temp_dir, transfer_name))
    # Rename temp dir to the same as the file
    os.rename(temp_dir, transfer_path)

if __name__ == '__main__':
    transfer_path = sys.argv[1]
    main(transfer_path)
