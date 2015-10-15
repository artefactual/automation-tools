#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import os


HASHFUNC_EXT = 'md5'


def extract_checksum(filename, metadata_path):
    filename = '{}.{}'.format(os.path.join(metadata_path, filename),
                              HASHFUNC_EXT)
    try:
        with open(filename, 'r') as f:
            return f.readline().strip()
    except IOError:
        return None


def main(transfer_path):
    """
    Generate the standard checksum file in a transfer where every object has
    its checksum stored in a separate text file under the metadata directory,
    e.g. transfer-A is the original transfer and transfer-B is the resulting
    transfer after running this script.

       .
       ├── transfer-A
       |   ├── foobar.mp3
       │   └── metadata
       │       └── foobar.mp3.md5
       └── transfer-B
        ├── foobar.mp3
        └── metadata
            ├── checksum.md5
            └── foobar.mp3.md5

    """
    transfer_path = os.path.abspath(transfer_path)
    metadata_path = os.path.join(transfer_path, 'metadata')

    if not os.path.exists(metadata_path):
        print('metadata directory cannot be found', metadata_path,
              file=sys.stderr)
        return 1

    checksum_file = os.path.join(metadata_path, 'checksum.{}'.format(
                                 HASHFUNC_EXT))
    with open(checksum_file, 'w') as f:
        for root, dirs, files in os.walk(transfer_path):
            if root == transfer_path:
                dirs.remove('metadata')
            for item in files:
                file_path = os.path.join(root, item)
                hashsum = extract_checksum(item, metadata_path)
                if hashsum is None:
                    print('Cannot find checksum of', file_path, file=sys.stderr)
                    continue
                hashfile = './{}'.format(os.path.relpath(file_path, transfer_path))
                checksum_entry = '{}  {}\n'.format(hashsum, hashfile)
                f.write(checksum_entry)

    print('Checksums file generated:', checksum_file)
    return 0


if __name__ == '__main__':
    transfer_path = sys.argv[1]
    sys.exit(main(transfer_path))

