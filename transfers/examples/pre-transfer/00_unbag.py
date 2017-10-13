#!/usr/bin/env python

# Script to re-package unzipped bags as standard transfers, utilizing checksums from bag manifest.
# Assumes bags are structured as either bag/data/(content) or bag/data/objects/(content).
# Enables use of scripts to add metadata to SIP without failing transfer at bag validation.

from __future__ import print_function, unicode_literals

import os
import shutil
import sys


def main(transfer_path):
    transfer_path = os.path.abspath(transfer_path)

    # check if transfer is an unzipped bag
    if not os.path.isfile(os.path.join(transfer_path, 'bag-info.txt')):
        return 1

    # move files in data up one level if 'objects' folder already exists
    data_path = os.path.join(transfer_path, 'data')
    if os.path.isdir(os.path.join(data_path, 'objects')):
        data_contents = os.listdir(data_path)
        data_contents = [os.path.join(data_path, filename) for filename in data_contents]
        for f in data_contents:
            shutil.move(f, transfer_path)
    # otherwise, rename data to objects
    else:
        os.rename(data_path, os.path.join(transfer_path, 'objects'))

    # create metadata and subdoc folders if don't already exist
    metadata_dir = os.path.join(transfer_path, 'metadata')
    subdoc_dir = os.path.join(metadata_dir, 'submissionDocumentation')
    if not os.path.isdir(metadata_dir):
        os.mkdir(metadata_dir)
    if not os.path.isdir(subdoc_dir):
        os.mkdir(subdoc_dir)

    # write manifest checksums to checksum file
    with open(os.path.join(transfer_path, 'manifest-md5.txt'), 'r') as old_file:
        with open(os.path.join(metadata_dir, 'checksum.md5'), 'w') as new_file:
                for line in old_file:
                    if "data/objects/" in line:
                        new_line = line.replace("data/objects/", "../objects/")
                    else:
                        new_line = line.replace("data/", "../objects/")
                    new_file.write(new_line)

    # move bag files to submissionDocumentation
    for bagfile in 'bag-info.txt', 'bagit.txt', 'manifest-md5.txt', 'tagmanifest-md5.txt':
        shutil.move(os.path.join(transfer_path, bagfile), subdoc_dir)

    return 0


if __name__ == '__main__':
    transfer_path = sys.argv[1]
    sys.exit(main(transfer_path))
