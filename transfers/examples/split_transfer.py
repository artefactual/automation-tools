#!/usr/bin/env python3
r"""
Split a single large transfer into several.

Usage example:

   $ ./split_transfer.py \
       --prefix="Foobar_" \
       /var/archivematica/automation-sources/very-big-source/Foobar/Foobar-SIP/ \
       /var/archivematica/automation-sources/very-big-source/Foobar/Foobar-SIP-splitted/

The original location is not modified. Preferably run locally. It's been tested over NFS. It uses rsync so if you run it twice the files won't be copied again unless they don't match (rsync provides multiple matching algorithms).

Make sure that you have permissions on the locations you are reading or writing!
"""
from __future__ import print_function, unicode_literals

import argparse
import csv
import os
import subprocess
import sys


class SIPMetadata(object):
    def __init__(self, source_sip, csv_delimiter):
        self.csv_file = os.path.join(source_sip, 'metadata', 'metadata.csv')
        self.csv_delimiter = csv_delimiter
        self.index_csv()

    def index_csv(self):
        self.index = dict()
        with open(self.csv_file, 'rt') as csvf:
            csvr = csv.reader(csvf, delimiter=self.csv_delimiter)
            for i, row in enumerate(csvr):
                if i == 0:
                    self.headers = row
                    continue
                path = row[0]
                self.index[path] = row

    def get_object_metadata(self, path):
        return (self.headers, self.index[path])


def rsync(src, dst, verbose=False):
    print('Copying objects... [src={}] [dst={}]'.format(src, dst))
    subprocess.check_call(['rsync', '-a', src, dst])


def make_dirs(dirname):
    try:
        os.makedirs(dirname)
        print("make", dirname)
    except OSError:
        pass


def main(source_sip, target_dir, csv_delimiter, prefix=None, metadata_only=False):
    metadata = SIPMetadata(source_sip, csv_delimiter)
    objects_dir = os.path.abspath(os.path.join(source_sip, 'objects'))
    target_dir = os.path.abspath(target_dir)

    # Create submissionDocumentation transfer
    sdoc_prefix = 'transfer_' if prefix is None else prefix
    sdoc_dir_src = os.path.join(source_sip, 'metadata', 'submissionDocumentation', '')
    sdoc_dir_dst = os.path.join(target_dir, '{}submissionDocumentation'.format(sdoc_prefix), 'metadata', 'submissionDocumentation', '')

    make_dirs(sdoc_dir_dst)

    rsync(sdoc_dir_src, sdoc_dir_dst, verbose=True)
    print('\033[92m{}: {}\033[00m'.format('submissionDocumentation should be available at', sdoc_dir_dst))

    # List of directories under objects/
    objects_dirs = os.listdir(objects_dir)
    if len(objects_dirs) < 1:
        print('Object directory is empty: {}'.format(objects_dir))

    for i, item in enumerate(objects_dirs):
        print('- {}'.format(item))
        src = os.path.join(objects_dir, item, '')

        item_dst = item if prefix is None else prefix + item
        dst_objects = os.path.join(target_dir, item_dst, 'objects', item, '')
        dst_metadata = os.path.join(target_dir, item_dst, 'metadata', '')

        # Move objects
        if not metadata_only:
            make_dirs(dst_objects)
            rsync(src, dst_objects)

        # Split metadata
        make_dirs(dst_metadata)
        try:
            headers, mdata = metadata.get_object_metadata('objects/{}'.format(item))
        except KeyError:
            print('No metadata for {}'.format(item))
        else:
            print('Writing metadata...')
            csv_file = os.path.join(dst_metadata, 'metadata.csv')
            with open(csv_file, 'w+') as csvf:
                csvw = csv.writer(csvf)
                csvw.writerow(headers)
                csvw.writerow(mdata)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Split a single large transfer into several')
    parser.add_argument('source_sip', help='Transfer to split')
    parser.add_argument('target_dir', help='Directory to place the output in')
    parser.add_argument('--csv-delimiter', type=str, default=',', help='Delimiter of the CSV metadata file.')
    parser.add_argument('--prefix', type=str, default=None, help='Prefix of the resulting split transfers.')
    parser.add_argument('--metadata-only', action='store_true', help='Only update the metadata.csv; do not move files')
    args = parser.parse_args()
    sys.exit(main(args.source_sip, args.target_dir, args.csv_delimiter, args.prefix, args.metadata_only))
