#!/usr/bin/env python

from __future__ import print_function

import json
import os
import sys

def main(transfer_path):
    basename = os.path.basename(transfer_path)
    try:
        dc_id, _, _ = basename.split('---')
    except ValueError:
        return 1
    print('Identifier: ', dc_id, end='')
    metadata = [
        {
            'parts': 'objects',
            'dc.identifier': dc_id,
        }
    ]
    metadata_path = os.path.join(transfer_path, 'metadata')
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    metadata_path = os.path.join(metadata_path, 'metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    return 0

if __name__ == '__main__':
    transfer_path = sys.argv[1]
    sys.exit(main(transfer_path))
