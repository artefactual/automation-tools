#!/usr/bin/env python2

import json
import os
import sys

def main(transfer_path):
    basename = os.path.basename(transfer_path)
    try:
        component_number, component_id, _ = basename.split('---')
    except ValueError:
        return 1
    # we really need the component number in the json file
    # this is wrongly being labeled "component.identifier"
    # but won't change it because this is how the archivematica
    # code is currently parsing it
    metadata = [
        {
            'component.identifier': component_number,
        }
    ]
    metadata_path = os.path.join(transfer_path, 'metadata')
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)
    metadata_path = os.path.join(metadata_path, 'netx.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f)
    return 0

if __name__ == '__main__':
    transfer_path = sys.argv[1]
    sys.exit(main(transfer_path))
