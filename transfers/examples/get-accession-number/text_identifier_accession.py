#!/usr/bin/env python

from __future__ import print_function

import sys


def main(dirname):
    # Expecting a directory name like sometext---dc.identifier---accession
    parts = dirname.rsplit('---', 1)
    try:
        print('"' + parts[1] + '"')  # Accession ID must be quoted
    except Exception:
        print('None')


if __name__ == '__main__':
    main(sys.argv[1])
    sys.exit(0)
