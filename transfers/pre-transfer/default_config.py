#!/usr/bin/env python2

import os
import shutil
import sys


def main(transfer_path):
    # Update default config
    print 'copying new processingMCP to', transfer_path
    destination = os.path.join(transfer_path, 'processingMCP.xml')
    shutil.copyfile("pre-transfer/defaultProcessingMCP.xml", destination)


if __name__ == '__main__':
    transfer_path = sys.argv[1]
    main(transfer_path)
