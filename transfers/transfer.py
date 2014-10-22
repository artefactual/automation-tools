#!/usr/bin/env python2

from __future__ import print_function

import datetime
import errno
import os
import requests
import shutil
import time


# Change these to match your config
url = 'http://127.0.0.1/api/transfer/'
user_name = 'test'
api_key = 'xxx'

path = "/mnt/foo"

count_file = "/var/archivematica/transfer/count"
processing_available = "/etc/archivematica/transfer/processing-available/"


def list_transfers(url, api_key, user_name):
    get_url = url + "/unapproved?username=" + user_name + "&api_key=" + api_key
    g = requests.get(get_url)
    return g.json()


def approve_transfer(directory_name, url, api_key, user_name):
    print("approving " + directory_name)
    time.sleep(6)
    # List available transfers
    post_url = url + "/approve"
    waiting_transfers = list_transfers(url, api_key, user_name)
    for a in waiting_transfers['results']:
        print("found waiting transfer: " + a['directory'])
        if a['directory'] == directory_name:
            # Post to approve transfer
            params = {'username': user_name, 'api_key': api_key, 'type': a['type'], 'directory': directory_name}
            r = requests.post(post_url, data=params)

            return r.status_code
            break
        else:
            return -1
            print(a['directory'] + " is not what we are looking for")


def copy(src, dest):
    try:
        shutil.copytree(src, dest)
    except OSError as e:
        # If the error was caused because the source wasn't a directory
        if e.errno == errno.ENOTDIR:
            shutil.copy(src, dest)
        else:
            print('Directory not copied. Error: %s' % e)

now = datetime.datetime.now()
print("waking up at " + str(now))

with open(count_file, 'r') as f:
    last_transfer = int(f.readline())

start_at = last_transfer
dirs = os.listdir(path)
dirs.sort()
target = dirs[start_at]
print("starting with " + target)
time.sleep(8)
destination = "/var/archivematica/sharedDirectory/watchedDirectories/activeTransfers/standardTransfer/" + target
source = path + target
copy(source, destination)
# Update default config
# TODO pick correct processingMCP based on existence of access dir

shutil.copyfile(processing_available + "ChangedProcessingMCP.xml", destination + "/processingMCP.xml")
print("ready to start")
result = approve_transfer(target, url, api_key, user_name)
if result == 200:
    start_at = start_at + 1
    print("it was a good time, saving")
    with open(count_file, 'w') as f:
        print(start_at, file=f)

print("finished " + target + " at " + str(datetime.datetime.now()))
print(" ")
