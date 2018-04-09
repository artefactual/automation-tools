#!/bin/bash
set -eux

python /home/user/git/artefactual/automation-tools/transfers/reingest.py --processfromstorage --config /home/user/git/artefactual/automation-tools/transfers/reingestconfig.json
