#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
import lxml
import logging
import metsrw
import sys


def _load_mets(filename):
    try:
        mets = metsrw.METSDocument.fromfile(filename)  # Reads a file
        return mets
    except lxml.etree.XMLSyntaxError as e:
        logging.error("METS %s", e)
        sys.exit(1)
    except IOError as e:
        logging.error("File does not exist %s", e)
        sys.exit(1)


def read_premis_data(mets_file):
    mets = _load_mets(mets_file)
    info = []
    for entry in mets.all_files():
        filepath = entry.path
        objs = entry.get_premis_objects()
        for obj in objs:
            entry = {}
            entry["date_modified"] = obj.date_created_by_application
            entry["filepath"] = filepath
            info.append(entry)
    return info
