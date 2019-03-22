# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals
import lxml
import logging
import metsrw
import sys


def _load_mets(mets_file):
    """Read the METS file at the path provided."""
    try:
        mets = metsrw.METSDocument.fromfile(mets_file)  # Reads a file
        return mets
    except lxml.etree.XMLSyntaxError as e:
        logging.error("METS %s", e)
        sys.exit(1)
    except IOError as e:
        logging.error("File does not exist %s", e)
        sys.exit(1)


def read_premis_data(mets_file):
    """Read PREMIS information and retrieve information of some utility."""
    mets = _load_mets(mets_file)
    info = []
    for mets_entry in mets.all_files():
        filepath = mets_entry.path
        try:
            objs = mets_entry.get_premis_objects()
        except IndexError:
            continue
        for obj in objs:
            entry = {}
            entry["date_modified"] = obj.date_created_by_application
            entry["filepath"] = filepath
            info.append(entry)
    return info
