# -*- coding: utf-8 -*-

"""Enable METS to be consumed by the de-duplication application."""

from __future__ import print_function, unicode_literals
import logging
import sys

import lxml

import metsrw


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
        if mets_entry.type != "Item":
            continue
        objs = mets_entry.get_premis_objects()
        for obj in objs:
            entry = {}
            entry["date_modified"] = obj.date_created_by_application
            entry["filepath"] = filepath
            info.append(entry)
    return info
