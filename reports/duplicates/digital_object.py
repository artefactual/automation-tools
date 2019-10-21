#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Digital object class to help with matching objects across bags."""

import json
import os
import time

try:
    from . import hashutils
except (ValueError, ImportError):
    import hashutils


class DigitalObjectException(Exception):
    """If there's a problem raise this."""


class DigitalObject:
    """Digital object class which allows us to represent files as objects in
    a way that makes them easy to compare for attributes which determine
    whether they are duplicates.
    """

    # Object members.
    basename = None
    date_modified = None
    dirname = None
    filepath = None
    hashes = None
    package_uuid = None
    package_name = None

    # This string eases our comparison to bag objects.
    DATA_OBJ_PATH_FOR_COMPARISON = os.path.join("data", "objects")

    def __init__(self, path=None, transfer_path=None):
        """Populate the digital object metadata. If we don't supply a path
        we'll just return an empty object to be populated on our own terms.
        """
        if not path:
            self.basename = None
            self.date_modified = None
            self.dirname = None
            self.filepath = None
            self.hashes = []
            self.package_uuid = None
            self.package_name = None
            self.flag = False
        if path:
            if not transfer_path:
                raise DigitalObjectException("Transfer path isn't set")
            self.filepath = path
            # Create a comparison path to compare directly to bag objects.
            self.comparison_path = path.replace(
                transfer_path, self.DATA_OBJ_PATH_FOR_COMPARISON
            )
            self.set_basename(self.comparison_path)
            self.set_dirname(self.comparison_path)
            self.hashes = hashutils.hash_(path)
            self.date_modified = self.get_timestamp(path)
            self.flag = False

    def set_basename(self, path):
        """Setter to enable basename to be updated. Basename is otherwise
        described as the filename including file extension.
        """
        self.basename = os.path.basename(path)

    def set_dirname(self, path):
        """Setter to enable directory name to be updated."""
        self.dirname = os.path.dirname(path)

    def as_dict(self):
        """Enable return of digital object as a dictionary for consumption."""
        return self.__dict__

    def __str__(self):
        """Let's override this!"""
        return json.dumps(
            self.__dict__, sort_keys=True, indent=4, separators=(",", ": ")
        )

    def __eq__(self, other):
        """Comparison operator for the digital object class. If two hashes
        match, and the given file path, we will return True.
        """
        ret = False
        for key in self.hashes.keys():
            if key in other.hashes.keys():
                ret = True
                break
        # TODO: we don't want to break this comparison, now we use a comparison
        # path and filepath and the semantics may not be clear enough.
        if self.comparison_path != other.filepath:
            ret = False
        if self.date_modified != other.date_modified:
            ret = False
        return ret

    def __mod__(self, other):
        """Modulo operator for the digital object class. If two hashes match,
        and the given file-path, then return zero. If there is any partial
        match, then return basis information. % is potentially useful for
        debugging, or enhanced reporting.
        """
        if self.__eq__(other):
            return 0
        # ret is False, repurpose to return basis information.
        ret = ""
        for key in self.hashes.keys():
            if key in other.hashes.keys():
                msg = "checksum match"
                ret = self.__concat_basis__(ret, msg)
                break
        if self.date_modified == other.date_modified:
            msg = "date modified match"
            ret = self.__concat_basis__(ret, msg)
        if self.basename == other.basename:
            msg = "filename match"
            ret = self.__concat_basis__(ret, msg)
        if self.dirname == other.dirname:
            msg = "directory name match"
            ret = self.__concat_basis__(ret, msg)
        if not ret:
            return "No matching components"
        return ret

    def __ne__(self, other):
        """Not equals operator to return where two digital objects do not
        match each other.

        NB. Between modulo (%) and (!=) there isn't a lot of consistency so we
        might need to select one or the other based on what is most useful,
        i.e. if modulo is better for difference, choose that. Otherwise,
        choose something other than an operator for comparison. Not as
        elegant looking, but it does the trick.
        """
        if self.__eq__(other):
            return 0
        ret = ""
        hash_ = False
        for key in self.hashes.keys():
            if key in other.hashes.keys():
                hash_ = True
                break
        if not hash_:
            msg = "checksums do not match"
            ret = self.__contact_basis__(ret, msg)
        if self.date_modified != other.date_modified:
            msg = "modified dates do not match"
            ret = self.__concat_basis__(ret, msg)
        if self.basename != other.basename:
            msg = "filenames do not match"
            ret = self.__concat_basis__(ret, msg)
        if self.dirname != other.dirname:
            msg = "directory names do not match"
            ret = self.__concat_basis__(ret, msg)
        if not ret:
            return "All components match"
        return ret

    @staticmethod
    def __concat_basis__(ret, msg):
        """Helper function to bring basis information together usefully."""
        if ret:
            return "{}; {}".format(ret, msg)
        return msg

    @staticmethod
    def get_timestamp(path):
        """Retrieve modified time from a file. Format it, and return it."""
        return time.strftime("%Y-%m-%d", time.localtime(os.path.getmtime(path)))
