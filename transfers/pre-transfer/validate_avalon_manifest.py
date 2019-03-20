#!/usr/bin/env python3
"""
Parse Avalon Media System manifest file and validate against
rules prior to ingest into Archivematica.
"""

import argparse
import collections
import csv
import os
import sys


class ManifestValidator(object):
    def __init__(self, source_folder):
        self.source_folder = source_folder
        self.index = dict()

    @staticmethod
    def check_admin_data(name, author):
        """
        Validate the administrative data line in an Avalon CSV.
        :param name: str: reference name
        :param author: str: reference author
        :return: bool: whether or not this is a valid line
        """
        return bool(name and author)

    @staticmethod
    def check_header_data(row):
        """
        Validate header data line in an Avalon CSV.
        :param row: list: metadata fields
        """
        all_headers = [
            "Bibliographic ID",
            "Bibliographic ID Label",
            "Other Identifier",
            "Other Identifier Type",
            "Title",
            "Creator",
            "Contributor",
            "Genre",
            "Publisher",
            "Date Created",
            "Date Issued",
            "Abstract",
            "Language",
            "Physical Description",
            "Related Item URL",
            "Related Item Label",
            "Topical Subject",
            "Geographic Subject",
            "Temporal Subject",
            "Terms of Use",
            "Table of Contents",
            "Statement of Responsibility",
            "Note",
            "Note Type",
            "Publish",
            "Hidden",
            "File",
            "Label",
            "Offset",
            "Skip Transcoding",
            "Absolute Location",
            "Date Ingested",
        ]
        req_headers = ["Title", "Date Issued", "File"]
        unique_headers = [
            "Bibliographic ID",
            "Bibliographic ID Label",
            "Title",
            "Date Created",
            "Date Issued",
            "Abstract",
            "Physical Description",
            "Terms of Use",
        ]
        collected_headers = collections.Counter(row).items()
        repeated_headers = [k for k, v in collected_headers if v > 1]

        for x in row:
            while "" in row:
                row.remove("")
            if x[0] == " " or x[-1] == " ":
                raise ValueError(
                    (
                        "Header fields cannot have leading or trailing blanks. Invalid field: "
                        + str(x)
                    )
                )

        if not (set(row).issubset(set(all_headers))):
            raise ValueError(
                "Manifest includes invalid metadata field(s). Invalid field(s): "
                + str(list(set(row) - set(all_headers)))
            )

        if any(x in repeated_headers for x in unique_headers):
            raise ValueError(
                "A non-repeatable header field is repeated. Repeating field(s): "
                + str(repeated_headers)
            )

        if not (all(x in row for x in req_headers)):
            if not "Bibliographic ID" in row:
                raise ValueError(
                    (
                        "One of the required headers is missing: Title, Date Issued, File."
                    )
                )
        return True

    @staticmethod
    def check_field_pairs(row):
        """
        Checks paired fields have associated pair.
        :param row: list: metadata fields
        """
        for i, field in enumerate(row):
            if field == "Other Identifier Type":
                if not all(
                    f in row for f in ["Other Identifier", "Other Identifier Type"]
                ):
                    raise ValueError(
                        ("Other Identifier Type field missing its required pair.")
                    )
            if field == "Related Item Label":
                if not all(
                    f in row for f in ["Related Item URL", "Related Item Label"]
                ):
                    raise ValueError(
                        ("Related Item Label field missing its required pair.")
                    )
            if field == "Note Type":
                if not all(f in row for f in ["Note", "Note Type"]):
                    raise ValueError(("Note Type field missing its required pair."))

    @staticmethod
    def get_file_columns(row):
        """
        Identify columns containing file data.
        :param row: list: metadata fields
        """
        columns = []
        for i, field in enumerate(row):
            if field == "File":
                columns.append(i)
        return columns

    @staticmethod
    def check_file_exts(row, file_cols):
        """
        Checks for only one period in filepath, unless specifying
        transcoded video quality.
        :param row: list: metadata fields
        :param file_cols: list: columns holding file data
        """
        for c in file_cols:
            if row[c].count(".") > 1 and not any(
                q in row[c] for q in [".high.", ".medium.", ".low"]
            ):
                raise ValueError(
                    ("Filepath " + row[c] + " contains" " more than one period.")
                )

    @staticmethod
    def get_op_columns(row):
        """
        Identify columns containing file data.
        :param row: list: metadata fields
        """
        columns = []
        for i, field in enumerate(row):
            if field == "Publish" or field == "Hidden":
                columns.append(i)
        return columns

    @staticmethod
    def check_op_fields(row, op_cols):
        """
        Checks that operational fields are marked only as "yes" or "no."
        :param row: list: metadata fields
        :param op_cols: list: columns holding operational field data
        """
        for c in op_cols:
            if row[c]:
                if not (row[c].lower() == "yes" or row[c].lower() == "no"):
                    raise ValueError(
                        (
                            "Publish/Hidden fields must have boolean value (yes or no). Value is "
                            + str(row[c])
                        )
                    )

    def validate_csv(self, manifest):
        """
        Validates Avalon Manifest file against predetermined rules. See
        https://wiki.dlib.indiana.edu/display/VarVideo/Batch+Ingest+Package+Format
        for official guidelines.
        :param manifest: csv object
        """
        with open(manifest, "rt") as csvf:
            csvr = csv.reader(csvf)
            for i, row in enumerate(csvr):
                if i == 0:
                    if not ManifestValidator.check_admin_data(row[0], row[1]):
                        raise ValueError(
                            (
                                "Administrative data must include"
                                " reference name and author."
                            )
                        )
                    print("Administrative data is OK")

                if i == 1:
                    if ManifestValidator.check_header_data(row):
                        print("Header data is OK")
                    file_cols = ManifestValidator.get_file_columns(row)
                    op_cols = ManifestValidator.get_op_columns(row)
                    ManifestValidator.check_field_pairs(row)

                if i >= 2:
                    ManifestValidator.check_file_exts(row, file_cols)
                    ManifestValidator.check_op_fields(row, op_cols)
            print("Manifest is valid!")

    def find_csv(self, source_folder):
        """
        Checks for CSV at top level of source folder and returns it.
        :param source_folder: absolute path for SIP
        """
        files = os.listdir(source_folder)
        paths = [fn for fn in files if fn.endswith(".csv")]
        if len(paths) and not len(paths) > 1:
            return source_folder + paths[0]
        raise ValueError("Manifest not found.")


def main(source_folder):
    manifest = ManifestValidator(source_folder)
    manifest.validate_csv(manifest.find_csv(source_folder))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_folder", help="Avalon Manifest file")
    args = parser.parse_args()
    sys.exit(main(args.source_folder))

