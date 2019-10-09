# -*- coding: utf-8 -*-

import logging

from pandas import DataFrame

try:
    from . import utils
except ImportError:
    import utils


logger = logging.getLogger("accruals")


class CSVException(Exception):
    """Exception to return if there is a problem generating the report."""


class CSVOut:
    """Conveniently wrap CSV output capability."""

    @staticmethod
    def output_reports(
        aip_index, transfers, dupe_reports, near_reports, no_match_reports
    ):
        CSVOut.stat_manifests(aip_index, transfers)
        CSVOut.dupe_csv_out(dupe_reports, "")
        CSVOut.near_csv_out(near_reports, "")
        CSVOut.no_match_csv_out(no_match_reports, "")

    @staticmethod
    def stat_manifests(aip_index, transfers):
        """Output some statistics about the transfer."""
        SUMMARY_FILE = "accruals_aip_store_summary.json"
        MANIFEST_DATA = "manifest_data"
        PACKAGES = "packages"
        summary = {}
        aipcount = 0
        aipdict = aip_index.get(MANIFEST_DATA)
        keys = aipdict.keys()
        for key in keys:
            aipcount += len(aipdict.get(key, []))
        number_of_packages = len(aip_index.get(PACKAGES, {}).keys())
        summary["count_of_files_across_aips"] = aipcount
        summary["number_of_aips"] = number_of_packages
        logger.info(
            "Number of files in '%s' AIPs in the AIP store: %s",
            number_of_packages,
            aipcount,
        )
        summary["numer_of_transfers"] = len(transfers)
        logger.info("Number of transfers: %s", len(transfers))
        for no, transfer in enumerate(transfers, 1):
            summary["files_in_transfer-{}".format(no)] = len(transfer)
            logger.info("Number of items in transfer %s: %s", no, len(transfer))
        print(utils.json_pretty_print(summary))
        with open(SUMMARY_FILE, "w") as summary_file:
            summary_file.write(utils.json_pretty_print(summary))

    @staticmethod
    def dupe_csv_out(duplicate_report, filename):
        """Copy of the original csv_out as we understand where this code is
        going.
        """
        accrual_comparison_csv = "true_duplicates_comparison.csv"
        if not duplicate_report:
            with open(accrual_comparison_csv, "w") as no_report:
                no_report.write(
                    "No true duplicates detected between accruals and AIPs\n"
                )
            return
        cols = [
            "keep",
            "path",
            "in_transfer_name",
            "hash",
            "modified_date",
            "already_in_package",
        ]
        csv = []
        for transfer in duplicate_report:
            transfer_name = transfer.keys()
            if len(transfer_name) > 1:
                raise CSVException(
                    "Too many keys to deal with: {}".format(transfer_name)
                )
            row_data = transfer.get(list(transfer_name)[0], {})
            for datum in row_data:
                row = []
                row.append("")
                row.append(datum.filepath)
                row.append(list(transfer_name)[0])
                hash_ = list(datum.hashes.keys())[0]
                row.append("{} ({})".format(hash_, datum.hashes[hash_]))
                row.append(datum.date_modified)
                row.append(datum.package_name)
                csv.append(row)
        df = DataFrame(csv, columns=cols)
        df.sort_values(by=["in_transfer_name"])
        logger.info("Outputting report to: %s", accrual_comparison_csv)
        df.to_csv(accrual_comparison_csv, index=None, header=True, encoding="utf8")

    @staticmethod
    def near_csv_out(near_report, filename):
        """Create a report of near duplicates. Non-matches will have their
        own report.
        """
        accrual_comparison_csv = "near_matches_comparison.csv"
        if not near_report:
            with open(accrual_comparison_csv, "w") as no_report:
                no_report.write("No near matches detected between accruals and AIPs\n")
            return
        cols = [
            "keep",
            "path",
            "in_transfer_name",
            "hash",
            "modified_date",
            "has_similar_in_package",
            "package_file_path",
            "match_basis",
        ]
        csv = []
        for transfer in near_report:
            for transfer_name, transfer_items in transfer.items():
                for transfer_item in transfer_items:
                    row = []
                    row.append("")
                    row.append(transfer_item[0].filepath)
                    row.append(transfer_name)
                    hash_ = list(transfer_item[0].hashes.keys())[0]
                    row.append("{} ({})".format(hash_, transfer_item[0].hashes[hash_]))
                    row.append(transfer_item[0].date_modified)
                    row.append(transfer_item[1].package_name)
                    row.append(transfer_item[1].filepath)
                    row.append(transfer_item[0] % transfer_item[1])
                    csv.append(row)
        df = DataFrame(csv, columns=cols)
        df.sort_values(by=["in_transfer_name"])
        logger.info("Outputting report to: %s", accrual_comparison_csv)
        df.to_csv(accrual_comparison_csv, index=None, header=True, encoding="utf8")

    @staticmethod
    def no_match_csv_out(no_match_report, filename):
        """Create a report of non-matches."""
        accrual_comparison_csv = "non_matches_list.csv"
        if not no_match_report:
            with open(accrual_comparison_csv, "w") as no_report:
                no_report.write("No non-matches between accruals and AIPs\n")
            return
        cols = ["keep", "path", "in_transfer_name", "hash", "modified_date", "is_new"]
        csv = []
        for transfer in no_match_report:
            for transfer_name, transfer_items in transfer.items():
                for transfer_item in transfer_items:
                    row = []
                    row.append("yes")
                    row.append(transfer_item.filepath)
                    row.append(transfer_name)
                    hash_ = list(transfer_item.hashes.keys())[0]
                    row.append("{} ({})".format(hash_, transfer_item.hashes[hash_]))
                    row.append(transfer_item.date_modified)
                    row.append("True")
                    csv.append(row)
        df = DataFrame(csv, columns=cols)
        df.sort_values(by=["in_transfer_name"])
        logger.info("Outputting report to: %s", accrual_comparison_csv)
        df.to_csv(accrual_comparison_csv, index=None, header=True, encoding="utf8")
