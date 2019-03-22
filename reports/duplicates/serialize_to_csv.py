# -*- coding: utf-8 -*-

from pandas import DataFrame


class CSVOut:
    """Conveniently wrap CSV output capability."""

    @staticmethod
    def csv_out(duplicate_report, filename):
        """Output a CSV using Pandas and a bit of magic."""
        dupes = duplicate_report.get("manifest_data", {})
        cols = 0
        arr = [
            "file_path",
            "date_modified",
            "base_name",
            "dir_name",
            "package_name",
            "package_uuid",
        ]
        rows = []
        headers = None
        for key, value in dupes.items():
            cols = max(cols, len(value))
        # Create headers for our spreadsheet.
        headers = arr * cols
        for i in range(len(headers)):
            headers[i] = "{}_{}".format(headers[i], str(i).zfill(2))
        # Make sure that checksum is the first and only non-duplicated value.
        headers = ["Checksum"] + headers
        for key, value in dupes.items():
            records = []
            for prop in value:
                record = []
                record.append(prop.get("filepath", "NaN"))
                record.append(prop.get("date_modified", "NaN"))
                record.append(prop.get("basename", "NaN"))
                record.append(prop.get("dirname", "NaN"))
                record.append(prop.get("package_name", "NaN"))
                record.append(prop.get("package_uuid", "NaN"))
                records = records + record
            # Fill blank spaces in row. Might also be possible as a Pandas series.
            space = cols * len(arr) - len(records)
            if space:
                filler = ["NaN"] * space
                records = records + filler
            # Create a checksum entry for our spreadsheet.
            records = [key] + records
            # Create a dict from two lists.
            dictionary = dict(zip(headers, records))
            rows.append(dictionary)
        df = DataFrame(columns=headers)
        for entry in rows:
            df = df.append(entry, ignore_index=True)
            # Sort the columns in alphabetical order to pair similar headers.
            cols = sorted(df.columns.tolist())
            cols_no_suffix = [x.rsplit("_", 1)[0] for x in cols]
            df = df[cols]
        df.to_csv(filename, index=None, header=cols_no_suffix, encoding="utf8")
