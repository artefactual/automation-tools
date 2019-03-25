# Duplicates

Duplicates can identify duplicate entries across AIPs across your entire AIP
store.

## Configuration

**Python**

The duplicates module has its own dependencies. To ensure it can run, please 
install these first: 

* `$ sudo pip install -r requirements.txt`

**Storage Service**

To configure your report, modify [config.json](config.json) with information
about how to connect to your Storage Service, e.g.
```json
{
	"storage_service_url": "http://127.0.0.1:62081",
	"storage_service_user": "test",
	"storage_service_api_key": "test"
}
```

## Running the script

Once configured there are a number of ways to run the script.

* **From the duplicates directory:** `$ python duplicates.py`
* **From the report folder as a module:** `$ python -m duplicates.duplicates`
* **From the automation-tools folder as a module:** `$ python -m reports.duplicates.duplicates`

## Output

The tool has two outputs:

* `aipstore-duplicates.json`
* `aipstore-duplicates.csv`

A description of those follows:

* **Json**: Which reports on the packages across which duplicates have been
found and lists duplicate objects organized by checksum. The output might be
useful for developers creating other tooling around this work, e.g.
visualizations, as json is an easy to manipulate standard in most programming
languages.

The json output is organised as follows:
```json
{
    "manifest_data": {
        "{matched-checksum-1}": [
            {
                "basename": "{filename}",
                "date_modified": "{modified-date}",
                "dirname": "{directory-name}",
                "filepath": "{relative-path}",
                "package_name": "{package-name}",
                "package_uuid": "{package-uuid}"
            },
            {
                "basename": "{filename}",
                "date_modified": "{modified-date}",
                "dirname": "{directory-name}",
                "filepath": "{relative-path}",
                "package_name": "{package-name}",
                "package_uuid": "{package-uuid}"
            },
            {
                "basename": "{filename}",
                "date_modified": "{modified-date}",
                "dirname": "{directory-name}",
                "filepath": "{relative-path}",
                "package_name": "{package-name}",
                "package_uuid": "{package-uuid}"
            }
        ],
        "{matched-checksum-2}": [
            {
                "basename": "{filename}",
                "date_modified": "{modified-date}",
                "dirname": "{directory-name}",
                "filepath": "{relative-path}",
                "package_name": "{package-name}",
                "package_uuid": "{package-uuid}"
            },
            {
                "basename": "{filename}",
                "date_modified": "{modified-date}",
                "dirname": "{directory-name}",
                "filepath": "{relative-path}",
                "package_name": "{package-name}",
                "package_uuid": "{package-uuid}"
            }
        ]
    },
    "packages": {
        "{package-uuid}": "{package-name}",
        "{package-uuid}": "{package-name}"
    }
}
```

* **CSV**: Which reports the same information but as a 2D representation. The
CSV is ready-made to be manipulated in tools such as
[OpenRefine](http://openrefine.org/). The CSV dynamically resizes depending on
where some rows have different numbers of duplicate files to report.

## Process followed

Much of the work done by this package relies on the
[amclient package](https://github.com/artefactual-labs/amclient). The process
used to create a report is as follows:

1. Retrieve a list of all AIPs across all pipelines.
2. For every AIP download the bag manifest for the AIP (all manifest
permutations are tested so all duplicates are discovered whether you are using
MD5, SHA1 or SHA256 in your Archivematica instances).
3. For every entry in the bag manifest record the checksum, package, and path.
4. Filter objects with matching checksums into a duplicates report.
5. For every matched file in the duplicates report download the package METS
file.
6. Using the METS file augment the report with date_modified information.
(Other data might be added in future).
7. Output the report as JSON to `aipstore-duplicates.json`.
8. Re-format the report to output in a 2D table to `aipstore-duplicates.csv`.

## Future work

As a standalone module, the duplicates work could be developed in a number of
ways that might be desirable in an archival appraisal workflow.
