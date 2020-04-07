# Gen3 SDK for Python

The Gen3 SDK for Python provides classes for handling the authentication flow using a refresh token and getting an access token from the commons. The access token is then refreshed as necessary while the refresh token remains valid. The submission client contains various functions for submitting, exporting, and deleting data from a Gen3 data commons.

Some docs for this SDK are available at [http://gen3sdk-python.rtfd.io/](http://gen3sdk-python.rtfd.io/)

**Table of Contents**

---

<!-- MarkdownTOC autolink=true -->

- [Auth](#auth)
- [IndexClient](#indexclient)
- [SubmissionClient](#submissionclient)
- [Metadata](#metadata)
- [Indexing Tools](#indexing-tools)
    - [Download Manifest](#download-manifest)
    - [Verify Manifest](#verify-manifest)
    - [Indexing Manifest](#indexing-manifest)
- [Metadata Tools](#metadata-tools)
    - [Ingest Manifest](#ingest-manifest)
    - [Searching Indexd to get GUID for Metadata Ingestion](#searching-indexd-to-get-guid-for-metadata-ingestion)
    - [Manifest Merge](#manifest-merge)
        - [Ideal Scenario \(Column to Column Match, Indexing:Metadata Manifest Rows\)](#ideal-scenario-column-to-column-match-indexingmetadata-manifest-rows)
        - [Non-Ideal Scenario \(Partial URL Matching\)](#non-ideal-scenario-partial-url-matching)

<!-- /MarkdownTOC -->

---

## Auth

This contains an auth wrapper for supporting JWT based authentication with `requests`. The access token is generated from the refresh token and is regenerated on expiration.

## IndexClient

This is the client for interacting with the Indexd service for GUID brokering and resolution.

## SubmissionClient

This is the client for interacting with the Gen3 submission service including GraphQL queries.

## Metadata

For interacting with Gen3's metadata service.

```python
import sys
import logging
import asyncio

from gen3.auth import Gen3Auth
from gen3.metadata import Gen3Metadata

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"

def main():
    auth = Gen3Auth(COMMONS, refresh_file="credentials.json")
    mds = Gen3Metadata(COMMONS, auth_provider=auth)

    if mds.is_healthy():
        print(mds.get_version())

        guid = "95a41871-444c-48ae-8004-63f4ed1f0691"
        metadata = {
            "foo": "bar",
            "fizz": "buzz",
            "nested_details": {
                "key1": "value1"
            }
        }
        mds.create(guid, metadata, overwrite=True)

        guids = mds.query("nested_details.key1=value1")

        print(guids)
        # >>> ['95a41871-444c-48ae-8004-63f4ed1f0691']

if __name__ == "__main__":
    main()
```


## Indexing Tools

### Download Manifest

How to download a manifest `object-manifest.csv` of all file objects in indexd for a given commons:

```python
import sys
import logging
import asyncio

from gen3.tools import indexing
from gen3.tools.indexing.verify_manifest import manifest_row_parsers

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        indexing.async_download_object_manifest(
            COMMONS,
            output_filename="object-manifest.csv",
            num_processes=8,
            max_concurrent_requests=24,
        )
    )


if __name__ == "__main__":
    main()

```

The output file will contain columns `guid, urls, authz, acl, md5, file_size, file_name` with info
populated from indexd.

### Verify Manifest

How to verify the file objects in indexd against a "source of truth" manifest.

> Bonus: How to override default parsing of manifest to match a different structure.

In the example below we assume a manifest named `alternate-manifest.csv` already exists
with info of what's expected in indexd. The headers in the `alternate-manifest.csv`
are `guid, urls, authz, acl, md5, size, file_name`.

> NOTE: The alternate manifest headers differ rfom the default headers described above (`file_size` doesn't exist and should be taken from `size`)

```python
import sys
import logging
import asyncio

from gen3.tools import indexing
from gen3.tools.indexing.verify_manifest import manifest_row_parsers

logging.basicConfig(filename="output.log", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"


def main():
    def _get_file_size(row):
        try:
            return int(row.get("size"))
        except Exception:
            logging.warning(f"could not convert this to an int: {row.get('size')}")
            return row.get("size")

    # override default parsers
    manifest_row_parsers["file_size"] = _get_file_size

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    loop.run_until_complete(
        indexing.async_verify_object_manifest(
            COMMONS, manifest_file="alternate-manifest.csv"
        )
    )


if __name__ == "__main__":
    main()
```

A more complex example is below. In this example:

* The input file is a tab-separated value file (instead of default CSV)
    * Note the `manifest_file_delimiter` argument
* The arrays in the file are represented with Python-like list syntax
    * ex: `['DEV', 'test']` for the `acl` column
* We are using more Python processes (20) to speed up the verify process
    * NOTE: You need to be careful about this, as indexd itself needs to support
            scaling to this number of concurrent requests coming in

```python
import sys
import logging

from gen3.tools import indexing
from gen3.tools.indexing.verify_manifest import manifest_row_parsers

logging.basicConfig(filename="output.log", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"


def main():
    def _get_file_size(row):
        try:
            return int(row.get("size"))
        except Exception:
            logging.warning(f"could not convert this to an int: {row.get('size')}")
            return row.get("size")

    def _get_acl_from_row(row):
        return [row.get("acl").strip().strip("[").strip("]").strip("'")]

    def _get_authz_from_row(row):
        return [row.get("authz").strip().strip("[").strip("]").strip("'")]

    def _get_urls_from_row(row):
        return [row.get("url").strip()]

    # override default parsers
    manifest_row_parsers["file_size"] = _get_file_size
    manifest_row_parsers["acl"] = _get_acl_from_row
    manifest_row_parsers["authz"] = _get_authz_from_row
    manifest_row_parsers["urls"] = _get_urls_from_row

    indexing.verify_object_manifest(
        COMMONS,
        manifest_file="output-manifest.csv",
        manifest_file_delimiter="\t",
        num_processes=20,
    )


if __name__ == "__main__":
    main()

```

### Indexing Manifest

The module for indexing object files in a manifest (against indexd's API).

The manifest format can be either tsv or csv. The fields that are lists (like acl, authz, and urls)
separate the values with commas or spaces (but you must use spaces if the file is a csv).
The field values can contain single quote, open bracket and the closed bracket. However, they will
be removed in the preprocessing step.

The following is an example of tsv manifest.
```
guid	md5	size	authz	acl	url
255e396f-f1f8-11e9-9a07-0a80fada099c	473d83400bc1bc9dc635e334faddf33c	363455714	/programs/DEV/project/test	['Open']	[s3://examplebucket/test1.raw]
255e396f-f1f8-11e9-9a07-0a80fada097c	473d83400bc1bc9dc635e334fadd433c	543434443	/programs/DEV/project/test	phs0001 phs0002	s3://examplebucket/test3.raw gs://examplebucket/test3.raw
255e396f-f1f8-11e9-9a07-0a80fada096c	473d83400bc1bc9dc635e334fadd433c	363455714	/programs/DEV/project/test	['phs0001', 'phs0002']	['s3://examplebucket/test4.raw', 'gs://examplebucket/test3.raw']
```

```python
import sys
import logging

from gen3.auth import Gen3Auth
from gen3.tools.indexing import index_object_manifest

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"
MANIFEST = "./example_manifest.tsv"


def main():
    auth = Gen3Auth(COMMONS, refresh_file="credentials.json")

    # use basic auth for admin privileges in indexd
    # auth = ("basic_auth_username", "basic_auth_password")

    index_object_manifest(
        commons_url=COMMONS,
        manifest_file=MANIFEST,
        thread_num=8,
        auth=auth,
        replace_urls=False,
        manifest_file_delimiter="\t" # put "," if the manifest is csv file
    )

if __name__ == "__main__":
    main()

```

## Metadata Tools

### Ingest Manifest

For populating the metadata service via a file filled with metadata. Uses asynchronous
calls for you.

The file provided must contain a "guid" column (or you can use a different column name or different logic entirely by providing a `guid_for_row` function)

The row contents can contain valid JSON and this script will correctly nest that JSON
in the resulting metadata.

```python
import sys
import logging
import asyncio

from gen3.auth import Gen3Auth
from gen3.tools import metadata
from gen3.tools.metadata.ingest_manifest import manifest_row_parsers

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"

# a file containing a "guid" column and additional, arbitrary columns to populate
# into the metadata service
MANIFEST = "dbgap_extract_guid.tsv"

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    auth = Gen3Auth(COMMONS, refresh_file="credentials.json")

    # must provide a str to namespace the metadata from the file in a block in
    # the metadata service
    metadata_source = "dbgap"

    # (optional) override default guid parsing behavior
    def _custom_get_guid_for_row(commons_url, row, lock):
        """
        Given a row from the manifest, return the guid to use for the metadata object.

        Args:
            commons_url (str): root domain for commons where mds lives
            row (dict): column_name:row_value
            lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
                connections if making a call to an external service

        Returns:
            str: guid
        """
        return row.get("guid") # OR row.get("some_other_column")

    # (optional) override default guid parsing behavior
    manifest_row_parsers["guid_for_row"] = _custom_get_guid_for_row

    loop.run_until_complete(
        metadata.async_ingest_metadata_manifest(
            COMMONS, manifest_file=MANIFEST, metadata_source=metadata_source, auth=auth
        )
    )

if __name__ == "__main__":
    main()

```

Example file:

```
guid    submitted_sample_id biosample_id    dbgap_sample_id sra_sample_id   submitted_subject_id    dbgap_subject_id    consent_code    consent_short_name  sex body_site   analyte_type    sample_use  repository  dbgap_status    sra_data_details    study_accession study_accession_with_consent    study_with_consent  study_subject_id
95a41871-222c-48ae-8004-63f4ed1f0691    NWD680715   SAMN04109058    1784155 SRS1361261  DBG00391    1360750 2   HMB-IRB-MDS female  Blood   DNA ["Seq_DNA_SNP_CNV"] TOPMed_WGS_Amish    Loaded  {"status": "public", "experiments": "1", "runs": "1", "bases": "135458977924", "size_Gb": "25", "experiment_type": "WGS", "platform": "ILLUMINA", "center": "UM-TOPMed"}    phs000956.v3.p1 phs000956.v3.p1.c2  phs000956.c2    phs000956.v3_DBG00391
```

Would result in the following metadata records in the metadata service:

```python
{
    _guid_type: "indexed_file_object",
    dbgap: {
        sex: "female",
        body_site: "Blood",
        repository: "TOPMed_WGS_Amish",
        sample_use: [
            "Seq_DNA_SNP_CNV"
        ],
        analyte_type: "DNA",
        biosample_id: "SAMN04109058",
        consent_code: 2,
        dbgap_status: "Loaded",
        sra_sample_id: "SRS1361261",
        dbgap_sample_id: 1784155,
        study_accession: "phs000956.v3.p1",
        dbgap_subject_id: 1360750,
        sra_data_details: {
            runs: "1",
            bases: "135458977924",
            center: "UM-TOPMed",
            status: "public",
            size_Gb: "25",
            platform: "ILLUMINA",
            experiments: "1",
            experiment_type: "WGS"
        },
        study_subject_id: "phs000956.v3_DBG00391",
        consent_short_name: "HMB-IRB-MDS",
        study_with_consent: "phs000956.c2",
        submitted_sample_id: "NWD680715",
        submitted_subject_id: "DBG00391",
        study_accession_with_consent: "phs000956.v3.p1.c2"
    }
}
```

> NOTE: `_guid_type` is populated automatically, depending on if the provided GUID exists in indexd or not. Either `indexed_file_object` or `metadata_object`.

### Searching Indexd to get GUID for Metadata Ingestion

It is possible to try and dynamically retrieve a GUID for a row in the manifest file
provided. However, this is limited by indexd's ability to scale to the queries you
want to run. Indexd's querying capabilities are limited and don't scale well with a
large volume of records (it is meant to be a key:value store much like the metadata service).

```python
import sys
import logging
import asyncio

from gen3.auth import Gen3Auth
from gen3.tools import metadata
from gen3.tools.metadata.ingest_manifest import manifest_row_parsers

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"
MANIFEST = "dbgap_extract.tsv"


def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    auth = Gen3Auth(COMMONS, refresh_file="credentials.json")

    # must provide a str to namespace the metadata from the file in a block in
    # the metadata service
    metadata_source = "dbgap"

    # (optional) override default indexd querying (NOTE: must be async)
    async def _custom_query_for_associated_indexd_record_guid(commons_url, row, lock):
        """
        Given a row from the manifest, return the guid for the related indexd record.

        WARNING: The query endpoint this uses in indexd is incredibly slow when there are
                 lots of indexd records.

        Args:
            commons_url (str): root domain for commons where mds lives
            row (dict): column_name:row_value
            lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
                connections

        Returns:
            str: guid or None
        """
        mapping = {"urls": "submitted_sample_id"}

        # special query endpoint for matching url patterns
        records = []
        if "urls" in mapping:
            pattern = row.get(mapping["urls"])
            logging.debug(
                f"trying to find matching record matching url pattern: {pattern}"
            )
            records = await metadata.async_query_urls_from_indexd(
                pattern, commons_url, lock
            )

        logging.debug(f"matching record(s): {records}")

        if len(records) > 1:
            msg = (
                "Multiple records were found with the given search criteria, this is assumed "
                "to be unintentional so the metadata will NOT be linked to these records:\n"
                f"{records}"
            )
            logging.warning(msg)
            records = []

        guid = None
        if len(records) == 1:
            guid = records[0].get("did")

        return guid

    # (optional) override default indexd querying
    manifest_row_parsers[
        "indexed_file_object_guid"
    ] = _custom_query_for_associated_indexd_record_guid

    loop.run_until_complete(
        # get_guid_from_file=False tells tool to try and get the guid using
        # the provided custom query function
        metadata.async_ingest_metadata_manifest(
            COMMONS,
            manifest_file=MANIFEST,
            metadata_source=metadata_source,
            auth=auth,
            get_guid_from_file=False,
        )
    )


if __name__ == "__main__":
    main()

```

Setting `get_guid_from_file`  to `False` tells tool to try and get the guid using
the provided custom query function instead of relying on a column in the manifest.

> NOTE: By default, the `indexed_file_object_guid` function attempts to query indexd URLs to pattern match
whatever is in the manifest column `submitted_sample_id`.