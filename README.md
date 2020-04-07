# Gen3 SDK for Python

The Gen3 PSDK for Python provides classes and functions for handling common tasks when interacting with a Gen3 commons. The API for a commons can be overwhelming, so this SDK aims
to simplify communication with various microservices in a clear Python package.

The docs here contain general descriptions of the different pieces of the SDK and example scripts. For detailed API documentation, see the link below:

* [Detailed API Documentation](https://uc-cdis.github.io/gen3sdk-python/_build/html/index.html)

## Table of Contents

- [Installation](#installation)
- [Quickstart Example](#quickstart-example)
- [Quickstart Example w/ Auth](#quickstart-example-w-auth)
- [Available Classes](#available-classes)
    - [Gen3Auth](#gen3auth)
    - [Gen3Index](#gen3index)
    - [Gen3Submission](#gen3submission)
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

---

## Installation

To get the latest released version of the SDK:

`pip install gen3`

To use the latest code in this repo you can clone this and then run:

`python setup.py install`

> Developer Note: If you want to edit this SDK and test it you can do a development install with `python setup.py develop`.

## Quickstart Example

```python
"""
This script will use an instance of the Gen3Index class to communicate with a Gen3
Commons indexing service to get some basic information.

The example commons we're using is an open Canine Data Commons.
"""
from gen3.index import Gen3Index

# Gen3 Commons URL
COMMONS = "https://caninedc.org/"


def main():
    index = Gen3Index(COMMONS)
    if not index.is_healthy():
        print(f"uh oh! The indexing service is not healthy in the commons {COMMONS}")
        exit()

    print("some file stats:")
    print(index.get_stats())

    print("example GUID record:")
    print(index.get(guid="afea506a-62d0-4e8e-9388-19d3c5ac52be"))


if __name__ == "__main__":
    main()

```

## Quickstart Example w/ Auth

Some Gen3 API endpoints require authentication and special privileges to be able to use. The SDK can automate a lot of this by simply providing it with an API Key you download from the Gen3 Commons UI after logging in.

> NOTE: The below script will most likely fail for you because your user doesn't have access to create in that commons. However, the example is still important because if you *did* have access, this would handle passing your access token to the commons API correctly.

```python
"""
This script will use an instance of the Gen3Index class to attempt to create a
new indexed file record in the specified Gen3 Commons indexing service.

The example commons we're using is an open Canine Data Commons.
"""
from gen3.index import Gen3Index
from gen3.auth import Gen3Auth

# Gen3 Commons URL
COMMONS = "https://caninedc.org/"

# An API Key downloaded from the above commons' "Profile" page
API_KEY_FILEPATH = "credentials.json"


def main():
    auth = Gen3Auth(COMMONS, refresh_file=API_KEY_FILEPATH)
    index = Gen3Index(COMMONS, auth_provider=auth)
    if not index.is_healthy():
        print(f"uh oh! The indexing service is not healthy in the commons {COMMONS}")
        exit()

    print("trying to create new indexed file object record:\n")
    try:
        response = index.create_record(
            hashes={"md5": "ab167e49d25b488939b1ede42752458b"}, size=42, acl=["*"]
        )
    except Exception as exc:
        print(
            "\nERROR ocurred when trying to create the record, you probably don't have access."
        )


if __name__ == "__main__":
    main()

```

## Available Classes

### Gen3Auth

This contains an auth wrapper for supporting JWT based authentication with `requests`. The access token is generated from the refresh token and is regenerated on expiration.

### Gen3Index

This is the client for interacting with the Indexd service for GUID brokering and resolution.

### Gen3Submission

This is the client for interacting with the Gen3 submission service including GraphQL queries.

## Indexing Tools

### Download Manifest

How to download a manifest `object-manifest.csv` of all file objects in indexd for a given commons:

```
import sys
import logging
import asyncio

from gen3.index import Gen3Index
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

```
import sys
import logging
import asyncio

from gen3.index import Gen3Index
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

```
import sys
import logging

from gen3.index import Gen3Index
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

```
import sys
import logging

from gen3.auth import Gen3Auth
from gen3.index import Gen3Index
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
