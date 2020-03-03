# Gen3 SDK for Python

The Gen3 SDK for Python provides classes for handling the authentication flow using a refresh token and getting an access token from the commons. The access token is then refreshed as necessary while the refresh token remains valid. The submission client contains various functions for submitting, exporting, and deleting data from a Gen3 data commons.

Docs for this SDK are available at [http://gen3sdk-python.rtfd.io/](http://gen3sdk-python.rtfd.io/)

## Auth

This contains an auth wrapper for supporting JWT based authentication with `requests`. The access token is generated from the refresh token and is regenerated on expiration.

## IndexClient

This is the client for interacting with the Indexd service for GUID brokering and resolution.

## SubmissionClient

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

The output file will contain columns `guid, urls, authz, acl, md5, file_size` with info
populated from indexd.

### Verify Manifest

How to verify the file objects in indexd against a "source of truth" manifest.

> Bonus: How to override default parsing of manifest to match a different structure.

In the example below we assume a manifest named `alternate-manifest.csv` already exists
with info of what's expected in indexd. The headers in the `alternate-manifest.csv`
are `guid, urls, authz, acl, md5, size`.

> NOTE: The alternate manifest headers differ rfom the default headers described above (`file_size` doesn't exist and should be taken from `size`)

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

    # override default parsers
    manifest_row_parsers["file_size"] = _get_file_size

    indexing.verify_object_manifest(
        COMMONS, manifest_file="alternate-manifest.csv", num_processes=20
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
separate the values with commas or spaces.
The field values can contain single quote, open bracket and the closed bracket. However, they will 
be removed in the preprocessing step.

The following is an example of tsv manifest.
```
guid	md5	size	authz   acl	url
255e396f-f1f8-11e9-9a07-0a80fada099c	473d83400bc1bc9dc635e334faddf33c	363455714	/program/DEV/project/test   'Open']	[s3://pdcdatastore/test1.raw]
255e396f-f1f8-11e9-9a07-0a80fada097c	473d83400bc1bc9dc635e334fadd433c	543434443	/program/DEV/project/test   phs0001 phs0002	s3://pdcdatastore/test3.raw
255e396f-f1f8-11e9-9a07-0a80fada096c	473d83400bc1bc9dc635e334fadd433c	363455714	/program/DEV/project/test   ['phs0001', 'phs0002']	['s3://pdcdatastore/test4.raw']
```

```
import sys
import logging

from gen3.index import Gen3Index
from gen3.tools import indexing

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"
MANIFEST = "./example_manifest.tsv"

def main():
    indexing.manifest_indexing(
        common_url=COMMONS,
        manifest=MANIFEST,
        thread_num=8,
        api_key="./credentials.json",
        #auth = "admin,admin", # comment the above line and un-comment this line if basic auth is used
        replace_urls=False,
        dem="\t" # put "," if the manifest is csv file
    )

if __name__ == "__main__":
    main()

```