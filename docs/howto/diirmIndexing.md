## Indexing Tools

TOC
- [Download Manifest](#download-manifest)
- [Verify Manifest](#verify-manifest)
- [Indexing Manifest](#indexing-manifest)
- [Merge Bucket Manifests](#merge-bucket-manifests)
- [Validate Manifest Format](#validate-manifest-format)

### Download Manifest

How to download a manifest `object-manifest.csv` of all file objects in indexd for a given commons:

```python
import sys
import logging
import asyncio

from gen3.tools import indexing
from gen3.tools.indexing.verify_manifest import manifest_row_parsers
from gen3.utils import get_or_create_event_loop_for_thread

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"

def main():
    loop = get_or_create_event_loop_for_thread()


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

    loop = get_or_create_event_loop_for_thread()


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
from gen3.tools.indexing.index_manifest import index_object_manifest

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

MANIFEST = "./example_manifest.tsv"


def main():
    auth = Gen3Auth(refresh_file="credentials.json")

    # use basic auth for admin privileges in indexd
    # auth = ("basic_auth_username", "basic_auth_password")

    index_object_manifest(
        commons_url=auth.endpoint,
        manifest_file=MANIFEST,
        thread_num=8,
        auth=auth,
        replace_urls=False,
        manifest_file_delimiter="\t", # put "," if the manifest is csv file
        submit_additional_metadata_columns=False, # set to True to submit additional metadata to the metadata service
    )

if __name__ == "__main__":
    main()

```

### Merge Bucket Manifests

To merge bucket manifests contained in `/input_manifests` into one output manifest on the basis of `md5`:
```
import sys
import logging

from gen3.tools.indexing.merge_manifests import merge_bucket_manifests

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

INPUT_MANIFEST_DIRECTORY = "./input_manifests"
OUTPUT_MANIFEST = "merged-bucket-manifest.tsv"


def main():
    merge_bucket_manifests(
        directory=INPUT_MANIFEST_DIRECTORY,
        merge_column="md5",
        output_manifest=OUTPUT_MANIFEST,
    )


if __name__ == "__main__":
    main()
```

### Validate Manifest Format

`gen3.tools.indexing.is_valid_manifest_format` validates the contents of a
manifest of file objects and logs all errors found. Each logged error message
includes a description along with the line number and column in which the error
occurred. md5, size, url, and authz values can be validated.


`is_valid_manifest_format` can validate md5, size, url and authz values by
making use of the `MD5Validator`, `SizeValidator`, `URLValidator`, and
`AuthzValidator` classes defined in `gen3.tools.utils`,
respectively. See documentation in these `Validator` subclasses for more details
on how specific values are validated.

`is_valid_manifest_format` attempts to automatically map manifest column
names to `Validator` subclasses based on the `ALLOWED_COLUMN_NAMES` tuple
attribute defined in each `Validator` subclass.

The input manifest may contain extra columns that are not intended to be
validated, and columns can appear in any order.

Example:

```python
import sys
import logging

from gen3.tools.indexing import is_valid_manifest_format

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

MANIFEST = (
    "tests/validate_manifest_format/manifests/manifest_with_many_types_of_errors.tsv"
)


def main():

    is_valid_manifest_format(
        manifest_path=MANIFEST,
        column_names_to_enums=None,
        allowed_protocols=["s3", "gs"],
        allow_base64_encoded_md5=False,
        error_on_empty_url=False,
        line_limit=None,
    )


if __name__ == "__main__":
    main()
```

The script above logs the following output:
```
INFO:root:validating "tests/validate_manifest_format/manifests/manifest_with_many_types_of_errors.tsv" manifest
INFO:root:mapped manifest column "md5" to "MD5Validator" class instance
INFO:root:mapped manifest column "urls" to "URLValidator" class instance
INFO:root:mapped manifest column "file_size" to "SizeValidator" class instance
INFO:root:mapped manifest column "authz" to "AuthzValidator" class instance
ERROR:root:line 2, "authz" value "invalid_authz" is invalid, expecting authz resource in format "/<resource>/<subresource>/.../<subresource>"
ERROR:root:line 3, "file_size" value "invalid_int" is not an integer
ERROR:root:line 4, "md5" value "invalid_md5" is invalid, expecting 32 hexadecimal characters
ERROR:root:line 5, "urls" value "invalid_url" is invalid, expecting URL in format "<protocol>://<hostname>/<path>", with protocol being one of ['s3', 'gs']
INFO:root:finished validating "tests/validate_manifest_format/manifests/manifest_with_many_types_of_errors.tsv" manifest
```

`column_names_to_enums` can be used to identify custom column names:
```python
import sys
import logging

from gen3.tools.indexing import is_valid_manifest_format
from gen3.tools.utils import Columns

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

MANIFEST = (
    "tests/validate_manifest_format/manifests/manifest_with_custom_column_names.tsv"
)

COLUMN_NAMES_TO_ENUMS = {
    "md5_with_underscores": Columns.MD5,
    "file size with spaces": Columns.SIZE,
    "Urls With Caps": Columns.URL,
    "authz with special chars!@*&": Columns.AUTHZ,
}


def main():

    is_valid_manifest_format(
        manifest_path=MANIFEST,
        column_names_to_enums=COLUMN_NAMES_TO_ENUMS,
        allowed_protocols=["s3", "gs"],
        allow_base64_encoded_md5=False,
        error_on_empty_url=False,
        line_limit=None,
    )


if __name__ == "__main__":
    main()
```
Which logs the following output:
```
INFO:root:validating "tests/validate_manifest_format/manifests/manifest_with_custom_column_names.tsv" manifest
INFO:root:mapped manifest column "authz with special chars!@*&" to "AuthzValidator" class instance
INFO:root:mapped manifest column "file size with spaces" to "SizeValidator" class instance
INFO:root:mapped manifest column "md5_with_underscores" to "MD5Validator" class instance
INFO:root:mapped manifest column "Urls With Caps" to "URLValidator" class instance
ERROR:root:line 2, "authz with special chars!@*&" value "invalid_authz" is invalid, expecting authz resource in format "/<resource>/<subresource>/.../<subresource>"
ERROR:root:line 3, "file size with spaces" value "invalid_int" is not an integer
ERROR:root:line 4, "md5_with_underscores" value "invalid_md5" is invalid, expecting 32 hexadecimal characters
ERROR:root:line 5, "Urls With Caps" value "invalid_url" is invalid, expecting URL in format "<protocol>://<hostname>/<path>", with protocol being one of ['s3', 'gs']
INFO:root:finished validating "tests/validate_manifest_format/manifests/manifest_with_custom_column_names.tsv" manifest
```

To see more examples, take a look at this test case: [test_is_valid_manifest_format.py](../../tests/validate_manifest_format/test_is_valid_manifest_format.py)
