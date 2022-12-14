# Scripting Quickstart

* install the sdk: `pip install gen3`
* for authenticated access to a commons - download an API key from the portal's Profile page, and save it as `~/.gen3/credentials.json`
* write a script that uses the sdk (examples below)
* function calls that are configured with a backoff are retried 3 times by default. The number of retries can be customized by setting the `GEN3SDK_MAX_RETRIES` environment variable

## Quickstart Example - Object Index

The Gen3 object index (indexd) provides public read access
that does not require authentication.

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

## Quickstart Example w/ Auth - Modify Object Index

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


# Install n API Key downloaded from the 
# commons' "Profile" page at ~/.gen3/credentials.json


def main():
    auth = Gen3Auth()
    index = Gen3Index(auth.endpoint, auth_provider=auth)
    if not index.is_healthy():
        print(f"uh oh! The indexing service is not healthy in the commons {auth.endpoint}")
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

def main():
    auth = Gen3Auth(refresh_file="credentials.json")
    mds = Gen3Metadata(auth_provider=auth)

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
