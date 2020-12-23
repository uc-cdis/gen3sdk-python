## Bundle Tools

TOC
- [Ingest Manifest](#ingest-manifest)
    - [Example Bundle](#example-bundle)

### Ingest Manifest

The only required columns are `bundle_name` and `ids`(list of object or bundles). The order of bundles in the manifest matters: you can reference a `bundle_name` in the `ids` list if that bundle_name appears before the bundle containing it. Lowest level bundles should only contain GUIDs to File Objects and should live at the top of the manifest. If a GUID is not provided for a bundle, indexd will assign a GUID for the bundle. Within the manifest `bundle_names` is to be used as a unique identifier.

#### Example Bundle:
To create the following bundle:
```
Bundle A
    +- BundleB
        +- BundleC
            +-File1
            +-File2
    +-File3
```
the manifest should look like this
```
bundle_name,ids,GUID(optional) // can also include other optional fields defined by the DRS spec
BundleC,[File1-GUID File2-GUID],
BundleB,[File3-GUID BundleC],
BundleA, [BundleB BundleC],
```

The following is an example csv manifest:
```
bundle_name,ids,GUID,size,type,checksum,description
A,[dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2],,,,,some description
B,['dg.TEST/1e9d3103-cbe2-4c39-917c-b3abad4750d2' 'dg.TEST/f2a39f98-6ae1-48a5-8d48-825a0c52a22b'],,789,,,something
C,[A 'B' dg.TEST/ed8f4658-6acd-4f96-9dd8-3709890c959e],,120,,,lalala
D,[A B C],,,[md5 sha256],[1234567 abc12345],
E,[A B],dg.xxxx/590ee63d-2790-477a-bbf8-d53873ca4933,,md5 sha256,abcdefg abcd123,
```
NOTE: DrsObjects/Bundles support multiple checksums so in the manifest define type and the hash respectively.

Example:
```
type,checksum
md5 sha256, abcde 12345
```
The above manifest would result to the following `checksums` field in the bundle:
```json
"checksums":[{"type": "md5", "checksum": "abcde"}, {"type": "sha256", "checksum": "12345"}]
```

```python
import sys
import logging

from gen3.auth import Gen3Auth
from gen3.tools.bundle.ingest_manifest import ingest_bundle_manifest

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

COMMONS = "https://{{insert-commons-here}}/"
MANIFEST = "./example_manifest.tsv"

def main():
    auth = Gen3Auth(COMMONS, refresh_file="credentials.json")

    # use basic auth for admin privileges in indexd
    # auth = ("basic_auth_username", "basic_auth_password")

    ingest_bundle_manifest(
        commons_url=COMMONS,
        manifest_file=MANIFEST,
        out_manifest_file="ingest_out.csv",
        auth=auth,
    )

if __name__ == "__main__":
    main()
```
