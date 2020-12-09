import sys
import logging
import csv

from gen3.auth import Gen3Auth
from gen3.tools.bundle.ingest_manifest import (
    ingest_bundle_manifest,
    _verify_and_process_bundle_manifest,
)

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# COMMONS = "https://{{insert-commons-here}}/"
# MANIFEST = "./example_manifest.tsv"
COMMONS = "https://nci-crdc.datacommons.io/index/"
def main():
    # auth = Gen3Auth(COMMONS, refresh_file="credentials.json")

    # use basic auth for admin privileges in indexd
    # auth = ("basic_auth_username", "basic_auth_password")
    csv.field_size_limit(sys.maxsize)

    record, g = _verify_and_process_bundle_manifest(
        manifest_file="/home/binam/Downloads/new_new.csv",
        manifest_file_delimiter=",",
    )
    print(record)

if __name__ == "__main__":
    main()
