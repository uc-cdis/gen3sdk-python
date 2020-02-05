"""
Module for indexing actions using sower job dispatcher

Attributes:
"""

import os
import requests
import json

from gen3.tools.manifest_indexing import manifest_indexing


def _download_file(url, filename):
    r = requests.get(url)
    with open(filename, "wb") as f:
        f.write(r.content)


if __name__ == "__main__":
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    input_data_json = json.loads(input_data)

    with open("/manifest-indexing-creds.json") as indexing_creds_file:
        indexing_creds = json.load(indexing_creds_file)

    auth = (
        indexing_creds.get("indexd_user", "gdcapi"),
        indexing_creds["indexd_password"],
    )

    filepath = "./manifest_tmp.tsv"
    _download_file(input_data_json["URL"], filepath)

    print("Start to index the manifest ...")

    host_url = input_data_json.get("host")
    if not host_url:
        host_url = "https://{}/index".format(hostname)

    manifest_indexing(
        filepath,
        host_url,
        input_data_json.get("thread_nums", 1),
        auth,
        input_data_json.get("prefix"),
        input_data_json.get("replace_urls"),
    )

    print("[out] {}".format(filepath))
