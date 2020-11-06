import aiohttp
import asyncio
import csv
import json
import logging
import os
import time
import urllib.parse
import uuid
import re
import sys
import traceback

from drsclient.client import DrsClient
from gen3.auth import Gen3Auth
from gen3.tools.indexing.index_manifest import _standardize_str
from gen3.utils import UUID_FORMAT, SIZE_FORMAT
from gen3.tools.indexing.manifest_columns import (
    GUID_COLUMN_NAMES,
    SIZE_COLUMN_NAMES,
    BUNDLENAME_COLUMN_NAME,
    IDS_COLUMN_NAME,
    DESCRIPTION_COLUMN_NAME,
    CHECKSUMS_COLUMN_NAME,
    ALIASES_COLUMN_NAME,
)

"""
NOTES:
- Assumption is that the user creates the manifest with the correct hierarchy. 

Bundle manifest format

bundle_name, ids, bundle_guid (optional) // can also include other optional fields
Bundle-B, [File-1-GUID, File-2-GUID],
Bundle-C, [File-3-GUID, File-4-GUID],
Bundle-A, [Bundle-B, Bundle-C],

json representaion:
[ 
  {
    "Bundle_name": "Bundle-B",
    "Ids": [ "File-1-GUID", "File-2-GUID" ],
     // optional fields can also be added
  },
  {
     "Bundle_name": "Bundle-C"
     "Ids": [ "File-3-GUID", "File-4-GUID"]
  },
  {
     "Bundle_name": "Bundle-A"
     "Ids": [ "Bundle-B, "Bundle-C"]
  }
]
"""


def _verify_format(s, format):
    """
    Make sure the input is in the right format
    """
    r = re.compile(format)
    if r.match(s) is not None:
        return True
    return False


def _standardize_str(s):
    """
    Remove unnecessary spaces, commas

    Ex. "abc    d" -> "abc d"
        "abc, d" -> "abc d"
    """
    memory = []
    s = s.replace(",", " ")
    res = ""
    for c in s:
        if c != " ":
            res += c
            memory = []
        elif not memory:
            res += c
            memory.append(" ")
    return res


def _replace_bundle_name_with_guid(bundles, bundle_name_to_guid):
    """
    replace the bundle names in the list of bundles with its associated guid
    """
    new_list = []
    for bundle in bundles:
        if _verify_format(bundle, UUID_FORMAT):
            new_list.append(bundle)
        else:
            if bundle in bundle_name_to_guid:
                new_list.append(bundle_name_to_guid[bundle])
    return new_list


def _verify_and_process_bundle_manifest(manifest_file, manifest_file_delimiter="\t"):
    """
    Verify the manifest and create list of all records that needs to be ingested

    Args:
        manifest_file(str): the path to the input manifest
        manifest_file_delimiter(str): delimiter
    
    Returns:
        record(dict): Valid bundle record to use in POST /bundle
        records(list): List of all records to be ingested
    NOTE: we dont do a check if indexd contains these ids since it checks that in POST /bundle
    """
    pass_verification = True
    records = []
    bundle_name_to_guid = {}  # dict containing bundle_name : bundle_guid association
    with open(manifest_file, encoding="utf-8-sig") as manifest:
        reader = csv.DictReader(manifest, delimiter=manifest_file_delimiter)
        row_n = 1
        for row_n, row in enumerate(reader, 1):
            record = {}
            for key, value in row.items():
                if key in BUNDLENAME_COLUMN_NAME:
                    # make sure bundle_name exists. bundle_name is a required field.
                    if value:
                        bundle_name = value
                        record["name"] = bundle_name
                        if bundle_name not in bundle_name_to_guid:
                            bundle_name_to_guid[
                                bundle_name
                            ] = ""  # to keep track of the available bundles.
                        else:
                            logging.error(
                                "ERROR: bundle_name {} at row {} is not unique".format(
                                    bundle_name, row_n
                                )
                            )
                    else:
                        logging.error(
                            "ERROR: row {} does not contain bundle_name. bundle_name is required".format(
                                row_n
                            )
                        )
                        pass_verification = False
                elif key in IDS_COLUMN_NAME:
                    standard = (
                        _standardize_str(value)
                        .strip()
                        .lstrip("[")
                        .rstrip("]")
                        .split(" ")
                    )
                    item_ids = []
                    for item in standard:
                        item_id = item.replace("'", "").replace('"', "")
                        if item_id == record["name"]:
                            logging.error(
                                "Error: Bundle at row {} contains itself".format(row_n)
                            )
                            pass_verification = False
                        elif (
                            _verify_format(item_id, UUID_FORMAT)
                            or item_id in bundle_name_to_guid
                        ):  # so if its a bundle name thats in the dictionary then keep it for processing it later
                            item_ids.append(item_id)
                        else:
                            logging.error(
                                "ERROR: {} {} at row {} must either be a UUID or reference another bundle in this manifest".format(
                                    key, bundle, row_n
                                )
                            )
                            pass_verification = False
                    record["bundles"] = item_ids
                elif key in GUID_COLUMN_NAMES and value:
                    if _verify_format(value, UUID_FORMAT):
                        record["guid"] = value
                    else:
                        logging.error(
                            "ERROR: {} {} at row {} is in an incorrect format".format(
                                key, value, row_n
                            )
                        )
                elif key in CHECKSUMS_COLUMN_NAME or key in ALIASES_COLUMN_NAME:
                    standard = (
                        _standardize_str(value)
                        .strip()
                        .lstrip("[")
                        .rstrip("]")
                        .split(" ")
                    )
                    values = []
                    for element in standard:
                        v = element.strip().replace("'", "").replace('"', "")
                        if v:
                            values.append(v)
                    record[key] = values
                # Add all the other optional fields
                elif value:
                    if key in SIZE_COLUMN_NAMES:
                        value = int(value)
                    record[key] = value
            records.append(record)
    if not pass_verification:
        logging.error("The manifest is not in the correct format!")
        return None, None
    return records, bundle_name_to_guid


def _write_csv(records, filename="output_manifest.csv"):
    fieldnames = [
        "name",
        "bundles",
        "guid",
        "size",
        "checksums",
        "description",
        "version",
        "aliases",
    ]
    with open(filename, mode="w") as outfile:
        writer = csv.DictWriter(outfile, delimiter=",", fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            writer.writerow(r)

    return filename


def ingest_bundle_manifest(
    commons_url,
    manifest_file,
    out_manifest_file="ingest_out.csv",
    manifest_file_delimiter=None,
    auth=None,
    token=None,
):
    logging.info("Starting the process ....")
    start_time = time.perf_counter()
    logging.info("start time: {}".format(start_time))
    commons_url = commons_url.strip("/")

    if not manifest_file_delimiter:
        file_ext = os.path.splitext(manifest_file)
        if file_ext[-1].lower() == ".tsv":
            manifest_file_delimiter = "\t"
        else:
            # default, assume CSV
            manifest_file_delimiter = ","

    try:
        records, bundle_name_to_guid = _verify_and_process_bundle_manifest(
            manifest_file, manifest_file_delimiter
        )
    except Exception as e:
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        logging.error("Can not read {}. Detail {}".format(manifest_file, e))
        return None

    if not records:
        return None

    drsclient = DrsClient(commons_url, auth=auth, token=token)
    total = len(records)
    # Iterate through the records list and post to indexd
    for start, record in enumerate(records, 1):
        bundle_name = record["name"]

        logging.info("Posting bundle {} of {}".format(start, total))

        # Check the bundle list to make sure they're all guids
        record["bundles"] = _replace_bundle_name_with_guid(
            record["bundles"], bundle_name_to_guid
        )
        resp = drsclient.create(**record)

        if resp.status_code != 200:
            logging.error(
                "Failed to create bundle {}. Status code: {} Detail: {}".format(
                    bundle_name, resp.status_code, resp.text
                )
            )
        else:
            rec = resp.json()
            guid = rec["bundle_id"]
            logging.info("Successfully created bundle {}".format(bundle_name))

            resp = drsclient.get(guid, expand=False)
            rec = resp.json()

            if resp.status_code != 200:
                logging.error(
                    "Failed to get bundle {} with guid {}".format(bundle_name, guid)
                )
            else:
                logging.info(
                    "Sucessfully received bundle {} with guid {}".format(
                        bundle_name, guid
                    )
                )
                # Associate bundle_name to its guid created by indexd
                bundle_name_to_guid[bundle_name] = guid
                if "guid" not in record:
                    record["guid"] = guid
                if "size" not in record:
                    record["size"] = rec["size"]

    logging.info("Published all {} bundles".format(total))
    logging.info("Start writng output manifest . . .")

    _write_csv(records)

    logging.info("Output csv created")
    logging.info("Done!")

    return records


if __name__ == "__main__":
    logging.basicConfig(filename="ingest_bundle_manifest.log", level=logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    ingest_bundle_manifest()
