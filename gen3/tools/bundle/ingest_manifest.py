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

# Pre-defined supported column names
GUID = ["guid", "GUID"]
BUNDLENAME = ["bundle_name"]
IDS = ["ids", "bundle_ids"]
SIZE = ["size"]
DESCRIPTION = ["description"]
CHECKSUMS = ["checksums", "checksum"]
ALIASES = ["alias", "aliases"]

UUID_FORMAT = (
    r"^.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
)
SIZE_FORMAT = r"^[0-9]*$"


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


def _replace_bundle_name_with_guid(bundles, MAIN_DICT):
    """
    replace the bundle names in the list of bundles with its associated guid
    """
    new_list = []
    for bundle in bundles:
        if _verify_format(bundle, UUID_FORMAT):
            new_list.append(bundle)
        else:
            if bundle in MAIN_DICT:
                new_list.append(MAIN_DICT[bundle])
    return new_list


def _create_bundle_record(manifest_file, manifest_file_delimiter="\t"):
    """
    get and verify info from single record and add it to the MAIN_DICT.

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
    MAIN_DICT = {}
    with open(manifest_file, encoding="utf-8-sig") as manifest:
        reader = csv.DictReader(manifest, delimiter=manifest_file_delimiter)
        row_n = 1
        for row in reader:
            row_n += 1
            record = {}
            for key, value in row.items():
                if key in BUNDLENAME:
                    # make sure bundle_name exists. bundle_name is a required field.
                    if len(value) > 0:
                        bundle_name = value
                        record["name"] = bundle_name
                        MAIN_DICT[
                            bundle_name
                        ] = ""  # to keep track of the available bundles.
                    else:
                        logging.error(
                            "ERROR: row {} does not contain bundle_name. bundle_name is required".format(
                                row_n
                            )
                        )
                        pass_verification = False
                elif key in IDS:
                    standard = (
                        _standardize_str(value)
                        .strip()
                        .lstrip("[")
                        .rstrip("]")
                        .split(" ")
                    )
                    bundles = []
                    for i in range(len(standard)):
                        bundle = standard[i].strip().replace("'", "").replace('"', "")
                        if bundle == record["name"]:
                            logging.error(
                                "Error: Bundle at row {} contains itself".format(row_n)
                            )
                            pass_verification = False
                        elif (
                            _verify_format(bundle, UUID_FORMAT) or bundle in MAIN_DICT
                        ):  # so if its a bundle name thats in the dictionary then keep it for processing it later
                            bundles.append(bundle)
                        else:
                            logging.error(
                                "ERROR: bundle_name:{} in list at row {} does not exist".format(
                                    bundle, row_n
                                )
                            )
                            pass_verification = False
                    record["bundles"] = bundles
                elif key in GUID and len(value) > 0:
                    if _verify_format(value, UUID_FORMAT):
                        record["guid"] = value
                    else:
                        logging.error(
                            "ERROR: guid: {} at row {} is in an incorrect format".format(
                                value, row_n
                            )
                        )
                elif key in CHECKSUMS or key in ALIASES:
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
                        if len(v) > 0:
                            values.append(v)
                        record[key] = values
                # Add all the other optional fields
                elif len(value) > 0:
                    if key in SIZE:
                        value = int(value)
                    record[key] = value
            records.append(record)
    if not pass_verification:
        logging.error("The manifest is not in the correct format!")
        return None, None
    return records, MAIN_DICT


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
        records, MAIN_DICT = _create_bundle_record(
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
    start = 0
    # Iterate through the records list and post to indexd
    for record in records:
        start += 1
        bundle_name = record["name"]

        logging.info("Posting bundle {} of {}".format(start, total))

        # Check the bundle list to make sure they're all guids
        record["bundles"] = _replace_bundle_name_with_guid(record["bundles"], MAIN_DICT)
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
                MAIN_DICT[bundle_name] = guid
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
    logging.basicConfig(filename="index_object_manifest.log", level=logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    ingest_bundle_manifest()
