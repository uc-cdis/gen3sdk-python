"""
Module for indexing object files in a manifest (against indexd's API).

The default manifest format created is a Tab-Separated Value file (tsv)
with rows for every record.

Fields that are lists (like acl, authz, and urls) separate the values with commas or spaces
See the Attributes session for supported column names.

All supported formats of acl, authz and url fields are shown in the below example.

guid    md5 size    acl authz   url
255e396f-f1f8-11e9-9a07-0a80fada099c    473d83400bc1bc9dc635e334faddf33c    363455714   ['Open']    [s3://pdcdatastore/test1.raw]
255e396f-f1f8-11e9-9a07-0a80fada098c    473d83400bc1bc9dc635e334faddd33c    343434344   Open    s3://pdcdatastore/test2.raw
255e396f-f1f8-11e9-9a07-0a80fada097c    473d83400bc1bc9dc635e334fadd433c    543434443   phs0001 phs0002 s3://pdcdatastore/test3.raw
255e396f-f1f8-11e9-9a07-0a80fada096c    473d83400bc1bc9dc635e334fadd433c    363455714   ['phs0001', 'phs0002']  ['s3://pdcdatastore/test4.raw']
255e396f-f1f8-11e9-9a07-0a80fada010c    473d83400bc1bc9dc635e334fadde33c    363455714   ['Open']    s3://pdcdatastore/test5.raw

Attributes:
    CURRENT_DIR (str): directory this file is in
    GUID (list(string)): supported file id column names
    SIZE (list(string)): supported file size column names
    MD5 (list(string)): supported md5 hash column names
    ACLS (list(string)): supported acl column names
    URLS (list(string)): supported url column names
    AUTHZ (list(string)): supported authz column names


Usages:
    python index_manifest.py --commons_url https://giangb.planx-pla.net  --manifest_file path_to_manifest --auth "admin,admin" --replace_urls False --thread_num 10
    python index_manifest.py --commons_url https://giangb.planx-pla.net  --manifest_file path_to_manifest --api_key ./credentials.json --replace_urls False --thread_num 10
"""
import os
import csv
import click
from functools import partial
import logging
from multiprocessing.dummy import Pool as ThreadPool
import threading
import re
import uuid
import copy
import sys
import traceback

from gen3.auth import Gen3Auth
import indexclient.client as client


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
# Pre-defined supported column names
GUID = ["guid", "GUID"]
FILENAME = ["filename", "file_name"]
SIZE = ["size", "filesize", "file_size"]
MD5 = ["md5", "md5_hash", "md5hash", "hash"]
ACLS = ["acl", "acls"]
URLS = ["url", "urls"]
AUTHZ = ["authz"]

UUID_FORMAT = (
    r"^.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
)
MD5_FORMAT = r"^[a-fA-F0-9]{32}$"
SIZE_FORMAT = r"^[0-9]*$"
ACL_FORMAT = r"^.*$"
URL_FORMAT = r"^.*$"
AUTHZ_FORMAT = r"^.*$"


class ThreadControl(object):
    """
    Class for thread synchronization
    """

    def __init__(self, processed_files=0, num_total_files=0):
        self.mutexLock = threading.Lock()
        self.num_processed_files = processed_files
        self.num_total_files = num_total_files


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


def _get_and_verify_fileinfos_from_tsv_manifest(
    manifest_file, manifest_file_delimiter="\t"
):
    """
    get and verify file infos from tsv manifest

    Args:
        manifest_file(str): the path to the input manifest
        manifest_file_delimiter(str): delimiter

    Returns:
        list(dict): list of file info
        [
            {
                "guid": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
        headers(list(str)): field names

    """
    files = []
    with open(manifest_file, "r", encoding="utf-8-sig") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=manifest_file_delimiter)
        fieldnames = csvReader.fieldnames

        logging.debug(f"got fieldnames from {manifest_file}: {fieldnames}")
        pass_verification = True
        row_number = 0
        for row in csvReader:
            standardized_dict = {}
            for key in row.keys():
                row_number = row_number + 1
                standardized_key = None
                if key.lower() in GUID:
                    fieldnames[fieldnames.index(key)] = "guid"
                    standardized_key = "guid"
                elif key.lower() in FILENAME:
                    fieldnames[fieldnames.index(key)] = "file_name"
                    standardized_key = "file_name"
                elif key.lower() in MD5:
                    fieldnames[fieldnames.index(key)] = "md5"
                    standardized_key = "md5"
                    if not _verify_format(row[key], MD5_FORMAT):
                        logging.error("ERROR: {} is not in md5 format", row[key])
                        pass_verification = False
                elif key.lower() in ACLS:
                    fieldnames[fieldnames.index(key)] = "acl"
                    standardized_key = "acl"
                    if not _verify_format(row[key], ACL_FORMAT):
                        logging.error("ERROR: {} is not in acl format", row[key])
                        pass_verification = False
                elif key.lower() in URLS:
                    fieldnames[fieldnames.index(key)] = "urls"
                    standardized_key = "urls"
                    if not _verify_format(row[key], URL_FORMAT):
                        logging.error("ERROR: {} is not in urls format", row[key])
                        pass_verification = False
                elif key.lower() in AUTHZ:
                    fieldnames[fieldnames.index(key)] = "authz"
                    standardized_key = "authz"
                    if not _verify_format(row[key], AUTHZ_FORMAT):
                        logging.error("ERROR: {} is not in authz format", row[key])
                        pass_verification = False
                elif key.lower() in SIZE:
                    fieldnames[fieldnames.index(key)] = "size"
                    standardized_key = "size"
                    if not _verify_format(row[key], SIZE_FORMAT):
                        logging.error("ERROR: {} is not in int format", row[key])
                        pass_verification = False

                if standardized_key:
                    try:
                        standardized_dict[standardized_key] = (
                            int(row[key])
                            if standardized_key == "size"
                            else row[key].strip()
                        )
                    except ValueError:
                        # don't break
                        pass

            if not {"urls", "md5", "size"}.issubset(set(standardized_dict.keys())):
                pass_verification = False

            if not pass_verification:
                logging.error(
                    f"row {row_number} with values {row} does not pass the validation"
                )

            files.append(standardized_dict)

    if not pass_verification:
        logging.error("The manifest is not in the correct format!!!")
        return None, None

    return files, fieldnames


def _write_csv(filename, files, fieldnames=None):
    """
    write to csv file

    Args:
        filename(str): file name
        files(list(dict)): list of file info
        [
            {
                "guid": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
        fieldnames(list(str)): list of column names

    Returns:
        None
    """

    if not files:
        return None
    fieldnames = fieldnames or files[0].keys()
    with open(filename, mode="w") as outfile:
        writer = csv.DictWriter(outfile, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()

        for f in files:
            writer.writerow(f)

    return filename


def _index_record(indexclient, replace_urls, thread_control, fi):
    """
    Index single file

    Args:
        indexclient(IndexClient): indexd client
        replace_urls(bool): replace urls or not
        fi(dict): file info

    Returns:
        None

    """

    try:
        urls = (
            [
                element.strip().replace("'", "").replace('"', "").replace("%20", " ")
                for element in _standardize_str(fi["urls"])
                .strip()
                .lstrip("[")
                .rstrip("]")
                .split(" ")
            ]
            if "urls" in fi and fi["urls"] != "[]" and fi["urls"]
            else []
        )
        authz = (
            [
                element.strip().replace("'", "").replace('"', "").replace("%20", " ")
                for element in _standardize_str(fi["authz"])
                .strip()
                .lstrip("[")
                .rstrip("]")
                .split(" ")
            ]
            if "authz" in fi and fi["authz"] != "[]" and fi["authz"]
            else []
        )

        if "acl" in fi:
            if fi["acl"].strip().lower() in {"[u'open']", "['open']"}:
                acl = ["*"]
            else:
                acl = (
                    [
                        element.strip()
                        .replace("'", "")
                        .replace('"', "")
                        .replace("%20", " ")
                        for element in _standardize_str(fi["acl"])
                        .strip()
                        .lstrip("[")
                        .rstrip("]")
                        .split(" ")
                    ]
                    if "acl" in fi and fi["acl"] != "[]" and fi["acl"]
                    else []
                )
        else:
            acl = []

        if "file_name" in fi:
            file_name = _standardize_str(fi["file_name"])
        else:
            file_name = ""

        doc = None

        if fi.get("guid"):
            doc = indexclient.get(fi["guid"])

        if doc is not None:
            if doc.size != fi.get("size") or doc.hashes.get("md5") != fi.get("md5"):
                logging.error(
                    "The guid {} with different size/hash already exist. Can not index it without getting a new guid".format(
                        fi.get("guid")
                    )
                )
            else:
                need_update = False

                if replace_urls and set(urls) != set(doc.urls):
                    doc.urls = urls
                    need_update = True

                    # indexd doesn't like when records have metadata for non-existing
                    # urls
                    new_urls_metadata = copy.deepcopy(doc.urls_metadata)
                    for url, metadata in doc.urls_metadata.items():
                        if url not in urls:
                            del new_urls_metadata[url]
                    doc.urls_metadata = new_urls_metadata

                elif not replace_urls:
                    for url in urls:
                        if url not in doc.urls:
                            doc.urls.append(url)
                            need_update = True

                if set(doc.acl) != set(acl):
                    doc.acl = acl
                    need_update = True

                if set(doc.authz) != set(authz):
                    doc.authz = authz
                    need_update = True

                if doc.file_name != file_name:
                    doc.file_name = file_name
                    need_update = True

                if need_update:
                    logging.info(f"updating {doc.did} to: {doc.to_json()}")
                    doc.patch()
        else:
            if fi.get("guid"):
                guid = fi.get("guid", "").strip()
            else:
                guid = None

            record = {
                "did": guid,
                "hashes": {"md5": fi.get("md5", "").strip()},
                "size": fi.get("size", 0),
                "acl": acl,
                "authz": authz,
                "urls": urls,
                "file_name": file_name,
            }
            logging.info(f"creating: {record}")
            doc = indexclient.create(**record)

            fi["guid"] = doc.did

    except Exception as e:
        # Don't break for any reason
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        logging.error(
            "Can not update/create an indexd record with guid {}. Detail {}".format(
                fi.get("guid"), e
            )
        )

    thread_control.mutexLock.acquire()
    thread_control.num_processed_files += 1
    if (thread_control.num_processed_files * 10) % thread_control.num_total_files == 0:
        logging.info(
            "Progress: {}%".format(
                thread_control.num_processed_files
                * 100.0
                / thread_control.num_total_files
            )
        )
    thread_control.mutexLock.release()


def index_object_manifest(
    commons_url,
    manifest_file,
    thread_num,
    auth=None,
    replace_urls=True,
    manifest_file_delimiter=None,
    output_filename="indexing-output-manifest.csv",
):
    """
    Loop through all the files in the manifest, update/create records in indexd
    update indexd if the url is not in the record url list or acl has changed

    Args:
        commons_url(str): common url
        manifest_file(str): path to the manifest
        thread_num(int): number of threads for indexing
        auth(Gen3Auth): Gen3 auth or tuple with basic auth name and password
        replace_urls(bool): flag to indicate if replace urls or not
        manifest_file_delimiter(str): manifest's delimiter

    Returns:
        files(list(dict)): list of file info
        [
            {
                "guid": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
        headers(list(str)): list of fieldnames

    """
    logging.info("Start the process ...")
    service_location = "index"
    commons_url = commons_url.strip("/")
    # if running locally, indexd is deployed by itself without a location relative
    # to the commons
    if "http://localhost" in commons_url:
        service_location = ""

    if not commons_url.endswith(service_location):
        commons_url += "/" + service_location

    indexclient = client.IndexClient(commons_url, "v0", auth=auth)

    # if delimter not specified, try to get based on file ext
    if not manifest_file_delimiter:
        file_ext = os.path.splitext(manifest_file)
        if file_ext[-1].lower() == ".tsv":
            manifest_file_delimiter = "\t"
        else:
            # default, assume CSV
            manifest_file_delimiter = ","

    try:
        files, headers = _get_and_verify_fileinfos_from_tsv_manifest(
            manifest_file, manifest_file_delimiter
        )
    except Exception as e:
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        logging.error("Can not read {}. Detail {}".format(manifest_file, e))
        return None, None

    # Early terminate
    if not files:
        return None, None

    try:
        headers.index("guid")
    except ValueError:
        headers.insert(0, "guid")

    pool = ThreadPool(thread_num)

    thread_control = ThreadControl(num_total_files=len(files))
    part_func = partial(_index_record, indexclient, replace_urls, thread_control)

    try:
        pool.map_async(part_func, files).get()
    except KeyboardInterrupt:
        pool.terminate()

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    output_filename = os.path.abspath(output_filename)
    logging.info(f"Writing output to {output_filename}")

    # remove existing output if it exists
    if os.path.isfile(output_filename):
        os.unlink(output_filename)

    _write_csv(os.path.join(CURRENT_DIR, output_filename), files, headers)

    return files, headers


@click.command()
@click.option(
    "--commons_url",
    help="Root domain (url) for a commons containing indexd.",
    required=True,
)
@click.option("--manifest_file", help="The path to input manifest")
@click.option("--thread_num", type=int, help="Number of threads", default=1)
@click.option("--api_key", help="path to api key")
@click.option("--auth", help="basic auth")
@click.option("--replace_urls", type=bool, help="Replace urls or not", default=False)
@click.option(
    "--manifest_file_delimiter",
    help="string character that delimites the file (tab or comma). Defaults to tab.",
    default="\t",
)
@click.option(
    "--out_manifest_file",
    help="The path to output manifest",
    default="indexing-output-manifest.csv",
)
def index_object_manifest_cli(
    commons_url,
    manifest_file,
    thread_num,
    api_key,
    auth,
    replace_urls,
    manifest_file_delimiter,
    out_manifest_file,
):
    """
    Commandline interface for indexing a given manifest to indexd

    Args:
        commons_url (str): root domain for common
        manifest_file (str): the full path to the manifest
        thread_num (int): number of threads being requested
        api_key (str): the path to api key
        auth(str): the basic auth
        replace_urls(bool): Replace urls or not
            NOTE: if both api_key and auth are specified, it will ignore the later and
            take the former as a default
        manifest_file_delimiter(str): manifest's delimiter
        out_manifest_file(str): path to the output manifest

    """

    if api_key:
        auth = Gen3Auth(commons_url, refresh_file=api_key)
    else:
        auth = tuple(auth.split(",")) if auth else None

    files, headers = index_object_manifest(
        commons_url + "/index",
        manifest_file,
        int(thread_num),
        auth,
        replace_urls,
        manifest_file_delimiter,
        output_filename=out_manifest_file,
    )


if __name__ == "__main__":
    logging.basicConfig(filename="index_object_manifest.log", level=logging.DEBUG)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    index_object_manifest_cli()
