"""
Module for indexing object files in a manifest (against indexd's API). 

The default manifest format created is a Tab-Separated Value file (tsv)
with rows for every record.

Fields that are lists (like acl, authz, and urls) separate the values with commas.
See the Attributes session for supported column names. 

All supported formats of acl and url fields are shown in the below example.

guid	md5	size	acl	url
255e396f-f1f8-11e9-9a07-0a80fada099c	473d83400bc1bc9dc635e334faddf33c	363455714	['Open']	[s3://pdcdatastore/test1.raw]
255e396f-f1f8-11e9-9a07-0a80fada098c	473d83400bc1bc9dc635e334faddd33c	343434344	Open	s3://pdcdatastore/test2.raw
255e396f-f1f8-11e9-9a07-0a80fada097c	473d83400bc1bc9dc635e334fadd433c	543434443	phs0001, phs0002	s3://pdcdatastore/test3.raw
255e396f-f1f8-11e9-9a07-0a80fada096c	473d83400bc1bc9dc635e334fadd433c	363455714	['phs0001', 'phs0002']	['s3://pdcdatastore/test4.raw']
255e396f-f1f8-11e9-9a07-0a80fada010c	473d83400bc1bc9dc635e334fadde33c	363455714	['Open']	s3://pdcdatastore/test5.raw

Attributes:
    CURRENT_DIR (str): directory this file is in
    GUID (list(string)): supported file id column names
    SIZE (list(string)): supported file size column names
    MD5 (list(string)): supported md5 hash column names
    ACLS (list(string)): supported acl column names
    URLS (list(string)): supported url column names


Usages:
    python manifest_indexing.py indexing --common_url https://giangb.planx-pla.net/index/  --manifest_path path_to_manifest --auth "admin,admin" --replace_urls False --thread_num 10
"""
import os
import csv
from functools import partial
import logging
from multiprocessing.dummy import Pool as ThreadPool
import threading
import re
import uuid
import argparse
import copy

import indexclient.client as client


# Pre-defined supported column names
GUID = ["guid", "uuid"]
FILENAME = ["filename", "file_name"]
SIZE = ["size", "filesize", "file_size"]
MD5 = ["md5", "md5_hash", "md5hash", "hash"]
ACLS = ["acl", "acls"]
URLS = ["url", "urls"]

UUID_FORMAT = (
    "^.*[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
)
MD5_FORMAT = "^[a-fA-F0-9]{32}$"
ACL_FORMAT = "^\[.*\]$"
URL_FORMAT = "^\[.*\]$"

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
LOGGING_FILE = os.path.join(CURRENT_DIR, "manifest_indexing.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler(LOGGING_FILE), logging.StreamHandler()],
)


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


def _get_and_verify_fileinfos_from_tsv_manifest(manifest_file, dem="\t"):
    """
    get and verify file infos from tsv manifest

    Args:
        manifest_file(str): the path to the input manifest
        dem(str): delimiter
    
    Returns:
        list(dict): list of file info
        [
            {
                "GUID": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
    """
    files = []
    with open(manifest_file, "r") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=dem)
        fieldnames = csvReader.fieldnames
        pass_verification = True
        for row in csvReader:
            standardized_dict = {}
            for key in row.keys():
                standardized_key = None
                if key.lower() in GUID:
                    fieldnames[fieldnames.index(key)] = "GUID"
                    standardized_key = "GUID"
                elif key.lower() in FILENAME:
                    fieldnames[fieldnames.index(key)] = "filename"
                    standardized_key = "filename"
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
                        row[key] = "[{}]".format(row[key])
                elif key.lower() in URLS:
                    fieldnames[fieldnames.index(key)] = "url"
                    standardized_key = "url"
                    if not _verify_format(row[key], URL_FORMAT):
                        row[key] = "[{}]".format(row[key])

                if standardized_key:
                    standardized_dict[standardized_key] = row[key].strip()

                elif key in SIZE:
                    standardized_dict["size"] = int(row["size"].strip())

            files.append(standardized_dict)

    if not pass_verification:
        logging.error("The manifest is not in the correct format!!!.")
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
                "GUID": "guid_example",
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
        urls = [
            element.strip().replace("'", "")
            for element in fi.get("url", "").strip()[1:-1].split(",")
        ]

        if fi.get("acl", "").strip().lower() in {"[u'open']", "['open']"}:
            acl = ["*"]
        else:
            acl = [
                element.strip().replace("'", "")
                for element in fi.get("acl", "").strip()[1:-1].split(",")
            ]

        guid = uuid.uuid4() if not fi.get("GUID") else fi.get("GUID")

        doc = None

        if fi.get("GUID"):
            doc = indexclient.get(guid)

        if doc is not None:
            if doc.size != fi.get("size") or doc.hashes.get("md5") != fi.get("md5"):
                logging.error(
                    "The guid {} with different size/hash already exist. Can not index it without getting a new GUID".format(
                        fi.get("GUID")
                    )
                )
            else:
                need_update = False
                for url in urls:
                    if not replace_urls and url not in doc.urls:
                        doc.urls.append(url)
                        need_update = True

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

                if set(doc.acl) != set(acl):
                    doc.acl = acl
                    need_update = True

                if need_update:
                    doc.patch()
        else:
            doc = indexclient.create(
                did=fi.get("GUID", "").strip(),
                hashes={"md5": fi.get("md5", "").strip()},
                size=fi.get("size", 0),
                acl=acl,
                urls=urls,
            )

    except Exception as e:
        # Don't break for any reason
        logging.error(
            "Can not update/create an indexd record with guid {}. Detail {}".format(
                fi.get("GUID"), e
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


def manifest_indexing(
    manifest,
    common_url,
    thread_num,
    auth=None,
    replace_urls=False,
    dem="\t",
):
    """
    Loop through all the files in the manifest, update/create records in indexd
    update indexd if the url is not in the record url list or acl has changed

    Args:
        manifest(str): path to the manifest
        common_url(str): common url
        thread_num(int): number of threads for indexing
        auth(Gen3Auth): Gen3 auth
        dem(str): manifest's delimiter
    
    Returns:
        logging_file(str): path to the logging file
        output_manifest(str): path to output manifest. None if the output manifest 
        and the input manifest are the same

    """
    logging.info("Start the process ...")
    indexclient = client.IndexClient(common_url, "v0", auth=auth)

    try:
        files, headers = _get_and_verify_fileinfos_from_tsv_manifest(manifest, dem)
    except Exception as e:
        logging.error("Can not read {}. Detail {}".format(manifest, e))
        return None, None

    try:
        headers.index("url")
    except ValueError as e:
        logging.error("The manifest {} has wrong format".format(manifest, e))
        return None, None

    # Generate uuid if missing
    for fi in files:
        if fi.get("GUID") is None:
            fi["GUID"] = uuid.uuid4()

    do_gen_uuid = False
    try:
        headers.index("GUID")
    except ValueError:
        headers.append("GUID")
        do_gen_uuid = True

    pool = ThreadPool(thread_num)

    thread_control = ThreadControl(num_total_files=len(files))
    part_func = partial(
        _index_record, indexclient, replace_urls, thread_control
    )

    try:
        pool.map_async(part_func, files).get()
    except KeyboardInterrupt:
        pool.terminate()

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    logging.info("Done!!!")
    if do_gen_uuid:
        return (
            LOGGING_FILE,
            _write_csv(
                os.path.join(CURRENT_DIR, "output_manifest.tsv"), files, headers
            ),
        )
    else:
        return LOGGING_FILE, None


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    indexing_cmd = subparsers.add_parser("indexing")
    indexing_cmd.add_argument("--common_url", required=True, help="Common link.")
    indexing_cmd.add_argument(
        "--manifest_path", required=True, help="The path to input manifest"
    )
    indexing_cmd.add_argument(
        "--thread_num", required=False, default=1, help="Number of threads"
    )
    indexing_cmd.add_argument("--auth", required=False, help="auth")
    indexing_cmd.add_argument(
        "--replace_urls", required=False, help="Replace urls or not"
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    auth = tuple(args.auth.split(",")) if args.auth else None

    manifest_indexing(
        args.manifest_path,
        args.common_url,
        int(args.thread_num),
        auth,
        args.replace_urls,
    )
