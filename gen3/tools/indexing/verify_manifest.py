"""
Module for indexing actions for verify indexd against a manifest of
expected indexed file objects. Supports
multiple processes using Python's multiprocessing library.

The default manifest format expected is a Comma-Separated Value file (csv)
with rows for every record. A header row is required with field names. The default
expected header row is: guid,authz,acl,file_size,md5,urls

There is a default mapping for those column names above but you can override it.
Fields that expect lists (like acl, authz, and urls) by default assume these values are
separated with spaces. If you need alternate behavior, you can simply override the
`manifest_row_parsers` for specific fields and replace the default parsing function
with a custom one. For example:

```
from gen3.tools import indexing
from gen3.tools.indexing.verify_manifest import manifest_row_parsers

def _get_authz_from_row(row):
    return [row.get("authz").strip().strip("[").strip("]").strip("'")]

# override default parsers
manifest_row_parsers["authz"] = _get_authz_from_row

indexing.verify_object_manifest(COMMONS)
```

The output from this verification is a file containing any errors in the following
format:

{guid}|{error_name}|expected {value_from_manifest}|actual {value_from_indexd}
ex: 93d9af72-b0f1-450c-a5c6-7d3d8d2083b4|authz|expected ['']|actual ['/programs/DEV/projects/test']

Attributes:
    CURRENT_DIR (str): abs path of current directory where this file lives
    manifest_row_parsers (TYPE): Description
    TMP_FOLDER (str): Folder directory for placing temporary files
        NOTE: We have to use a temporary folder b/c Python's file writing is not
              thread-safe so we can't have all processes writing to the same file.
              To workaround this, we have each process write to a file and concat
              them all post-processing.
"""
import csv
import glob
import logging
import fnmatch
from multiprocessing import Pool, Process, Manager, Queue
import multiprocessing
import os
import sys
import shutil
import math
import time
import urllib.parse

from gen3.index import Gen3Index

TMP_FOLDER = os.path.abspath("./tmp") + "/"
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def _get_guid_from_row(row):
    """
    Given a row from the manifest, return the field representing expected indexd guid.

    Args:
        row (dict): column_name:row_value

    Returns:
        str: guid
    """
    guid = row.get("guid")
    if not guid:
        guid = row.get("GUID")
    return guid


def _get_md5_from_row(row):
    """
    Given a row from the manifest, return the field representing file's md5 sum.

    Args:
        row (dict): column_name:row_value

    Returns:
        str: md5 sum for file
    """
    if "md5" in row:
        return row["md5"]
    elif "md5sum" in row:
        return row["md5sum"]
    else:
        return None


def _get_file_size_from_row(row):
    """
    Given a row from the manifest, return the field representing file's size in bytes.

    Args:
        row (dict): column_name:row_value

    Returns:
        int: integer representing file size in bytes
    """
    try:
        if "file_size" in row:
            return int(row["file_size"])
        elif "size" in row:
            return int(row["size"])
        else:
            return None
    except Exception:
        logging.warning(f"could not convert this to an int: {row.get('file_size')}")
        return row.get("file_size")


def _get_acl_from_row(row):
    """
    Given a row from the manifest, return the field representing file's expected acls.

    Args:
        row (dict): column_name:row_value

    Returns:
        List[str]: acls for the indexd record
    """
    return [item for item in row.get("acl", "").strip().split(" ") if item]


def _get_authz_from_row(row):
    """
    Given a row from the manifest, return the field representing file's expected authz
    resources.

    Args:
        row (dict): column_name:row_value

    Returns:
        List[str]: authz resources for the indexd record
    """
    return [item for item in row.get("authz", "").strip().split(" ") if item]


def _get_urls_from_row(row):
    """
    Given a row from the manifest, return the field representing file's expected urls.

    Args:
        row (dict): column_name:row_value

    Returns:
        List[str]: urls for indexd record file location(s)
    """
    if "urls" in row:
        return [item for item in row.get("urls", "").strip().split(" ") if item]
    elif "url" in row:
        return [item for item in row.get("urls", "").strip().split(" ") if item]
    else:
        return []


def _get_file_name_from_row(row):
    """
    Given a row from the manifest, return the field representing file's expected file_name.

    Args:
        row (dict): column_name:row_value

    Returns:
        List[str]: file_name for indexd record file location(s)
    """
    if "file_name" in row:
        return row["file_name"]
    elif "filename" in row:
        return row["filename"]
    elif "name" in row:
        return row["name"]
    else:
        return None


manifest_row_parsers = {
    "guid": _get_guid_from_row,
    "md5": _get_md5_from_row,
    "file_size": _get_file_size_from_row,
    "acl": _get_acl_from_row,
    "authz": _get_authz_from_row,
    "urls": _get_urls_from_row,
    "file_name": _get_file_name_from_row,
}


def verify_object_manifest(
    commons_url,
    manifest_file,
    num_processes=20,
    manifest_row_parsers=manifest_row_parsers,
    manifest_file_delimiter=",",
    log_output_filename=f"verify-manifest-errors-{time.time()}.log",
):
    """
    Verify all the indexd records provided in the manifest file.

    Args:
        commons_url (str): host url for the commons where indexd lives
        log_output_filename (str): filename for output logs
        num_processes (int): number of parallel python processes to run
        manifest_file (str): the file to verify against
        manifest_row_parsers (Dict{indexd_field:func_to_parse_row}): Row parsers
        manifest_file_delimiter (str): delimeter in manifest_file
    """
    start_time = time.time()
    logging.info(f"start time: {start_time}")

    # ensure tmp directory exists and is empty
    os.makedirs(TMP_FOLDER, exist_ok=True)
    for file in os.listdir(TMP_FOLDER):
        file_path = os.path.join(TMP_FOLDER, file)
        if os.path.isfile(file_path):
            os.unlink(file_path)

    _verify_all_index_records_in_file(
        commons_url,
        log_output_filename,
        num_processes,
        manifest_file,
        manifest_row_parsers,
        manifest_file_delimiter,
    )

    end_time = time.time()
    logging.info(f"end time: {end_time}")
    logging.info(f"run time: {end_time-start_time}")


def _verify_all_index_records_in_file(
    commons_url,
    log_output_filename,
    num_processes,
    manifest_file,
    manifest_row_parsers,
    manifest_file_delimiter,
):
    """
    Verify all the indexd records provided in the manifest file by creating a thread-safe
    queue and dumping all the rows from the manifest into it. Then start up processes
    to parallelly pop off the queue and verify the manifest row against indexd's API.
    Then combine all the output logs into a single file.
    """
    index = Gen3Index(commons_url)

    queue = Queue()

    logging.info(f"adding rows from {manifest_file} to queue...")

    with open(manifest_file, encoding="utf-8-sig") as csvfile:
        manifest_reader = csv.DictReader(csvfile, delimiter=manifest_file_delimiter)
        for row in manifest_reader:
            row = {key.strip(" "): value for key, value in row.items()}
            queue.put(row)

    logging.info(
        f"done adding to queue, sending {num_processes} STOP messages b/c {num_processes} processes"
    )

    for x in range(num_processes):
        queue.put("STOP")

    _start_processes_and_process_queue(
        queue, commons_url, num_processes, manifest_row_parsers
    )

    logging.info(
        f"done processing queue, combining outputs to single file {log_output_filename}"
    )

    # remove existing output if it exists
    if os.path.isfile(log_output_filename):
        os.unlink(log_output_filename)

    with open(log_output_filename, "wb") as outfile:
        for filename in glob.glob(TMP_FOLDER + "*"):
            if log_output_filename == filename:
                # don't want to copy the output into the output
                continue
            logging.info(f"combining {filename} into {log_output_filename}")
            with open(filename, "rb") as readfile:
                shutil.copyfileobj(readfile, outfile)

    logging.info(f"done writing output to file {log_output_filename}")


def _start_processes_and_process_queue(
    queue, commons_url, num_processes, manifest_row_parsers
):
    """
    Startup num_processes and wait for them to finish processing the provided queue.
    """
    logging.info(f"starting {num_processes} processes..")

    processes = []
    for x in range(num_processes):
        p = Process(
            target=_verify_records_in_indexd,
            args=(queue, commons_url, manifest_row_parsers),
        )
        p.start()
        processes.append(p)

    logging.info(f"waiting for processes to finish processing queue...")

    for process in processes:
        process.join()


def _verify_records_in_indexd(queue, commons_url, manifest_row_parsers):
    """
    Keep getting items from the queue and verifying that indexd contains the expected
    fields from that row. If there are any issues, log errors into a file. Return
    when nothing is left in the queue.
    """
    index = Gen3Index(commons_url)
    row = queue.get()
    process_name = multiprocessing.current_process().name
    file_name = TMP_FOLDER + str(process_name) + ".log"

    with open(file_name, "w+", encoding="utf8") as file:
        while row != "STOP":
            guid = manifest_row_parsers["guid"](row)
            authz = manifest_row_parsers["authz"](row)
            acl = manifest_row_parsers["acl"](row)
            file_size = manifest_row_parsers["file_size"](row)
            md5 = manifest_row_parsers["md5"](row)
            urls = manifest_row_parsers["urls"](row)
            file_name = manifest_row_parsers["file_name"](row)

            try:
                actual_record = index.get_record(guid)
                if not actual_record:
                    raise Exception(
                        f"Index client could not find record for GUID: {guid}"
                    )
            except Exception as exc:
                output = f"{guid}|no_record|expected {row}|actual {exc}\n"
                file.write(output)
                logging.error(output)
                row = queue.get()
                continue

            logging.info(f"verifying {guid}...")

            if sorted(authz) != sorted(actual_record["authz"]):
                output = (
                    f"{guid}|authz|expected {authz}|actual {actual_record['authz']}\n"
                )
                file.write(output)
                logging.error(output)

            if sorted(acl) != sorted(actual_record["acl"]):
                output = f"{guid}|acl|expected {acl}|actual {actual_record['acl']}\n"
                file.write(output)
                logging.error(output)

            if file_size != actual_record["size"]:
                if (
                    not file_size
                    and file_size != 0
                    and not actual_record["size"]
                    and actual_record["size"] != 0
                ):
                    # actual and expected are both either empty string or None
                    # so even though they're not equal, they represent null value so
                    # we don't need to consider this an error in validation
                    pass
                else:
                    output = f"{guid}|file_size|expected {file_size}|actual {actual_record['size']}\n"
                    file.write(output)
                    logging.error(output)

            if md5 != actual_record["hashes"].get("md5"):
                if (
                    not md5
                    and md5 != 0
                    and not actual_record["hashes"].get("md5")
                    and actual_record["hashes"].get("md5") != 0
                ):
                    # actual and expected are both either empty string or None
                    # so even though they're not equal, they represent null value so
                    # we don't need to consider this an error in validation
                    pass
                else:
                    output = f"{guid}|md5|expected {md5}|actual {actual_record['hashes'].get('md5')}\n"
                    file.write(output)
                    logging.error(output)

            if sorted(urls) != sorted(actual_record["urls"]):
                output = f"{guid}|urls|expected {urls}|actual {actual_record['urls']}\n"
                file.write(output)
                logging.error(output)

            if not actual_record["file_name"] and file_name:
                # if the actual record name is "" or None but something was specified
                # in the manifest, we have a problem
                output = f"{guid}|file_name|expected {file_name}|actual {actual_record['file_name']}\n"
                file.write(output)
                logging.error(output)

            row = queue.get()

    logging.info(f"{process_name}:Stop")
