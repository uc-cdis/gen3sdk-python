"""
Module for indexing actions for verify indexd against a manifest of
expected indexed file objects. Supports
multiple processes using Python's multiprocessing library.

The default manifest format expected is a Comma-Separated Value file (csv)
with rows for every record. A header row is required with field names. There
is a default mapping provided but you can override it. Fields that expect
lists (like acl, authz, and urls) by default assume these values are separated with spaces.

Attributes:
    TMP_FOLDER (str): Folder directory for placing temporary files
    CURRENT_DIR (str): abs path of current directory where this file lives
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
    guid = row.get("guid")
    if not guid:
        return row.get("GUID")


def _get_md5_from_row(row):
    return row.get("md5")


def _get_file_size_from_row(row):
    try:
        return int(row.get("file_size"))
    except Exception:
        logging.warning(f"could not convert this to an int: {row.get('file_size')}")
        return row.get("file_size")


def _get_acl_from_row(row):
    return [item for item in row.get("acl", "").split(" ") if item]


def _get_authz_from_row(row):
    return [item for item in row.get("authz", "").split(" ") if item]


def _get_urls_from_row(row):
    return [item for item in row.get("urls", "").split(" ") if item]


manifest_row_parsers = {
    "guid": _get_guid_from_row,
    "md5": _get_md5_from_row,
    "file_size": _get_file_size_from_row,
    "acl": _get_acl_from_row,
    "authz": _get_authz_from_row,
    "urls": _get_urls_from_row,
}


def verify_object_manifest(
    commons_url,
    manifest_file,
    num_processes=20,
    manifest_row_parsers=manifest_row_parsers,
    manifest_file_delimter=",",
    log_output_filename=f"verify-manifest-errors-{time.time()}.log",
):
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
        manifest_file_delimter,
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
    manifest_file_delimter,
):
    index = Gen3Index(commons_url)

    queue = Queue()

    logging.info(f"adding rows from {manifest_file} to queue...")

    with open(manifest_file, encoding="utf-8-sig") as csvfile:
        manifest_reader = csv.DictReader(csvfile, delimiter=manifest_file_delimter)
        for row in manifest_reader:
            row = {key.strip(" "): value.strip(" ") for key, value in row.items()}
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
                output = f"{guid}|file_size|expected {file_size}|actual {actual_record['size']}\n"
                file.write(output)
                logging.error(output)

            if md5 != actual_record["hashes"].get("md5"):
                output = f"{guid}|md5|expected {md5}|actual {actual_record['hashes'].get('md5')}\n"
                file.write(output)
                logging.error(output)

            if sorted(urls) != sorted(actual_record["urls"]):
                output = f"{guid}|urls|expected {urls}|actual {actual_record['urls']}\n"
                file.write(output)
                logging.error(output)

            row = queue.get()

    logging.info(f"{process_name}:Stop")
