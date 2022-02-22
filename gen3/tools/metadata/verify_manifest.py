"""
Module for metadata actions for verifying a manifest of
metadata objects (against mds's API). Supports
multiple processes and coroutines using Python's asyncio library.

Manifest must include a guid column in addition to an arbitrary number of columns
representing expected metadata fields. Fields that are key:value mappings should be
valid JSON strings.

There is a default mapping for getting the GUID and metadata fields but you can override it.
Fields that expect lists (like acl, authz, and urls) by default assume these values are
separated with spaces. If you need alternate behavior, you can simply override the
`manifest_row_parsers`.

The output from this verification is a file containing any errors in the following
format:

{guid}|{mismatched_field}|expected {value_from_manifest}|actual {value_from_mds}

Attributes:
    CURRENT_DIR (str): directory this file is in
    MAX_CONCURRENT_REQUESTS (int): maximum number of desired concurrent requests across
        processes/threads
"""
import asyncio
import aiohttp
import csv
from collections import OrderedDict
from collections.abc import Mapping
import json
from cdislogging import get_logger

import os
import time

from gen3.metadata import Gen3Metadata
from gen3.utils import get_or_create_event_loop_for_thread

MAX_CONCURRENT_REQUESTS = 24
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

logging = get_logger("__name__")


def _get_guid_from_row(row):
    """
    Given a row from the manifest, return the field representing expected mds guid.

    Args:
        row (dict): column_name:row_value

    Returns:
        str: guid
    """
    guid = row.get("guid")
    if not guid:
        guid = row.get("GUID")
    return guid


def _get_metadata_from_row(row):
    """
    Given a row from the manifest, return the field representing metadata.

    Args:
        row (dict): column_name:row_value

    Returns:
    """
    metadata = dict(row)

    # make sure guid is not part of the metadata
    if "guid" in metadata:
        del metadata["guid"]

    if "GUID" in metadata:
        del metadata["GUID"]

    return metadata


manifest_row_parsers = {"guid": _get_guid_from_row, "metadata": _get_metadata_from_row}


async def async_verify_metadata_manifest(
    commons_url,
    manifest_file,
    metadata_source,
    max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
    manifest_row_parsers=manifest_row_parsers,
    manifest_file_delimiter=None,
    output_filename=f"verify-metadata-errors-{time.time()}.log",
):
    """
    Verify all file object records

    Args:
        commons_url (str): root domain for commons where mds lives
        manifest_file (str): the file to verify against
        metadata_source (str): the source of the metadata you are verifying, in practice
            this means the first nested section in the metadata service
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
        manifest_row_parsers (Dict{mds_field:func_to_parse_row}): Row parsers
        manifest_file_delimiter (str): delimeter in manifest_file
        output_filename (str): filename for output logs
    """
    start_time = time.perf_counter()
    logging.info(f"start time: {start_time}")

    # if delimiter not specified, try to get based on file ext
    if not manifest_file_delimiter:
        file_ext = os.path.splitext(manifest_file)
        if file_ext[-1].lower() == ".tsv":
            manifest_file_delimiter = "\t"
        else:
            # default, assume CSV
            manifest_file_delimiter = ","

    await _verify_all_metadata_records_in_file(
        commons_url,
        manifest_file,
        manifest_file_delimiter,
        max_concurrent_requests,
        output_filename,
        metadata_source,
    )

    end_time = time.perf_counter()
    logging.info(f"end time: {end_time}")
    logging.info(f"run time: {end_time-start_time}")


async def _verify_all_metadata_records_in_file(
    commons_url,
    manifest_file,
    manifest_file_delimiter,
    max_concurrent_requests,
    output_filename,
    metadata_source,
):
    """
    Getting mds records and write output to a file. This function
    creates semaphores to limit the number of concurrent http connections that
    get opened to send requests to mds.

    It then uses asyncio to start a number of coroutines. Steps:
        1) requests to mds to get records (writes resulting records to a queue)
        2) reading those records from the queue and writing to a file

    Args:
        commons_url (str): root domain for commons where mds lives
        manifest_file (str): the file to verify against
        manifest_file_delimiter (str): delimeter in manifest_file
        output_filename (str, optional): filename for output
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
        metadata_source (str): the source of the metadata you are verifying, in practice
            this means the first nested section in the metadata service
    """
    max_requests = int(max_concurrent_requests)
    logging.debug(f"max concurrent requests: {max_requests}")
    lock = asyncio.Semaphore(max_requests)
    queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    with open(manifest_file, encoding="utf-8-sig") as manifest:
        reader = csv.DictReader(manifest, delimiter=manifest_file_delimiter)
        for row in reader:
            new_row = {}
            for key, value in row.items():
                new_row[key.strip()] = value.strip()
            await queue.put(new_row)

    await asyncio.gather(
        *(
            _parse_from_queue(queue, lock, commons_url, output_queue, metadata_source)
            # why "+ (max_concurrent_requests / 4)"?
            # This is because the max requests at any given time could be
            # waiting for metadata responses all at once and there's processing done
            # before that semaphore, so this just adds a few extra processes to get
            # through the queue up to that point of metadata requests so it's ready
            # right away when a lock is released. Not entirely necessary but speeds
            # things up a tiny bit to always ensure something is waiting for that lock
            for x in range(
                0, int(max_concurrent_requests + (max_concurrent_requests / 4))
            )
        )
    )

    output_filename = os.path.abspath(output_filename)
    logging.info(
        f"done processing, writing output queue to single file {output_filename}"
    )

    # remove existing output if it exists
    if os.path.isfile(output_filename):
        os.unlink(output_filename)

    with open(output_filename, "w") as outfile:
        while not output_queue.empty():
            line = await output_queue.get()
            outfile.write(line)

    logging.info(f"done writing output to file {output_filename}")


async def _parse_from_queue(queue, lock, commons_url, output_queue, metadata_source):
    """
    Keep getting items from the queue and verifying that mds contains the expected
    fields from that row. If there are any issues, log errors into a file. Return
    when nothing is left in the queue.

    Args:
        queue (asyncio.Queue): queue to read mds records from
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
        commons_url (str): root domain for commons where mds lives
        output_queue (asyncio.Queue): queue for output
        metadata_source (str): the source of the metadata you are verifying, in practice
            this means the first nested section in the metadata service
    """
    loop = get_or_create_event_loop_for_thread()

    while not queue.empty():
        row = await queue.get()

        guid = manifest_row_parsers["guid"](row)
        metadata = manifest_row_parsers["metadata"](row)

        actual_record = await _get_record_from_mds(guid, commons_url, lock)
        if not actual_record:
            output = f"{guid}|no_record|expected {row}|actual None\n"
            await output_queue.put(output)
            logging.error(output)
        else:
            logging.info(f"verifying {guid}...")

            for expected_key, expected_value in metadata.items():
                actual_value_raw = actual_record.get(metadata_source, {}).get(
                    expected_key
                )
                try:
                    actual_value = json.loads(str(actual_value_raw))
                except json.decoder.JSONDecodeError as exc:
                    actual_value = actual_value_raw

                try:
                    expected_value = json.loads(str(expected_value))
                except json.decoder.JSONDecodeError as exc:
                    pass

                # compare as dicts if necessary, otherwise just compare values
                if (
                    isinstance(expected_value, Mapping)
                    and not _are_matching_dicts(expected_value, actual_value)
                ) or (actual_value != expected_value):
                    output = f"{guid}|{metadata_source}.{expected_key}|expected {expected_value}|actual {actual_value}\n"
                    await output_queue.put(output)
                    logging.error(output)


def _are_matching_dicts(dict_a, dict_b):
    # if one of these isn't a dict, just try a normal comparison
    if not getattr(dict_a, "keys", None) or not getattr(dict_b, "keys", None):
        return dict_a == dict_b

    if len(dict_a.keys()) != len(dict_b.keys()):
        return False

    for key_a, value_a in dict_a.items():
        value_b = dict_b.get(key_a)
        if value_a != value_b:
            return False

    return True


async def _get_record_from_mds(guid, commons_url, lock):
    """
    Gets a semaphore then requests a record for the given guid

    Args:
        guid (str): mds record globally unique id
        commons_url (str): root domain for commons where mds lives
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
    """
    mds = Gen3Metadata(commons_url)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        try:
            return await mds.async_get(guid, _ssl=ssl)
        except aiohttp.client_exceptions.ClientResponseError:
            return None
