"""
TODO
"""
import asyncio
import csv
import json
import logging
import os
import time
import urllib.parse

from gen3.index import Gen3Index
from gen3.metadata import Gen3Metadata

TMP_FOLDER = os.path.abspath("./tmp") + "/"
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
MAX_CONCURRENT_REQUESTS = 24
COLUMN_TO_USE_AS_GUID = "guid"
GUID_TYPE_FOR_INDEXED_FILE_OBJECT = "indexed_file_object"
GUID_TYPE_FOR_NON_INDEXED_FILE_OBJECT = "metadata_object"


def _get_guid_from_file(commons_url, row, lock):
    """
    Given a row from the manifest, return the guid to use for the metadata object.

    Args:
        commons_url (str): root domain for commons where indexd lives
        row (dict): column_name:row_value
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections

    Returns:
        str: guid
    """
    return row.get(COLUMN_TO_USE_AS_GUID)


async def _query_for_associated_indexd_record_guid(commons_url, row, lock):
    """
    Given a row from the manifest, return the guid for the related indexd record.

    Args:
        commons_url (str): root domain for commons where indexd lives
        row (dict): column_name:row_value
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections

    Returns:
        str: guid
    """
    mapping = {"urls": "submitted_sample_id"}
    # Alternate example:
    #
    # mapping = {
    #     "acl": "study_with_consent",
    #     "size": "file_size",
    # }

    if "urls" in mapping and len(mapping.items()) > 1:
        msg = (
            "You cannot pattern match 'urls' and exact match other fields for mapping "
            "from indexd record to metadata columns. You can match by URL pattern "
            "*OR* match exact record fields like size, hash, uploader, url, acl, authz."
            f"\nYou provided mapping: {mapping}"
        )
        logging.error(msg)
        raise Exception(msg)

    # special query endpoint for matching url patterns, other fields
    # just use get with params
    if "urls" in mapping:
        pattern = row.get(mapping["urls"])
        logging.debug(f"trying to find matching record matching url pattern: {pattern}")
        records = await _query_urls_from_indexd(pattern, commons_url, lock)
    else:
        params = {
            mapping_key: row.get(mapping_value)
            for mapping_key, mapping_value in mapping.items()
        }
        logging.debug(f"trying to find matching record matching params: {params}")
        records = await _get_with_params_from_indexd(params, commons_url, lock)

    logging.debug(f"matching record(s): {records}")

    if len(records) > 1:
        msg = (
            "Multiple records were found with the given search criteria, this is assumed "
            "to be unintentional so the metadata will NOT be linked to these records:\n"
            f"{records}"
        )
        logging.warning(msg)
        records = []

    guid = None
    if len(records) == 1:
        guid = records[0].get("did")

    return guid


manifest_row_parsers = {
    "guid_from_file": _get_guid_from_file,
    "indexed_file_object_guid": _query_for_associated_indexd_record_guid,
}


async def async_ingest_metadata_manifest(
    commons_url,
    manifest_file,
    metadata_source,
    auth=None,
    max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
    manifest_row_parsers=manifest_row_parsers,
    manifest_file_delimiter=None,
    output_filename=f"ingest-metadata-manifest-errors-{time.time()}.log",
    get_guid_from_file=True,
):
    """
    Ingest all metadata records into a manifest csv

    Args:
        commons_url (str): root domain for commons where indexd lives
        manifest_file (str): the file to ingest against
        metadata_namespace (str): the name of the source of metadata (used to namespace
            in the metadata service) ex: dbgap
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
        manifest_row_parsers (Dict{indexd_field:func_to_parse_row}): Row parsers
        manifest_file_delimiter (str): delimeter in manifest_file
        output_filename (str): filename for output logs
        get_guid_from_file (bool): whether or not to get the guid for metadata from file
            NOTE: When this is True, will use the function in
                  manifest_row_parsers["guid_from_file"] to determine the GUID
                  (usually just a specific column in the file row like "guid")
    """
    start_time = time.perf_counter()
    logging.info(f"start time: {start_time}")

    # if delimter not specified, try to get based on file ext
    if not manifest_file_delimiter:
        file_ext = os.path.splitext(manifest_file)
        if file_ext[-1].lower() == ".tsv":
            manifest_file_delimiter = "\t"
        else:
            # default, assume CSV
            manifest_file_delimiter = ","

    await _ingest_all_metadata_in_file(
        commons_url,
        manifest_file,
        metadata_source,
        auth,
        manifest_file_delimiter,
        max_concurrent_requests,
        output_filename.split("/")[-1],
        get_guid_from_file,
    )

    end_time = time.perf_counter()
    logging.info(f"end time: {end_time}")
    logging.info(f"run time: {end_time-start_time}")


async def _ingest_all_metadata_in_file(
    commons_url,
    manifest_file,
    metadata_source,
    auth,
    manifest_file_delimiter,
    max_concurrent_requests,
    output_filename,
    get_guid_from_file,
):
    """
    Getting metadata and writing to a file. This function
    creates semaphores to limit the number of concurrent http connections that
    get opened to send requests to indexd.

    It then uses asyncio to start a number of coroutines. Steps:
        1) requests to indexd to get records (writes resulting records to a queue)
        2) puts a final "DONE" in the queue to stop coroutine that will read from queue
        3) reading those records from the queue and writing to a file

    Args:
        commons_url (str): root domain for commons where indexd lives
        manifest_file (str): the file to ingest against
        manifest_file_delimiter (str): delimeter in manifest_file
        output_filename (str, optional): filename for output
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
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
                # I know this looks crazy, DictReader is doing goofy things when
                # column contains a JSON-like string so we're trying to fix it here
                # Basically make sure the resulting column is something that we can
                # later json.loads().
                # remove redudant quoting
                new_row[key.strip()] = (
                    value.strip().strip("'").strip('"').replace("''", "'")
                )
            await queue.put(new_row)

    for _ in range(0, int(max_concurrent_requests + (max_concurrent_requests / 4))):
        await queue.put("DONE")

    await asyncio.gather(
        *(
            _parse_from_queue(
                queue,
                lock,
                commons_url,
                output_queue,
                auth,
                get_guid_from_file,
                metadata_source,
            )
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


async def _parse_from_queue(
    queue, lock, commons_url, output_queue, auth, get_guid_from_file, metadata_source
):
    """
    Keep getting items from the queue and verifying that indexd contains the expected
    fields from that row. If there are any issues, log errors into an error queue. Return
    when nothing is left in the queue.

    Args:
        queue (asyncio.Queue): queue to read metadata from
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
        commons_url (str): root domain for commons where indexd lives
        output_queue (asyncio.Queue): queue for output
    """
    loop = asyncio.get_event_loop()

    row = await queue.get()

    while row != "DONE":
        if get_guid_from_file:
            guid = manifest_row_parsers["guid_from_file"](commons_url, row, lock)
            is_indexed_file_object = await _is_indexed_file_object(
                guid, commons_url, lock
            )
        else:
            guid = await manifest_row_parsers["indexed_file_object_guid"](
                commons_url, row, lock
            )
            is_indexed_file_object = True

        if guid:
            # construct metadata from rows, don't include redundant guid column
            logging.debug(f"row: {row}")
            metadata_from_file = {}

            for key, value in row.items():
                try:
                    new_value = json.loads(value)
                except json.decoder.JSONDecodeError as exc:
                    if "}" in value or "{" in value or "[" in value or "]" in value:
                        logging.warning(
                            f"Unable to json.loads a string that looks like json: {value}. "
                            f"adding as a string instead of nested json. Exception: {exc}"
                        )
                    new_value = value

                metadata_from_file[key] = new_value

            del metadata_from_file[COLUMN_TO_USE_AS_GUID]

            logging.debug(f"metadata from file: {metadata_from_file}")

            # namespace by metadata source
            metadata = {metadata_source: metadata_from_file}

            metadata["_guid_type"] = (
                GUID_TYPE_FOR_INDEXED_FILE_OBJECT
                if is_indexed_file_object
                else GUID_TYPE_FOR_NON_INDEXED_FILE_OBJECT
            )

            logging.debug(f"metadata: {metadata}")

            mds = Gen3Metadata(commons_url, auth_provider=auth)
            mds.update(guid, metadata)
        else:
            logging.warning(
                f"Did not add a metadata object for row because an invalid "
                f"GUID was parsed: {guid}.\nRow: {row}"
            )

        row = await queue.get()


async def _is_indexed_file_object(guid, commons_url, lock):
    """
    Gets a semaphore then requests a record for the given guid

    Args:
        guid (str): indexd record globally unique id
        commons_url (str): root domain for commons where indexd lives
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
    """
    index = Gen3Index(commons_url)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        record = await index.async_get_record(guid, _ssl=ssl)
        return bool(record)


async def _query_urls_from_indexd(pattern, commons_url, lock):
    """
    Gets a semaphore then requests a record for the given pattern

    Args:
        pattern (str): url pattern to match
        commons_url (str): root domain for commons where indexd lives
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
    """
    index = Gen3Index(commons_url)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        return await index.async_query_urls(pattern, _ssl=ssl)


async def _get_with_params_from_indexd(params, commons_url, lock):
    """
    Gets a semaphore then requests a record for the given params

    Args:
        params (str): params to match
        commons_url (str): root domain for commons where indexd lives
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
    """
    index = Gen3Index(commons_url)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        return await index.async_get_with_params(params, _ssl=ssl)
