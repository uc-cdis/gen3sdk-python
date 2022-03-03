"""
Tools for ingesting a CSV/TSV metadata manifest into the Metdata Service.

Attributes:
    COLUMN_TO_USE_AS_GUID (str): file column containing guid to use
    GUID_TYPE_FOR_INDEXED_FILE_OBJECT (str): type to populate in mds when guid exists
        in indexd
    GUID_TYPE_FOR_NON_INDEXED_FILE_OBJECT (str): type to populate in mds when guid does
        NOT exist in indexd
    manifest_row_parsers (Dict{str: function}): functions for parsing, users can override
        manifest_row_parsers = {
            "guid_from_file": _get_guid_for_row,
            "indexed_file_object_guid": _query_for_associated_indexd_record_guid,
        }

        "guid_for_row" is the function to retrieve the guid from the given file
        "indexed_file_object_guid" is the function to retrieve the guid from elsewhere,
            like indexd (by querying)

    MAX_CONCURRENT_REQUESTS (int): Maximum concurrent requests to mds for ingestion
"""
import aiohttp
import asyncio
import csv
import json
from cdislogging import get_logger

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

logging = get_logger("__name__")


def _get_guid_for_row(commons_url, row, lock):
    """
    Given a row from the manifest, return the guid to use for the metadata object.

    Args:
        commons_url (str): root domain for commons where mds lives
        row (dict): column_name:row_value
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections

    Returns:
        str: guid
    """
    return row.get(COLUMN_TO_USE_AS_GUID)


async def _query_for_associated_indexd_record_guid(
    commons_url, row, lock, output_queue
):
    """
    Given a row from the manifest, return the guid for the related indexd record.

    By default attempts to use a column "submitted_sample_id" to pattern match
    URLs in indexd records to find a single match.

    For example:
        "NWD12345" would match a record with url: "s3://some-bucket/file_NWD12345.cram"

    WARNING: The query endpoint this uses in indexd is incredibly slow when there are
             lots of indexd records.

    Args:
        commons_url (str): root domain for commons where mds lives
        row (dict): column_name:row_value
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
        output_queue (asyncio.Queue): queue for logging output

    Returns:
        str: guid or None
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
        await output_queue.put(msg)
        raise Exception(msg)

    # special query endpoint for matching url patterns, other fields
    # just use get with params
    if "urls" in mapping:
        pattern = row.get(mapping["urls"])
        logging.debug(f"trying to find matching record matching url pattern: {pattern}")
        records = await async_query_urls_from_indexd(pattern, commons_url, lock)
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
        await output_queue.put(msg)
        records = []

    guid = None
    if len(records) == 1:
        guid = records[0].get("did")

    return guid


manifest_row_parsers = {
    "guid_for_row": _get_guid_for_row,
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
    metadata_type=None,
):
    """
    Ingest all metadata records into a manifest csv

    Args:
        commons_url (str): root domain for commons where mds lives
        manifest_file (str): the file to ingest against
        metadata_source (str): the name of the source of metadata (used to namespace
            in the metadata service) ex: dbgap
        auth (Gen3Auth): Gen3 auth or tuple with basic auth name and password
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
        manifest_row_parsers (Dict{indexd_field:func_to_parse_row}): Row parsers
        manifest_file_delimiter (str): delimeter in manifest_file
        output_filename (str): filename for output logs
        get_guid_from_file (bool): whether or not to get the guid for metadata from file
            NOTE: When this is True, will use the function in
                  manifest_row_parsers["guid_for_row"] to determine the GUID
                  (usually just a specific column in the file row like "guid")
        metadata_type (str): the type of metadata to be filled into the _guid_type field.
            If provided, will override the default logic per GUID: (GUID_TYPE_FOR_INDEXED_FILE_OBJECT
                if is_indexed_file_object
                else GUID_TYPE_FOR_NON_INDEXED_FILE_OBJECT)
    """
    # if delimiter not specified, try to get based on file ext
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
        metadata_type,
    )


async def _ingest_all_metadata_in_file(
    commons_url,
    manifest_file,
    metadata_source,
    auth,
    manifest_file_delimiter,
    max_concurrent_requests,
    output_filename,
    get_guid_from_file,
    metadata_type,
):
    """
    Ingest metadata from file into metadata service. This function
    creates semaphores to limit the number of concurrent http connections that
    get opened to send requests to mds.

    It then uses asyncio to start a number of coroutines. Steps:
        1) reads given metadata file (writes resulting rows to a queue)
        2) posts/puts to mds to write metadata for rows

    Args:
        commons_url (str): root domain for commons where mds lives
        manifest_file (str): the file to ingest against
        metadata_source (str): the name of the source of metadata (used to namespace
            in the metadata service) ex: dbgap
        auth (Gen3Auth): Gen3 auth or tuple with basic auth name and password
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
        manifest_file_delimiter (str): delimeter in manifest_file
        output_filename (str): filename for output logs
        get_guid_from_file (bool): whether or not to get the guid for metadata from file
            NOTE: When this is True, will use the function in
                  manifest_row_parsers["guid_for_row"] to determine the GUID
                  (usually just a specific column in the file row like "guid")
        metadata_type (str): the type of metadata to be filled into the _guid_type field.
            If provided, will override the default logic per GUID: (GUID_TYPE_FOR_INDEXED_FILE_OBJECT
                if is_indexed_file_object
                else GUID_TYPE_FOR_NON_INDEXED_FILE_OBJECT)
    """
    max_requests = int(max_concurrent_requests)
    logging.debug(f"max concurrent requests: {max_requests}")
    lock = asyncio.Semaphore(max_requests)
    queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    start_time = time.perf_counter()
    msg = f"start time: {start_time}"
    logging.info(msg)
    await output_queue.put(msg)

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
                if value:
                    value = value.strip().strip("'").strip('"').replace("''", "'")
                new_row[key.strip()] = value
            await queue.put(new_row)

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
                metadata_type,
            )
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

    end_time = time.perf_counter()
    msg = f"end time: {end_time}"
    logging.info(msg)
    await output_queue.put(msg)

    msg = f"run time: {end_time-start_time}"
    logging.info(msg)
    await output_queue.put(msg)

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
            outfile.write(line + "\n")

    logging.info(f"done writing output to file {output_filename}")


async def _parse_from_queue(
    queue,
    lock,
    commons_url,
    output_queue,
    auth,
    get_guid_from_file,
    metadata_source,
    metadata_type,
):
    """
    Keep getting items from the queue and checking if indexd contains a record with
    that guid. Then create/update metadta for that GUID in the metadata service.
    Also log to output queue. Return when nothing is left in the queue.

    Args:
        queue (asyncio.Queue): queue to read metadata from
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
        commons_url (str): root domain for commons where mds lives
        output_queue (asyncio.Queue): queue for logging output
        auth (Gen3Auth): Gen3 auth or tuple with basic auth name and password
        get_guid_from_file (bool): whether or not to get the guid for metadata from file
            NOTE: When this is True, will use the function in
                  manifest_row_parsers["guid_for_row"] to determine the GUID
                  (usually just a specific column in the file row like "guid")
        metadata_source (str): the name of the source of metadata (used to namespace
            in the metadata service) ex: dbgap
        metadata_type (str): the type of metadata to be filled into the _guid_type field.
            If provided, will override the default logic per GUID: (GUID_TYPE_FOR_INDEXED_FILE_OBJECT
                if is_indexed_file_object
                else GUID_TYPE_FOR_NON_INDEXED_FILE_OBJECT)
    """
    while not queue.empty():
        row = await queue.get()

        if get_guid_from_file:
            guid = manifest_row_parsers["guid_for_row"](commons_url, row, lock)
            is_indexed_file_object = await _is_indexed_file_object(
                guid, commons_url, lock
            )
        else:
            guid = await manifest_row_parsers["indexed_file_object_guid"](
                commons_url, row, lock, output_queue
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
                        msg = (
                            f"Unable to json.loads a string that looks like json: {value}. "
                            f"adding as a string instead of nested json. Exception: {exc}"
                        )
                        logging.warning(msg)
                        await output_queue.put(msg)
                    new_value = value

                metadata_from_file[key] = new_value

            if COLUMN_TO_USE_AS_GUID in metadata_from_file.keys():
                del metadata_from_file[COLUMN_TO_USE_AS_GUID]

            logging.debug(f"metadata from file: {metadata_from_file}")

            # namespace by metadata source
            metadata = {metadata_source: metadata_from_file}

            if metadata_type:
                metadata["_guid_type"] = metadata_type
            else:
                metadata["_guid_type"] = (
                    GUID_TYPE_FOR_INDEXED_FILE_OBJECT
                    if is_indexed_file_object
                    else GUID_TYPE_FOR_NON_INDEXED_FILE_OBJECT
                )

            logging.debug(f"metadata: {metadata}")

            try:
                await _create_metadata(guid, metadata, auth, commons_url, lock)
                msg = f"Successfully created {guid}"
                logging.info(msg)
                await output_queue.put(msg)
            except Exception as exc:
                logging.debug(
                    f"Got conflict for {guid}. Let's update instead of create..."
                )
                await _update_metadata(guid, metadata, auth, commons_url, lock)
                msg = f"Successfully updated {guid}"
                logging.info(msg)
                await output_queue.put(msg)
        else:
            msg = (
                f"Did not add a metadata object for row because an invalid "
                f"GUID was parsed or no record with this GUID was found in "
                f"indexd: {guid}.\nRow: {row}"
            )
            logging.warning(msg)
            await output_queue.put(msg)


async def _create_metadata(guid, metadata, auth, commons_url, lock):
    """
    Gets a semaphore then creates metadata for guid

    Args:
        guid (str): indexd record globally unique id
        metadata (str): the metadata to add
        auth (Gen3Auth): Gen3 auth or tuple with basic auth name and password
        commons_url (str): root domain for commons where metadata service lives
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
    """
    mds = Gen3Metadata(commons_url, auth_provider=auth)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        response = await mds.async_create(guid, metadata, _ssl=ssl)
        return response


async def _update_metadata(guid, metadata, auth, commons_url, lock):
    """
    Gets a semaphore then updates metadata for guid

    Args:
        guid (str): indexd record globally unique id
        metadata (str): the metadata to add
        auth (Gen3Auth): Gen3 auth or tuple with basic auth name and password
        commons_url (str): root domain for commons where metadata service lives
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
    """
    mds = Gen3Metadata(commons_url, auth_provider=auth)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        response = await mds.async_update(guid, metadata, _ssl=ssl)
        return response


async def _is_indexed_file_object(guid, commons_url, lock):
    """
    Gets a semaphore then requests a record for the given guid

    Args:
        guid (str): indexd record globally unique id
        commons_url (str): root domain for commons where mds lives
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
    """
    index = Gen3Index(commons_url)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        try:
            record = await index.async_get_record(guid, _ssl=ssl)
        except Exception as exc:
            # if error, assume it does not exist
            return False

        return bool(record)


async def async_query_urls_from_indexd(pattern, commons_url, lock):
    """
    Gets a semaphore then requests a record for the given pattern

    Args:
        pattern (str): url pattern to match
        commons_url (str): root domain for commons where mds lives
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
        commons_url (str): root domain for commons where mds lives
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
