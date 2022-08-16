"""
Module for indexing actions for verifying a manifest of
indexed file objects (against indexd's API). Supports
multiple processes and coroutines using Python's asyncio library.

The default manifest format created is a Comma-Separated Value file (csv)
with rows for every record. A header row is created with field names:
guid,authz,acl,file_size,md5,urls,file_name

Fields that are lists (like acl, authz, and urls) separate the values with spaces.

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
    CURRENT_DIR (str): directory this file is in
    MAX_CONCURRENT_REQUESTS (int): maximum number of desired concurrent requests across
        processes/threads
"""
import aiohttp
import asyncio
import csv
from cdislogging import get_logger

import os
import time

from gen3.index import Gen3Index
from gen3.utils import get_or_create_event_loop_for_thread

MAX_CONCURRENT_REQUESTS = 24
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

logging = get_logger("__name__")


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


async def async_verify_object_manifest(
    commons_url,
    manifest_file,
    max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
    manifest_row_parsers=manifest_row_parsers,
    manifest_file_delimiter=None,
    output_filename=f"verify-manifest-errors-{time.time()}.log",
):
    """
    Verify all file object records into a manifest csv

    Args:
        commons_url (str): root domain for commons where indexd lives
        manifest_file (str): the file to verify against
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
        manifest_row_parsers (Dict{indexd_field:func_to_parse_row}): Row parsers
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

    logging.debug(f"detected {manifest_file_delimiter} as delimiter between columns")

    await _verify_all_index_records_in_file(
        commons_url,
        manifest_file,
        manifest_file_delimiter,
        max_concurrent_requests,
        output_filename,
    )

    end_time = time.perf_counter()
    logging.info(f"end time: {end_time}")
    logging.info(f"run time: {end_time-start_time}")


async def _verify_all_index_records_in_file(
    commons_url,
    manifest_file,
    manifest_file_delimiter,
    max_concurrent_requests,
    output_filename,
):
    """
    Getting indexd records and writing to a file. This function
    creates semaphores to limit the number of concurrent http connections that
    get opened to send requests to indexd.

    It then uses asyncio to start a number of coroutines. Steps:
        1) requests to indexd to get records (writes resulting records to a queue)
        2) puts a final "DONE" in the queue to stop coroutine that will read from queue
        3) reading those records from the queue and writing to a file

    Args:
        commons_url (str): root domain for commons where indexd lives
        manifest_file (str): the file to verify against
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
                if value:
                    value = value.strip()

                new_row[key.strip()] = value
            await queue.put(new_row)

    for _ in range(0, int(max_concurrent_requests + (max_concurrent_requests / 4))):
        await queue.put("DONE")

    await asyncio.gather(
        *(
            _parse_from_queue(queue, lock, commons_url, output_queue)
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


async def _parse_from_queue(queue, lock, commons_url, output_queue):
    """
    Keep getting items from the queue and verifying that indexd contains the expected
    fields from that row. If there are any issues, log errors into a file. Return
    when nothing is left in the queue.

    Args:
        queue (asyncio.Queue): queue to read indexd records from
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
        commons_url (str): root domain for commons where indexd lives
        output_queue (asyncio.Queue): queue for output
    """
    loop = get_or_create_event_loop_for_thread()

    row = await queue.get()

    while row != "DONE":
        guid = manifest_row_parsers["guid"](row)
        authz = manifest_row_parsers["authz"](row)
        acl = manifest_row_parsers["acl"](row)
        file_size = manifest_row_parsers["file_size"](row)
        md5 = manifest_row_parsers["md5"](row)
        urls = manifest_row_parsers["urls"](row)
        file_name = manifest_row_parsers["file_name"](row)

        actual_record = await _get_record_from_indexd(guid, commons_url, lock)
        if not actual_record:
            output = f"{guid}|no_record|expected {row}|actual None\n"
            await output_queue.put(output)
            logging.error(output)
        else:
            logging.info(f"verifying {guid}...")

            if sorted(authz) != sorted(actual_record["authz"]):
                output = (
                    f"{guid}|authz|expected {authz}|actual {actual_record['authz']}\n"
                )
                await output_queue.put(output)
                logging.error(output)

            if sorted(acl) != sorted(actual_record["acl"]):
                output = f"{guid}|acl|expected {acl}|actual {actual_record['acl']}\n"
                await output_queue.put(output)
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
                    await output_queue.put(output)
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
                    await output_queue.put(output)
                    logging.error(output)
            urls = [url.replace("%20", " ") for url in urls]
            if sorted(urls) != sorted(actual_record["urls"]):
                output = f"{guid}|urls|expected {urls}|actual {actual_record['urls']}\n"
                await output_queue.put(output)
                logging.error(output)

            if not actual_record["file_name"] and file_name:
                # if the actual record name is "" or None but something was specified
                # in the manifest, we have a problem
                output = f"{guid}|file_name|expected {file_name}|actual {actual_record['file_name']}\n"
                await output_queue.put(output)
                logging.error(output)

        row = await queue.get()


async def _get_record_from_indexd(guid, commons_url, lock):
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

        record = None

        try:
            return await index.async_get_record(guid, _ssl=ssl)

        except aiohttp.client_exceptions.ClientResponseError as exc:
            logging.warning(f"couldn't get record. error: {exc}")

        return record
