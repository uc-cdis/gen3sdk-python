import asyncio
import csv
from cdislogging import get_logger
import os
import copy
import time

from gen3.index import Gen3Index
from gen3 import index
from gen3.utils import get_urls, get_or_create_event_loop_for_thread

MAX_CONCURRENT_REQUESTS = 24
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

logging = get_logger("__name__")


async def async_fix_indexd_for_verify_error_log(
    commons_url,
    verify_output_filename,
    auth,
    max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
    output_filename=f"fix-output-{time.time()}.log",
):
    """
    Fix all file object records into a manifest csv

    Args:
        commons_url (str): root domain for commons where indexd lives
        verify_output_filename (str): the file to fix against
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
        manifest_row_parsers (Dict{indexd_field:func_to_parse_row}): Row parsers
        output_filename (str): filename for output logs
    """
    start_time = time.perf_counter()
    logging.info(f"start time: {start_time}")

    await _fix_all_index_records_in_file(
        commons_url,
        verify_output_filename,
        auth,
        max_concurrent_requests,
        output_filename.split("/")[-1],
    )

    end_time = time.perf_counter()
    logging.info(f"end time: {end_time}")
    logging.info(f"run time: {end_time-start_time}")


async def _fix_all_index_records_in_file(
    commons_url, verify_output_filename, auth, max_concurrent_requests, output_filename
):
    """
    Getting indexd records and writing to a file. This function
    creates semaphores to limit the number of concurrent http connections that
    get opened to send requests to indexd.

    It then uses asyncio to start a number of coroutines. Steps:
        1) requests to indexd to get records (writes resulting records to a queue)
        2) reading those records from the queue and writing to a file

    Args:
        commons_url (str): root domain for commons where indexd lives
        verify_output_filename (str): the file to fix against
        output_filename (str, optional): filename for output
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
    """
    max_requests = int(max_concurrent_requests)
    logging.debug(f"max concurrent requests: {max_requests}")
    lock = asyncio.Semaphore(max_requests)
    queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    logging.info(f"adding rows from {verify_output_filename} to queue...")

    with open(verify_output_filename, encoding="utf-8-sig") as file:
        for row in file:
            guid, error, expected, actual = [
                str(item).strip() for item in row.split("|") if item
            ]

            await queue.put(str(row).strip("\n"))

    logging.info(f"done adding to queue")

    await asyncio.gather(
        *(
            _parse_from_queue(queue, lock, commons_url, output_queue, auth)
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


async def _parse_from_queue(queue, lock, commons_url, output_queue, auth):
    """
    Keep getting items from the queue and fixing so that indexd contains the expected
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

    while not queue.empty():
        row = await queue.get()

        index = Gen3Index(commons_url, auth_provider=auth)

        guid, error, expected, actual = [
            str(item).strip() for item in row.split("|") if item
        ]
        expected = expected.replace("expected", "").strip(" ")
        actual = actual.replace("actual", "").strip(" ")

        try:
            actual_record = copy.deepcopy(
                await _get_record_from_indexd(guid, commons_url, lock)
            )
        except Exception as exc:
            msg = f"getting {guid} failed. error {exc}\n"
            output_queue.put(msg)
            logging.error(msg)

            row = queue.get()
            continue

        if error == "acl":
            acls = [
                expected,
                actual.strip("\n").strip("[").strip("]").strip("'").strip('"'),
            ]
            msg = f"updating {guid} setting acls: {acls}\n"
            output_queue.put(msg)
            logging.info(msg)

            response = await _update_record_from_indexd(
                guid, commons_url, lock, auth, acl=acls
            )
            msg = f"{guid}: update response {response}\n"
            output_queue.put(msg)
            logging.debug(msg)
        if error == "authz":
            authz = [
                expected,
                actual.strip("\n").strip("[").strip("]").strip("'").strip('"'),
            ]
            msg = f"updating {guid} setting authz: {authz}\n"
            output_queue.put(msg)
            logging.info(msg)

            response = await _update_record_from_indexd(
                guid, commons_url, lock, auth, authz=authz
            )
            msg = f"{guid}: update response {response}\n"
            output_queue.put(msg)
            logging.debug(msg)
        if error == "file_name":
            file_name = expected
            msg = f"updating {guid} setting file_name: {file_name}\n"
            output_queue.put(msg)
            logging.info(msg)

            response = await _update_record_from_indexd(
                guid, commons_url, lock, auth, file_name=file_name
            )
            msg = f"{guid}: update response {response}\n"
            output_queue.put(msg)
            logging.debug(msg)
        elif error == "size":
            raise NotImplementedError("size correction not officially supported yet")

            msg = f"getting {guid}, then deleting and recreating with size {expected}\n"
            output_queue.put(msg)
            logging.info(msg)

            msg = f"{guid}: get response {actual_record}\n"
            output_queue.put(msg)
            logging.debug(msg)
            try:
                response = index.delete_record(guid)
                msg = f"{guid}: delete response {response}\n"
                output_queue.put(msg)
                logging.info(msg)
            except Exception as exc:
                msg = f"{guid}: tried to delete. ignoring error {exc}\n"
                output_queue.put(msg)
                logging.error(msg)
                pass

            actual_record["size"] = int(expected)

            del actual_record["rev"]
            del actual_record["created_date"]
            del actual_record["form"]
            del actual_record["updated_date"]
            del actual_record["uploader"]
            del actual_record["urls_metadata"]

            msg = f"{guid}: creating {actual_record}\n"
            output_queue.put(msg)
            logging.info(msg)
            try:
                response = index.create_record(**actual_record)
                msg = f"{guid}: create response {response}\n"
                output_queue.put(msg)
                logging.info(msg)
            except Exception as exc:
                msg = f"{guid}: tried to create. ignoring error {exc}\n"
                output_queue.put(msg)
                logging.error(msg)
                pass

        elif error == "md5":
            raise NotImplementedError("md5 correction not officially supported yet")

            msg = f"getting {guid}, then deleting and recreating with md5 {expected}\n"
            output_queue.put(msg)
            logging.info(msg)

            msg = f"{guid}: get response {actual_record}\n"
            output_queue.put(msg)
            logging.debug(msg)
            try:
                response = index.delete_record(guid)
                msg = f"{guid}: delete response {response}\n"
                output_queue.put(msg)
                logging.info(msg)
            except Exception as exc:
                msg = f"{guid}: tried to delete. ignoring error {exc}\n"
                output_queue.put(msg)
                logging.error(msg)
                pass

            actual_record["hashes"]["md5"] = expected

            del actual_record["rev"]
            del actual_record["created_date"]
            del actual_record["form"]
            del actual_record["updated_date"]
            del actual_record["uploader"]
            del actual_record["urls_metadata"]

            msg = f"{guid}: creating {actual_record}\n"
            output_queue.put(msg)
            logging.debug(msg)
            try:
                response = index.create_record(**actual_record)
                msg = f"{guid}: create response {response}\n"
                output_queue.put(msg)
                logging.info(msg)
            except Exception as exc:
                msg = f"{guid}: tried to create. ignoring error {exc}\n"
                output_queue.put(msg)
                logging.error(msg)
                pass
        elif error == "url" or error == "urls":
            # raise NotImplementedError("url correction not officially supported yet")

            urls = get_urls(expected)
            msg = f"updating {guid} setting urls: {urls}\n"
            output_queue.put(msg)
            logging.info(msg)

            response = index.update_record(
                guid, urls=urls, urls_metadata={url: {} for url in urls}
            )
            msg = f"{guid}: update response {response}\n"
            output_queue.put(msg)
            logging.debug(msg)
        else:
            raise NotImplementedError(
                f"{error} correction not officially supported yet"
            )

            msg = f"WARNING:Did not fix {error} error for row {row}\n"
            output_queue.put(msg)
            logging.warning(msg)


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

        return await index.async_get_record(guid, _ssl=ssl)


async def _update_record_from_indexd(guid, commons_url, lock, auth, **kwargs):
    """
    Updates a semaphore then requests a record for the given guid

    Args:
        guid (str): indexd record globally unique id
        commons_url (str): root domain for commons where indexd lives
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
    """
    index = Gen3Index(commons_url, auth_provider=auth)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        return await index.async_update_record(guid, auth=auth, _ssl=ssl, **kwargs)
