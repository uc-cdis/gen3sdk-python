"""
Module for indexing actions for downloading a manifest of
indexed file objects (against indexd's API). Supports
multiple processes and coroutines using Python's asyncio library.

The default manifest format created is a Comma-Separated Value file (csv)
with rows for every record. A header row is created with field names:
guid,authz,acl,file_size,md5,urls,file_name

Fields that are lists (like acl, authz, and urls) separate the values with spaces.

Attributes:
    CURRENT_DIR (str): directory this file is in
    INDEXD_RECORD_PAGE_SIZE (int): number of records to request per page
    MAX_CONCURRENT_REQUESTS (int): maximum number of desired concurrent requests across
        processes/threads
    TMP_FOLDER (str): Folder directory for placing temporary files
        NOTE - We have to use a temporary folder b/c Python's file writing is not
              thread-safe so we can't have all processes writing to the same file.
              To workaround this, we have each process write to a file and concat
              them all post-processing.
"""
import asyncio
import aiofiles
import click
import time
import csv
import glob
from cdislogging import get_logger

import os
import sys
import shutil
import math

from gen3.index import Gen3Index
from gen3.utils import get_or_create_event_loop_for_thread

INDEXD_RECORD_PAGE_SIZE = 1024
MAX_CONCURRENT_REQUESTS = 24
CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
TMP_FOLDER = os.path.abspath(CURRENT_DIR + "/tmp") + "/"

logging = get_logger("__name__")


async def async_download_object_manifest(
    commons_url,
    output_filename="object-manifest.csv",
    num_processes=4,
    max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
):
    """
    Download all file object records into a manifest csv

    Args:
        commons_url (str): root domain for commons where indexd lives
        output_filename (str, optional): filename for output
        num_processes (int, optional): number of parallel python processes to use for
          hitting indexd api and processing
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
            NOTE: This is the TOTAL number, not just for this process. Used to help
            determine how many requests a process should be making at one time
    """
    start_time = time.perf_counter()
    logging.info(f"start time: {start_time}")

    # ensure tmp directory exists and is empty
    os.makedirs(TMP_FOLDER, exist_ok=True)
    for file in os.listdir(TMP_FOLDER):
        file_path = os.path.join(TMP_FOLDER, file)
        if os.path.isfile(file_path):
            os.unlink(file_path)

    result = await _write_all_index_records_to_file(
        commons_url, output_filename, num_processes, max_concurrent_requests
    )

    end_time = time.perf_counter()
    logging.info(f"end time: {end_time}")
    logging.info(f"run time: {end_time-start_time}")


async def _write_all_index_records_to_file(
    commons_url, output_filename, num_processes, max_concurrent_requests
):
    """
    Spins up number of processes provided to parse indexd records and eventually
    write to a single output file manifest.

    Args:
        commons_url (str): root domain for commons where indexd lives
        output_filename (str, optional): filename for output
        num_processes (int, optional): number of parallel python processes to use for
          hitting indexd api and processing
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
            NOTE: This is the TOTAL number, not just for this process. Used to help
            determine how many requests a process should be making at one time
    """
    index = Gen3Index(commons_url)
    logging.debug(f"requesting indexd stats...")
    num_files = int(index.get_stats().get("fileCount"))
    logging.debug(f"number files: {num_files}")
    # paging is 0-based, so subtract 1 from ceiling
    # note: float() is necessary to force Python 3 to not floor the result
    max_page = int(math.ceil(float(num_files) / INDEXD_RECORD_PAGE_SIZE)) - 1
    logging.debug(f"max page: {max_page}")
    logging.debug(f"num processes: {num_processes}")

    pages = [x for x in range(max_page + 1)]

    # batch pages into subprocesses
    chunk_size = int(math.ceil(float(len(pages)) / num_processes))
    logging.debug(f"page chunk size: {chunk_size}")

    if not chunk_size:
        page_chunks = []
    else:
        page_chunks = [
            pages[i : i + chunk_size] for i in range(0, len(pages), chunk_size)
        ]

    processes = []
    for x in range(len(page_chunks)):
        pages = ",".join(map(str, page_chunks[x]))

        # call the cli function below and pass in chunks of pages for each process
        command = (
            f"python {CURRENT_DIR}/download_manifest.py --commons_url "
            f"{commons_url} --pages {pages} --num_processes {num_processes} "
            f"--max_concurrent_requests {max_concurrent_requests}"
        )
        logging.info(command)

        process = await asyncio.create_subprocess_shell(command)

        logging.info(f"Process_{process.pid} - Started w/: {command}")
        processes.append(process)

    for process in processes:
        # wait for the subprocesses to finish
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logging.info(f"Process_{process.pid} - Done")
        else:
            logging.info(f"Process_{process.pid} - FAILED")

    logging.info(f"done processing, combining outputs to single file {output_filename}")

    # remove existing output if it exists
    if os.path.isfile(output_filename):
        os.unlink(output_filename)

    with open(output_filename, "wb") as outfile:
        outfile.write("guid,urls,authz,acl,md5,file_size,file_name\n".encode("utf8"))
        for filename in glob.glob(TMP_FOLDER + "*"):
            if output_filename == filename:
                # don't want to copy the output into the output
                continue
            logging.info(f"combining {filename} into {output_filename}")
            with open(filename, "rb") as readfile:
                shutil.copyfileobj(readfile, outfile)

    logging.info(f"done writing output to file {output_filename}")


@click.command()
@click.option(
    "--commons_url", help="Root domain (url) for a commons containing indexd."
)
@click.option(
    "--pages",
    help='Comma-Separated string of integers representing pages. ex: "2,4,5,6"',
)
@click.option(
    "--num_processes",
    type=int,
    help="number of processes you are running so we can make sure we don't open "
    'too many http connections. ex: "4"',
)
@click.option(
    "--max_concurrent_requests",
    type=int,
    help="number of processes you are running so we can make sure we don't open "
    'too many http connections. ex: "4"',
)
def write_page_records_to_files(
    commons_url, pages, num_processes, max_concurrent_requests
):
    """
    Command line interface function for requesting a number of pages of
    records from indexd and writing to a file in that process. num_processes
    is only used to calculate how many open connections this process should request.

    Args:
        commons_url (str): root domain for commons where indexd lives
        pages (List[int/str]): List of indexd pages to request
        num_processes (int): number of concurrent processes being requested
            (including this one)
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
            NOTE: This is the TOTAL number, not just for this process. Used to help
            determine how many requests a process should be making at one time

    Raises:
        AttributeError: No pages specified to get records from
    """
    if not pages:
        raise AttributeError("No pages specified to get records from.")

    pages = pages.strip().split(",")
    loop = get_or_create_event_loop_for_thread()

    result = loop.run_until_complete(
        _get_records_and_write_to_file(
            commons_url, pages, num_processes, max_concurrent_requests
        )
    )
    return result


async def _get_records_and_write_to_file(
    commons_url, pages, num_processes, max_concurrent_requests
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
        pages (List[int/str]): List of indexd pages to request
        num_processes (int): number of concurrent processes being requested
            (including this one)
    """
    max_requests = int(max_concurrent_requests / num_processes)
    logging.debug(f"max concurrent requests per process: {max_requests}")
    lock = asyncio.Semaphore(max_requests)
    queue = asyncio.Queue()
    write_to_file_task = asyncio.ensure_future(_parse_from_queue(queue))
    await asyncio.gather(
        *(
            _put_records_from_page_in_queue(page, commons_url, lock, queue)
            for page in pages
        )
    )
    await queue.put("DONE")
    await write_to_file_task


async def _put_records_from_page_in_queue(page, commons_url, lock, queue):
    """
    Gets a semaphore then requests records for the given page and
    puts them in a queue.

    Args:
        commons_url (str): root domain for commons where indexd lives
        page (int/str): indexd page to request
        lock (asyncio.Semaphore): semaphones used to limit ammount of concurrent http
            connections
        queue (asyncio.Queue): queue to put indexd records in
    """
    index = Gen3Index(commons_url)
    async with lock:
        # default ssl handling unless it's explicitly http://
        ssl = None
        if "https" not in commons_url:
            ssl = False

        records = await index.async_get_records_on_page(
            page=page, limit=INDEXD_RECORD_PAGE_SIZE, _ssl=ssl
        )
        await queue.put(records)


async def _parse_from_queue(queue):
    """
    Read from the queue and write to a file

    Args:
        queue (asyncio.Queue): queue to read indexd records from
    """
    loop = get_or_create_event_loop_for_thread()

    file_name = TMP_FOLDER + f"{os.getpid()}.csv"
    async with aiofiles.open(file_name, "w+", encoding="utf8") as file:
        logging.info(f"Write to {file_name}")
        csv_writer = csv.writer(file)

        records = await queue.get()
        while records != "DONE":
            if records:
                for record in list(records):
                    manifest_row = [
                        record.get("did"),
                        " ".join(
                            sorted(
                                [url.replace(" ", "%20") for url in record.get("urls")]
                            )
                        ),
                        " ".join(
                            sorted(
                                [
                                    auth.replace(" ", "%20")
                                    for auth in record.get("authz")
                                ]
                            )
                        ),
                        " ".join(
                            sorted([a.replace(" ", "%20") for a in record.get("acl")])
                        ),
                        record.get("hashes", {}).get("md5"),
                        record.get("size"),
                        record.get("file_name"),
                    ]
                    await csv_writer.writerow(manifest_row)

            records = await queue.get()

        file.flush()


if __name__ == "__main__":
    write_page_records_to_files()
