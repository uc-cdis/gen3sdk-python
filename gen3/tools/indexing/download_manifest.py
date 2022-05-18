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

from gen3.tools.utils import get_and_verify_fileinfos_from_manifest, MD5_STANDARD_KEY
from gen3.utils import get_or_create_event_loop_for_thread, yield_chunks
from gen3.index import Gen3Index

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
    input_manifest=None,
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
        input_manifest (str): Input file. Read available object data from objects in this
            file instead of reading everything in indexd. This will attempt to query
            indexd for only the records identified in this manifest.
    """
    start_time = time.perf_counter()
    logging.info(f"start time: {start_time}")

    # ensure tmp directories exists and are empty
    os.makedirs(TMP_FOLDER, exist_ok=True)
    os.makedirs(TMP_FOLDER + "input", exist_ok=True)
    os.makedirs(TMP_FOLDER + "output", exist_ok=True)
    for file in os.listdir(TMP_FOLDER):
        file_path = os.path.join(TMP_FOLDER, file)
        if os.path.isfile(file_path):
            os.unlink(file_path)

    for file in os.listdir(TMP_FOLDER + "input"):
        file_path = os.path.join(TMP_FOLDER + "input", file)
        if os.path.isfile(file_path):
            os.unlink(file_path)

    for file in os.listdir(TMP_FOLDER + "output"):
        file_path = os.path.join(TMP_FOLDER + "output", file)
        if os.path.isfile(file_path):
            os.unlink(file_path)

    result = await _write_all_index_records_to_file(
        commons_url,
        output_filename,
        num_processes,
        max_concurrent_requests,
        input_manifest,
    )

    end_time = time.perf_counter()
    logging.info(f"end time: {end_time}")
    logging.info(f"run time: {end_time-start_time}")


async def _write_all_index_records_to_file(
    commons_url, output_filename, num_processes, max_concurrent_requests, input_manifest
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
        input_manifest (str): Input file. Read available object data from objects in this
            file instead of reading everything in indexd. This will attempt to query
            indexd for only the records identified in this manifest.
    """
    # used when requesting all records
    page_chunks = []

    # used when an input manifest is provided, this will only read record info for
    # the records referenced in the manifest based on their checksums
    record_chunks = []

    if input_manifest:
        # create chunks of checksums
        logging.debug(f"parsing input file {input_manifest}")
        input_records, headers = get_and_verify_fileinfos_from_manifest(input_manifest)
        num_input_records = len(input_records)

        if not num_input_records:
            raise AttributeError(
                f"No checksums found in provided input file: {input_manifest}. "
                "Please check previous logs."
            )

        logging.debug(f"number input_records: {num_input_records}")
        logging.debug(f"num processes: {num_processes}")

        input_record_md5s = [record[MD5_STANDARD_KEY] for record in input_records]

        # batch records into subprocesses chunks
        chunk_size = int(math.ceil(float(num_input_records) / num_processes))
        logging.debug(f"records chunk size: {chunk_size}")

        record_chunks = list(yield_chunks(input_record_md5s, chunk_size))
    else:
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

        if chunk_size:
            page_chunks = [
                pages[i : i + chunk_size] for i in range(0, len(pages), chunk_size)
            ]

    processes = []
    for x in range(max(len(page_chunks), len(record_chunks))):
        pages = ",".join(map(str, page_chunks[x])) if page_chunks else ","
        record_checksums = (
            ",".join(map(str, record_chunks[x])) if record_chunks else ","
        )

        # write record_checksum chunks to temporary files since the size can overload
        # command line arguments
        checksums_chunk_filename = TMP_FOLDER + f"input/checksums_chunk_{x}.txt"
        logging.info(
            f"writing record_checksums chunk {x} into {checksums_chunk_filename}"
        )
        with open(checksums_chunk_filename, "wb") as outfile:
            outfile.write(record_checksums.encode("utf8"))

        # call the cli function below and pass in chunks of pages for each process
        command = (
            f"python {CURRENT_DIR}/download_manifest.py --commons_url "
            f"{commons_url} --pages {pages} --record-checksums-file {checksums_chunk_filename} "
            f"--num_processes {num_processes} --max_concurrent_requests {max_concurrent_requests}"
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
        for filename in glob.glob(TMP_FOLDER + "output/*"):
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
    "--record-checksums-file",
    help=(
        "File containing comma-Separated string of checksume to retrieve."
        "ex: /foo/bar.txt"
        'ex file contents: "333e7594cbe3275a152906392e433e8d,0f8deeb44c4f08d794f63af0e4229c97"'
    ),
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
    commons_url, pages, record_checksums_file, num_processes, max_concurrent_requests
):
    """
    Command line interface function for requesting a number of
    records from indexd and writing to a file in that process. num_processes
    is only used to calculate how many open connections this process should request.

    NOTE: YOU MUST USE EITHER `pages` OR `record-checksums-file`, YOU CANNOT USE BOTH

    Args:
        commons_url (str): root domain for commons where indexd lives
        pages (List[int/str]): List of indexd pages to request
        record_checksums_file (str): File with indexd checksums to request
        num_processes (int): number of concurrent processes being requested
            (including this one)
        max_concurrent_requests (int): the maximum number of concurrent requests allowed
            NOTE: This is the TOTAL number, not just for this process. Used to help
            determine how many requests a process should be making at one time

    Raises:
        AttributeError: No pages specified to get records from
    """
    if not pages and not record_checksums_file:
        raise AttributeError(
            "No info specified to get records with. "
            "Supply either pages or record-checksums-file"
        )

    pages = [item for item in pages.strip().strip(",").split(",") if item]
    record_checksums = []

    if record_checksums_file:
        with open(record_checksums_file, "r", encoding="utf8") as file:
            record_checksums_from_file = ",".join(file.readlines())
            record_checksums = [
                item
                for item in record_checksums_from_file.strip().strip(",").split(",")
                if item
            ]

    if not pages and not record_checksums:
        raise AttributeError(
            "No info specified to get records with. "
            "Supply either pages or record-checksums-file with checksums in the file. "
        )

    if pages and record_checksums:
        raise AttributeError(
            "YOU MUST USE EITHER `pages` OR `record-checksums-file`, YOU CANNOT USE BOTH. "
            f"You provided pages={pages} and record-checksums-file={record_checksums_file}."
        )

    loop = get_or_create_event_loop_for_thread()

    result = loop.run_until_complete(
        _get_records_and_write_to_file(
            commons_url, pages, record_checksums, num_processes, max_concurrent_requests
        )
    )
    return result


async def _get_records_and_write_to_file(
    commons_url, pages, record_checksums, num_processes, max_concurrent_requests
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
        record_checksums (List[str]): List of indexd checksums to request
        num_processes (int): number of concurrent processes being requested
            (including this one)
    """
    max_requests = int(max_concurrent_requests / num_processes)
    logging.debug(f"max concurrent requests per process: {max_requests}")
    lock = asyncio.Semaphore(max_requests)
    queue = asyncio.Queue()
    write_to_file_task = asyncio.ensure_future(_parse_from_queue(queue))

    if pages:
        logging.debug("putting records from page into queue")
        await asyncio.gather(
            *(
                _put_records_from_page_in_queue(page, commons_url, lock, queue)
                for page in pages
            )
        )
    else:
        logging.debug("putting records from checksum into queue")
        await asyncio.gather(
            *(
                _put_records_from_checksum_in_queue(checksum, commons_url, lock, queue)
                for checksum in record_checksums
            )
        )

    await queue.put("DONE")
    await write_to_file_task


async def _put_records_from_checksum_in_queue(checksum, commons_url, lock, queue):
    """
    Gets a semaphore then requests records for the given checksum and
    puts them in a queue.

    Args:
        commons_url (str): root domain for commons where indexd lives
        checksum (int/str): indexd checksum to request
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

        records = await index.async_get_records_from_checksum(
            checksum=checksum, _ssl=ssl
        )
        await queue.put(records)


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

    file_name = TMP_FOLDER + f"output/{os.getpid()}.csv"
    async with aiofiles.open(file_name, "w+", encoding="utf8") as file:
        logging.info(f"Writing to {file_name}")
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
