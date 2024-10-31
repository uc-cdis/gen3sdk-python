"""
Module for running post-indexing validation.
validate_manifest takes as input a manifest, an api credentials file, and an output path
then runs an async coroutine attempting to obtain a pre-signed url, download a file from each bucket
to verify it has been indexed, and generate a report in csv format.

The output format is as follows:
| ACL | Bucket | Protocol | Presigned URL Status | Download Status | GUID |
"""


import csv
import aiohttp
import asyncio
from gen3.auth import Gen3Auth
from cdislogging import get_logger, get_stream_handler


logger = get_logger(__name__)
logger.addHandler(get_stream_handler())
logger.setLevel("INFO")


class GuidError(Exception):
    pass


class Record:
    def __init__(self, guid, bucket, protocol, acl, access_token, commons, size):
        self.guid = guid
        self.bucket = bucket
        self.protocol = protocol
        self.acl = acl
        self.commons = commons
        self.size = size
        self.download_status = -1
        self.headers = {
            "accept": "application/json",
            "authorization": f"bearer {access_token}",
        }

    async def get_presigned_url(self, semaphore, session):
        """
        Generates a presigned_url for the given guid and protocol at the specified commons.

        Runs concurrently as many as the provided semaphore will allow.

        Args:
            semaphore: an asyncio semaphore
            session: an aiohttp client session connected to the commons

        """
        async with semaphore:
            try:
                url = (
                    self.commons
                    + f"/user/data/download/{self.guid}?protocol={self.protocol}"
                )
                async with session.get(url, headers=self.headers) as response:
                    if response.ok:
                        self.response, self.response_status = (
                            await response.json(),
                            response.status,
                        )
                    else:
                        logger.info(
                            f"Response unsuccessful; returned with status {response.status}"
                        )
                        self.response, self.response_status = {}, response.status
            except Exception as e:
                logger.error(f"Error fetching presigned URL {url}: {e}")
                self.response, self.response_status = {}, -1

            self.presigned_url = None
            if self.response:
                logger.info(f"presigned-url response: {self.response}")
                self.presigned_url = self.response.get("url")
            else:
                logger.warning(
                    f"FAILED TO GET PRESIGNED-URL: status_code = {self.response_status}"
                )

    async def download_file(self, semaphore, session):
        """
        Downloads a file from the provided url, which is a generated presigned_url.

        Runs concurrently as many as the provided semaphore will allow.

        Args:
            semaphore: an asyncio semaphore
            session: an aiohttp client session connected to the commons

        """
        async with semaphore:
            try:
                async with session.get(
                    self.presigned_url, headers={"Range": "bytes=0-1023"}
                ) as response:
                    if response.ok:
                        await response.read()
                        self.download_status = response.status
                    else:
                        self.download_status = response.status
            except Exception as e:
                logger.error(f"Error downloading file from {self.presigned_url}: {e}")
                self.download_status = -1


class Records:
    def __init__(self, semaphore, session, auth):
        self.access_token = auth.get_access_token()
        self.commons = auth.endpoint
        self.semaphore = semaphore
        self.session = session
        self.record_dict = {}
        self.record_sizes = {}
        self.headers = {
            "accept": "application/json",
            "authorization": f"bearer {self.access_token}",
        }

    def read_records_from_manifest(self, manifest):
        """
        Parses a manifest and creates a dictionary of Record objects.

        Args:
            manifest (str): the location of a manifest file
        """
        if manifest[-3:] == "tsv":
            sep = "\t"
        else:
            sep = ","
        with open(manifest, mode="r") as f:
            csv_reader = csv.DictReader(f, delimiter=sep)
            rows = [row for row in csv_reader]

            try:
                guid_cols = {"GUID", "guid", "id"}
                guid_col = list(guid_cols.intersection(set(csv_reader.fieldnames)))[0]
            except IndexError:
                raise GuidError(
                    "Manifest file has no column named 'GUID', 'guid', or 'id'"
                )

            for row in rows:
                url_parsed = False
                size = row["size"]
                guid = row[guid_col]
                for acl in row["acl"].split(" "):
                    if acl != "admin":
                        for url in row["url"].split(" "):
                            if "://" not in url:
                                continue
                            else:
                                protocol, bucket = (
                                    url.split("://")[0].replace("[", ""),
                                    url.split("/")[2],
                                )
                                key = (bucket, protocol, acl)
                                if key not in self.record_dict or int(
                                    self.record_dict[key].size
                                ) >= int(size):
                                    record = Record(
                                        guid,
                                        bucket,
                                        protocol,
                                        acl,
                                        self.access_token,
                                        self.commons,
                                        size,
                                    )
                                    self.record_dict[key] = record
                                url_parsed = True

                if url_parsed == False:
                    logger.warning(f"No url parsed for record {guid}")

    async def get_presigned_url_list(self):
        """
        Attempts to obtain a presigned url for every record.
        Runs concurrently according to defined semaphore.
        """
        tasks = []
        for (bucket, protocol, acl), record in self.record_dict.items():
            logger.info(
                f"picking one indexd record with GUID {record.guid} from acl {acl}"
            )
            logger.info(f"checking guid {record.guid} with protocol {protocol}...")
            tasks.append(
                (
                    record.get_presigned_url(self.semaphore, self.session),
                    protocol,
                    acl,
                    bucket,
                    record.guid,
                )
            )

        self.presigned_results = await asyncio.gather(*[task[0] for task in tasks])

    async def download_files(self):
        """
        Attempts to download a file for every record where a presigned url was able to be obtained.
        Runs concurrently according to defined semaphore.
        """
        download_tasks = []
        for (bucket, protocol, acl), record in self.record_dict.items():
            if record.presigned_url:
                logger.info(
                    f"Attempting to download from presigned URL: {record.presigned_url}"
                )
                download_tasks.append(
                    (
                        record.download_file(self.semaphore, self.session),
                        record.protocol,
                        record.acl,
                        record.response_status,
                        record.bucket,
                        record.guid,
                    )
                )
            else:
                logger.warning(
                    f"No presigned url associated with record {record.guid} from acl {acl}"
                )
                download_tasks.append(
                    (
                        None,
                        record.protocol,
                        record.acl,
                        record.response_status,
                        record.bucket,
                        record.guid,
                    )
                )

        self.download_responses = await asyncio.gather(
            *[task[0] for task in download_tasks if task[0] is not None]
        )

    def save_download_check_results_to_csv(self, csv_filename):
        """
        Generates results from presigned url generation and file downloads.
        Output format is: | ACL | Bucket | Protocol | Presigned URL Status | Download Status | GUID |

        Args:
            csv_filename (str): the relative file path of the output csv
        """
        download_results = []
        for record in self.record_dict.values():
            download_results.append(
                {
                    "acl": record.acl,
                    "bucket": record.bucket,
                    "protocol": record.protocol,
                    "presigned_url_status": record.response_status,
                    "download_status": record.download_status,
                    "guid": record.guid,
                }
            )

        self.download_results = download_results

        # Check if the results list is empty
        if not download_results:
            logger.warning("No results to save.")
            return

        # Define the CSV file header
        fieldnames = [
            "ACL",
            "Bucket",
            "Protocol",
            "Presigned URL Status",
            "Download Status",
            "GUID",
        ]

        with open(csv_filename, mode="w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            # Write the header row
            writer.writeheader()

            # Iterate through the DownloadCheckResult instances and write each row
            for result in download_results:
                writer.writerow(
                    {
                        "ACL": result["acl"],
                        "Bucket": result["bucket"],
                        "Protocol": result["protocol"],
                        "Presigned URL Status": result["presigned_url_status"],
                        "Download Status": result["download_status"],
                        "GUID": result["guid"],
                    }
                )

        logger.info(f"Results saved to {csv_filename}")


async def _validate_manifest_coro(MANIFEST, auth, output_file):
    """
    Manifest validation coroutine.
    Takes as input a manifest location, the location of an api credentials file, and an output file
    Attempts to obtain a presigned url from a record from each bucket then download the file.
    Outputs report in csv format.

    Args:
        MANIFEST (str): the location of a manifest file
        api_key (str): the location of an api credentials file
        output_file (str): the relative path of the output csv
    """
    logger.info("STARTING...")
    concurrent_limit = 5

    semaphore = asyncio.Semaphore(concurrent_limit)

    async with aiohttp.ClientSession() as session:
        records = Records(semaphore, session, auth)

        records.read_records_from_manifest(MANIFEST)
        await records.get_presigned_url_list()
        await records.download_files()
        records.save_download_check_results_to_csv(output_file)
        return records


def validate_manifest(MANIFEST, auth, output_file="results.csv"):
    """
    The driver for _validate_manifest_coro

    Args:
        MANIFEST (str): the location of a manifest file
        api_key (str): the location of an api credentials file
        output_file (str): the relative path of the output csv
    """
    return asyncio.run(_validate_manifest_coro(MANIFEST, auth, output_file))
