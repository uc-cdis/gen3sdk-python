"""
Module for running post-indexing validation.
validate_manifest takes as input a manifest, an api credentials file, and an output path
then attemps to obtain a pre-signed url and download a file from each bucket
to verify it has been indexed, and then generate a report in csv format.

The output format is as follows:
| ACL | Bucket | Protocol | Presigned URL Status | Download Status | GUID |
"""


import csv
from cdislogging import get_logger, get_stream_handler
from gen3.file import Gen3File
import requests


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
        self.response_status = -1
        self.download_status = -1
        self.headers = {
            "accept": "application/json",
            "authorization": f"bearer {access_token}",
        }

    def check_record(self, gen3file):
        """
        Checks the status of a record by generating a pre-signed URL and attempting to download the file.

        This method performs the following actions:
        1. Attempts to generate a pre-signed URL for the record identified by `self.guid` using the provided `gen3file` object.
        2. Logs the result of the pre-signed URL generation, including the response status code.
        3. If the URL is successfully generated (status code 200), it attempts to download the file from the generated URL.
        4. Logs the result of the download attempt, including the status code.
        5. Sets the `response_status` attribute to indicate the success or failure of the pre-signed URL generation.
        6. Sets the `download_status` attribute to indicate the success or failure of the download attempt (if applicable).

        Args:
            gen3file (object): An object that provides the `get_presigned_url` method to generate a pre-signed URL for the record.

        Returns:
            None: This function does not return any value. It modifies the `response_status` and `download_status` attributes.
        """
        logger.info(f"Checking record {self.guid}")
        try:
            resp = gen3file.get_presigned_url(self.guid)
            url = resp.get("url")
            response_status = 200
            logger.info(
                f"Pre-signed url successfully generated for record {self.guid} with status code {response_status}"
            )
        except requests.HTTPError as err:
            response_status = err.response.status_code
            logger.info(f"Pre-signed url generation failed for record {self.guid}")
        self.response_status = response_status

        download_success = -1
        if response_status == 200:
            try:
                download_success = requests.get(url).status_code
                logger.info(
                    f"Download process complete with status code {download_success}"
                )
            except:
                download_success = -1
        self.download_status = download_success
        return


class Records:
    def __init__(self, auth):
        self.auth = auth
        self.access_token = auth.get_access_token()
        self.commons = auth.endpoint
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
                                if key not in self.record_dict or (
                                    int(self.record_dict[key].size) >= int(size)
                                    and int(size) != 0
                                ):
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

    def check_records(self):
        """
        Iterates through all records in `self.record_dict` and checks each record's status.

        This method performs the following actions:
        1. Initializes a `Gen3File` object using the authentication information from `self.auth`.
        2. Iterates over the items in `self.record_dict`, where each item consists of a tuple `(bucket, protocol, acl)` and a `record` object.
        3. For each record, the `check_record` method is called, which attempts to generate a pre-signed URL and check the download status.

        Args:
            None: This method does not take any arguments beyond `self`.

        Returns:
            None: This function does not return any value. It triggers the `check_record` method for each record in `self.record_dict`.
        """
        gen3file = Gen3File(self.auth)
        for (bucket, protocol, acl), record in self.record_dict.items():
            record.check_record(gen3file)

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
            "acl",
            "bucket",
            "protocol",
            "presigned_url_status",
            "download_status",
            "guid",
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


def validate_manifest(MANIFEST, auth, output_file="results.csv"):
    """
    Takes as input a manifest location, a Gen3Auth instance, and an output file
    Attempts to obtain a presigned url from a record from each bucket then download the file.
    Outputs report in csv format.

    Args:
        MANIFEST (str): the location of a manifest file
        api_key (str): the location of an api credentials file
        auth (str): a Gen3Auth instance
    """
    logger.info("Starting...")
    records = Records(auth)
    records.read_records_from_manifest(MANIFEST)
    records.check_records()
    records.save_download_check_results_to_csv(output_file)
    return records
