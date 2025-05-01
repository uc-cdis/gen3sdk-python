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
import re


logger = get_logger(__name__)
logger.addHandler(get_stream_handler())
logger.setLevel("INFO")


class GuidError(Exception):
    pass


class ManifestError(Exception):
    pass


class Record:
    def __init__(self, guid, bucket, protocol, acl, size):
        self.guid = guid
        self.bucket = bucket
        self.protocol = protocol
        self.acl = acl
        self.size = size
        self.response_status = -1
        self.download_status = -1

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

        if response_status == 200:
            try:
                self.download_status = requests.get(url).status_code
                logger.info(
                    f"Download process complete with status code {self.download_status}"
                )
            except Exception as err:
                self.download_status = -1
                logger.info(
                    f"Download process failed due to {err}"
                )  # maybe should be logger.error
        return


class RecordParser:
    def __init__(self, auth):
        self.auth = auth
        self.record_dict = {}
        self.record_sizes = {}

    def read_records_from_manifest(self, manifest):
        """
        Parses a manifest and creates a dictionary of Record objects.

        Args:
            manifest (str): the location of a manifest file
        """
        tsv_pattern = "^.*\.tsv$"
        csv_pattern = "^.*\.csv$"
        if re.match(tsv_pattern, manifest):
            sep = "\t"
        elif re.match(csv_pattern, manifest):
            sep = ","
        else:
            raise ManifestError(
                "Please enter the path to a valid manifest in .csv or .tsv format"
            )

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

            url_pattern = r"^[a-zA-Z][a-zA-Z0-9+.-]*://[^\s/]+(?:/[^\s]*)*$"
            for row in rows:
                size = row["size"]
                guid = row[guid_col]
                for acl in row["acl"].split(" "):
                    if acl != "admin":
                        for url in row["url"].split(" "):
                            url = url.replace("[", "").replace("]", "")
                            if re.match(url_pattern, url) == None:
                                raise ManifestError(
                                    f"Manifest contains guid {guid} with invalid url format: {url}"
                                )
                            else:
                                protocol, bucket = (
                                    url.split("://")[0].replace("[", ""),
                                    url.split("/")[2],
                                )
                                key = (
                                    bucket,
                                    protocol,
                                    acl,
                                )  # Check a record for each unique (bucket, protocol, acl) combination
                                if (
                                    key not in self.record_dict
                                    or (  # Add record to the list of records if no matching (bucket,protocol,acl) found
                                        int(self.record_dict[key].size)
                                        >= int(
                                            size
                                        )  # Update to download smallest non-zero sized record
                                        and int(size)
                                        != 0  # Make sure record has non-zero size
                                    )
                                ):  # If it passes these we temporarily choose this record to check for the bucket, protocol, and acl
                                    record = Record(
                                        guid,
                                        bucket,
                                        protocol,
                                        acl,
                                        size,
                                    )
                                    self.record_dict[key] = record

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
        self.download_results = []
        for record in self.record_dict.values():
            self.download_results.append(
                {
                    "acl": record.acl,
                    "bucket": record.bucket,
                    "protocol": record.protocol,
                    "presigned_url_status": record.response_status,
                    "download_status": record.download_status,
                    "guid": record.guid,
                }
            )

        self.download_results = self.download_results

        # Check if the results list is empty
        if not self.download_results:
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
            for result in self.download_results:
                writer.writerow(
                    {
                        "acl": result["acl"],
                        "bucket": result["bucket"],
                        "protocol": result["protocol"],
                        "presigned_url_status": result["presigned_url_status"],
                        "download_status": result["download_status"],
                        "guid": result["guid"],
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
    records = RecordParser(auth)
    records.read_records_from_manifest(MANIFEST)
    records.check_records()
    records.save_download_check_results_to_csv(output_file)
    return records
