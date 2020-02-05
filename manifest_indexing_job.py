"""
Module for indexing actions using sower job dispatcher

Attributes:
"""

import os
import requests
import json
import logging
import boto3
from botocore.exceptions import ClientError

from gen3.tools.manifest_indexing import manifest_indexing


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return None

    return response


def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client("s3")
    try:
        response = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=expiration,
        )
    except ClientError as e:
        print(e)
        return None

    # The response contains the presigned URL
    return response


def _download_file(url, filename):
    r = requests.get(url)
    with open(filename, "wb") as f:
        f.write(r.content)


if __name__ == "__main__":
    hostname = os.environ["GEN3_HOSTNAME"]
    input_data = os.environ["INPUT_DATA"]

    input_data_json = json.loads(input_data)

    with open("/manifest-indexing-creds.json") as indexing_creds_file:
        indexing_creds = json.load(indexing_creds_file)

    auth = (
        indexing_creds.get("indexd_user", "gdcapi"),
        indexing_creds["indexd_password"],
    )

    filepath = "./manifest_tmp.tsv"
    _download_file(input_data_json["URL"], filepath)

    print("Start to index the manifest ...")

    host_url = input_data_json.get("host")
    if not host_url:
        host_url = "https://{}/index".format(hostname)

    log_ifle, output_manifest = manifest_indexing(
        filepath,
        host_url,
        input_data_json.get("thread_nums", 1),
        auth,
        input_data_json.get("prefix"),
        input_data_json.get("replace_urls"),
    )

    print("[out] {}".format(filepath))
