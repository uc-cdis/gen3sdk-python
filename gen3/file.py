import json
import requests
import json
import asyncio
import aiohttp
import aiofiles
import time
from tqdm import tqdm
from types import SimpleNamespace as Namespace
import os
import requests
from pathlib import Path

from cdislogging import get_logger

from gen3.index import Gen3Index
from gen3.utils import DEFAULT_BACKOFF_SETTINGS, raise_for_status_and_print_error
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

logging = get_logger("__name__")


MAX_RETRIES = 3


class Gen3File:
    """For interacting with Gen3 file management features.

    A class for interacting with the Gen3 file download services.
    Supports getting presigned urls right now.

    Args:
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3File class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(refresh_file="credentials.json")
        ... file = Gen3File(auth)

    """

    def __init__(self, endpoint=None, auth_provider=None):
        # auth_provider legacy interface required endpoint as 1st arg
        self._auth_provider = auth_provider or endpoint
        self._endpoint = self._auth_provider.endpoint
        self.unsuccessful_downloads = []

    def get_presigned_url(self, guid, protocol=None):
        """Generates a presigned URL for a file.

        Retrieves a presigned url for a file giving access to a file for a limited time.

        Args:
            guid (str): The GUID for the object to retrieve.
            protocol (:obj:`str`, optional): The protocol to use for picking the available URL for generating the presigned URL.

        Examples:

            >>> Gen3File.get_presigned_url(query)

        """
        api_url = "{}/user/data/download/{}".format(self._endpoint, guid)
        if protocol:
            api_url += "?protocol={}".format(protocol)
        resp = requests.get(api_url, auth=self._auth_provider)
        raise_for_status_and_print_error(resp)

        try:
            return resp.json()
        except:
            return resp.text

    def delete_file(self, guid):
        """
        This method is DEPRECATED. Use delete_file_locations() instead.
        Delete all locations of a stored data file and remove its record from indexd

        Args:
            guid (str): provide a UUID for file id to delete
        Returns:
            text: requests.delete text result
        """
        print("This method is DEPRECATED. Use delete_file_locations() instead.")
        api_url = "{}/user/data/{}".format(self._endpoint, guid)
        output = requests.delete(api_url, auth=self._auth_provider).text

        return output

    def delete_file_locations(self, guid):
        """
        Delete all locations of a stored data file and remove its record from indexd

        Args:
            guid (str): provide a UUID for file id to delete
        Returns:
            requests.Response : requests.delete result
        """
        api_url = "{}/user/data/{}".format(self._endpoint, guid)
        output = requests.delete(api_url, auth=self._auth_provider)

        return output

    def upload_file(
        self, file_name, authz=None, protocol=None, expires_in=None, bucket=None
    ):
        """
        Get a presigned url for a file to upload

        Args:
            file_name (str): file_name to use for upload
            authz (list): authorization scope for the file as list of paths, optional.
            protocol (str): Storage protocol to use for upload: "s3", "az".
                If this isn't set, the default will be "s3"
            expires_in (int): Amount in seconds that the signed url will expire from datetime.utcnow().
                Be sure to use a positive integer.
                This value will also be treated as <= MAX_PRESIGNED_URL_TTL in the fence configuration.
            bucket (str): Bucket to upload to. The bucket must be configured in the Fence instance's
                `ALLOWED_DATA_UPLOAD_BUCKETS` setting. If not specified, Fence defaults to the
                `DATA_UPLOAD_BUCKET` setting.
        Returns:
            Document: json representation for the file upload
        """
        api_url = f"{self._endpoint}/user/data/upload"
        body = {}
        if protocol:
            body["protocol"] = protocol
        if authz:
            body["authz"] = authz
        if expires_in:
            body["expires_in"] = expires_in
        if file_name:
            body["file_name"] = file_name
        if bucket:
            body["bucket"] = bucket

        headers = {"Content-Type": "application/json"}
        resp = requests.post(
            api_url, auth=self._auth_provider, json=body, headers=headers
        )
        raise_for_status_and_print_error(resp)
        try:
            data = json.loads(resp.text)
        except:
            return resp.text

        return data

    def _ensure_dirpath_exists(path: Path) -> Path:
        """Utility to create a directory if missing.
        Returns the path so that the call can be inlined in another call
        Args:
            path (Path): path to create
        Returns
            path of created directory
        """
        assert path
        out_path: Path = path

        if not out_path.exists():
            out_path.mkdir(parents=True, exist_ok=True)

        return out_path

    def download_single(self, object_id, path):
        """
        Download a single file using its GUID.

        Args:
            object_id (str): The file's unique ID
            path (str): Path to store the downloaded file at
        """
        try:
            url = self.get_presigned_url(object_id)
        except Exception as e:
            logging.critical(f"Unable to get a presigned URL for download: {e}")
            return False

        response = requests.get(url["url"], stream=True)
        if response.status_code != 200:
            logging.error(f"Response code: {response.status_code}")
            if response.status_code >= 500:
                for _ in range(MAX_RETRIES):
                    logging.info("Retrying now...")
                    # NOTE could be updated with exponential backoff
                    time.sleep(1)
                    response = requests.get(url["url"], stream=True)
                    if response.status == 200:
                        break
                if response.status != 200:
                    logging.critical("Response status not 200, try again later")
                    return False
            else:
                return False

        response.raise_for_status()

        total_size_in_bytes = int(response.headers.get("content-length"))
        total_downloaded = 0

        index = Gen3Index(self._auth_provider)
        record = index.get_record(object_id)

        filename = record["file_name"]

        out_path = Gen3File._ensure_dirpath_exists(Path(path))

        with open(os.path.join(out_path, filename), "wb") as f:
            for data in response.iter_content(4096):
                total_downloaded += len(data)
                f.write(data)

        if total_size_in_bytes == total_downloaded:
            logging.info(f"File {filename} downloaded successfully")

        else:
            logging.error(f"File {filename} not downloaded successfully")
            return False

        return True

    def upload_file_to_guid(
        self, guid, file_name, protocol=None, expires_in=None, bucket=None
    ):
        """
        Get a presigned url for a file to upload to the specified existing GUID

        Args:
            file_name (str): file_name to use for upload
            protocol (str): Storage protocol to use for upload: "s3", "az".
                If this isn't set, the default will be "s3"
            expires_in (int): Amount in seconds that the signed url will expire from datetime.utcnow().
                Be sure to use a positive integer.
                This value will also be treated as <= MAX_PRESIGNED_URL_TTL in the fence configuration.
            bucket (str): Bucket to upload to. The bucket must be configured in the Fence instance's
                `ALLOWED_DATA_UPLOAD_BUCKETS` setting. If not specified, Fence defaults to the
                `DATA_UPLOAD_BUCKET` setting.
        Returns:
            Document: json representation for the file upload
        """
        url = f"{self._endpoint}/user/data/upload/{guid}"
        params = {}
        if protocol:
            params["protocol"] = protocol
        if expires_in:
            params["expires_in"] = expires_in
        if file_name:
            params["file_name"] = file_name
        if bucket:
            params["bucket"] = bucket

        url_parts = list(urlparse(url))
        query = dict(parse_qsl(url_parts[4]))
        query.update(params)
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)

        resp = requests.get(url, auth=self._auth_provider)
        raise_for_status_and_print_error(resp)
        return resp.json()
