import base64
from dataclasses import dataclass, field

import json
from typing import List, Optional
import numpy
import requests
import json
import asyncio
import aiohttp
import aiofiles
import time
from tqdm.auto import tqdm
from types import SimpleNamespace as Namespace
import os
import requests
from pathlib import Path

from cdislogging import get_logger

from gen3.index import Gen3Index
from gen3.utils import DEFAULT_BACKOFF_SETTINGS, raise_for_status_and_print_error
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

logging = get_logger("__name__")


@dataclass
class EmbeddingContent:
    guid: str
    embedding_id: str
    embedding: numpy.ndarray
    authz: list[str]
    collection_id: int | None
    self: str
    metadata: dict[str, str] = field(default_factory=dict)


MAX_RETRIES = 3
DEFAULT_BATCH_SIZE = 500
SUPPORTED_CONTENT_TYPES = ["gen3_embeddings"]


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

    async def get_bulk_content(
        self,
        input_file=None,
        guids=None,
        batch_size=DEFAULT_BATCH_SIZE,
        content_type=SUPPORTED_CONTENT_TYPES[0],
        exclude_info=False,
        concurrency: int = 10,
    ) -> dict[str, EmbeddingContent]:
        """
        Retrieve bulk content for a set of GUIDs

        Args:
            input_file (str | None): Path to a file that contains one GUID per line.
            guids (tuple[str, ...]): One or more GUIDs supplied
            batch_size (int): How many GUIDs to send in each request to `/data/content`.
            content_type (str): type of content of GUIDs, this determines how to parse.
            exclude_info (bool): whether or not to exclude additional info in response
            concurrency (int): Maximum number of concurrent requests
        """
        if content_type not in SUPPORTED_CONTENT_TYPES:
            raise ValueError(
                f"Error: unsupported `content_type={content_type}`, not in supported: {SUPPORTED_CONTENT_TYPES}"
            )

        final_batch_size = min(batch_size, DEFAULT_BATCH_SIZE)
        logging.debug(
            f"Using batch_size={final_batch_size} for requests to /data/content"
        )
        if final_batch_size != batch_size:
            logging.warning(
                f"Requested batch_size={batch_size} too large, using default: {DEFAULT_BATCH_SIZE}"
            )

        if input_file and guids:
            raise ValueError("Error: provide either input_file or guids, not both.")

        all_guids: List[str] = []

        if input_file:
            with open(input_file) as f:
                for line in f:
                    guid = line.strip()
                    if guid:
                        all_guids.append(guid)
        elif guids:
            all_guids.extend(guids)
        else:
            raise ValueError("Error: provide either input_file or guids")

        if not all_guids:
            logging.error("No valid GUIDs found in the supplied input.")
            return {}

        embeddings: dict[str, EmbeddingContent] = {}
        batches = [
            all_guids[i : i + batch_size] for i in range(0, len(all_guids), batch_size)
        ]
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_one(batch: list[str]):
            async with semaphore:
                logging.debug(f"fetching batch of size {len(batch)}...")
                try:
                    batch_response = await asyncio.to_thread(
                        self.get_content, batch, exclude_info=exclude_info
                    )
                    return batch, batch_response
                except Exception as exc:
                    logging.error(f"API error on batch starting at {batch[0]}: {exc}")
                    return batch, None

        results = await asyncio.gather(*(fetch_one(batch) for batch in batches))

        for batch, batch_response in results:
            if batch_response is None:
                continue

            if isinstance(batch_response, str):
                logging.warning(
                    f"Warning: received raw text response for batch starting with {batch[0]}. Skipping."
                )
                continue

            if content_type == "gen3_embeddings":
                embeddings_from_batch = self.get_embeddings_from_bulk_content(
                    batch_response
                )
                embeddings.update(embeddings_from_batch)

        logging.debug(f"Successfully retrieved {len(embeddings)} records!")
        return embeddings

    def get_content(self, guids: list, exclude_info: bool = False) -> dict:
        """
        Bulk retrieve content for a list of GUIDs.

        The Gen3 API provides the `/data/content` endpoint which accepts a JSON body with
        an array of GUID strings.  This helper wraps that call and returns the parsed
        response.

        Args:
            guids (list): A list or tuple of GUIDs for which to fetch content.
            exclude_info (bool): whether or not to exclude additional info in response.

        Returns:
            dict | str: If the request succeeds and the body can be decoded as JSON, a mapping
                from each provided GUID to its associated content is returned

        Raises:
            requests.HTTPError: If the HTTP status code indicates an error
        """
        api_url = f"{self._endpoint}/user/data/content"
        if exclude_info:
            api_url += "?exclude_info=true"

        body = {"guids": guids}
        headers = {"Content-Type": "application/json"}

        resp = requests.post(
            api_url, auth=self._auth_provider, json=body, headers=headers
        )
        raise_for_status_and_print_error(resp)

        return resp.json()

    def get_embeddings_from_bulk_content(
        self, batch_response: dict
    ) -> dict[str, EmbeddingContent]:
        """
        Get a dict of parsed embeddings from a batch_resonse of GUIDs
        which are all embeddings.
        """
        batch_response = batch_response or {}

        if not batch_response:
            logging.warning("Empty batch_response. Continuing anyway...")

        embeddings = {}
        for guid, data in batch_response.get("guids", {}).items():
            embeddings[guid] = self.get_embeddings_from_bulk_content_guid(guid, data)
        return embeddings

    def get_embeddings_from_bulk_content_guid(
        self, guid: str, bulk_content_guid_data: dict
    ) -> EmbeddingContent:
        """
        Return an EmbeddingContent by parsing the response data for a particular GUID in a Bulk Content
        response which corresponds to a Gen3 EmbeddingContent.

        Note: this handles base64 decoding if the API call used that. and note that the resulting
            vector is a numpy array

        Args:
            guid (str): globally unique identifier for the blob of data provided
            bulk_content_guid_data (dict): data from Bulk Content response.get("guids", {}).get(guid)
                e.g. the data for the guid specified

        Returns:
            EmbeddingContent - a dataclass representation of the embedding
        """
        if not isinstance(bulk_content_guid_data, dict) or not bulk_content_guid_data:
            logging.info(f"Warning: did not find {guid} in output, adding empty row...")
            return EmbeddingContent(
                guid=guid,
                embedding_id="",
                embedding=numpy.ndarray([]),
                authz=[],
                collection_id=None,
                self="",
                metadata={},
            )

        embedding_id = bulk_content_guid_data.get(
            "embedding_id", ""
        ) or bulk_content_guid_data.get("id", "")
        raw_vector = (
            bulk_content_guid_data.get("vector")
            or bulk_content_guid_data.get("embedding")
            or []
        )

        embedding_vector = None

        # handle vector if base64 version of API used
        if not raw_vector and "vector_base64" in bulk_content_guid_data:
            if "precision" not in bulk_content_guid_data:
                raise Exception(
                    f"`vector_base64` found but no `precision` specified. Unable to parse."
                )

            vector_data_type = (
                numpy.float16
                if bulk_content_guid_data["precision"] == "float16"
                else numpy.float32
            )

            # endpoint may be using binary representation
            vector_base64_str = bulk_content_guid_data["vector_base64"]

            # re-pad the string to a multiple of 4 (handles any missing '=' signs)
            padding_needed = -len(vector_base64_str) % 4
            padded_b64 = vector_base64_str + ("=" * padding_needed)
            decoded_bytes = base64.urlsafe_b64decode(padded_b64)

            embedding_vector = numpy.frombuffer(decoded_bytes, dtype=vector_data_type)

        if embedding_vector is None:
            embedding_vector = numpy.array(raw_vector)

        bulk_content_guid_data_info = bulk_content_guid_data.get("info", {}) or {}

        authz_val = bulk_content_guid_data_info.get("authz", [])
        collection_id_val = bulk_content_guid_data_info.get("collection_id", "")

        url_or_self = bulk_content_guid_data_info.get("self", "")

        metadata = bulk_content_guid_data_info.get("metadata", {})

        return EmbeddingContent(
            guid=guid,
            embedding_id=embedding_id,
            embedding=embedding_vector,
            authz=authz_val,
            collection_id=collection_id_val,
            self=url_or_self,
            metadata=metadata,
        )

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
