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
from gen3.utils import DEFAULT_BACKOFF_SETTINGS


logging = get_logger("__name__")


MAX_RETRIES = 3


def _load_manifest(manifest_file_path):

    """
    Function to convert manifest to python objects, stored in a list.

    Args:
        manifest_file_path (str): path to the manifest file. The manifest should be a JSON file
            in the following format:
            [
                { "object_id": "", "file_name"(optional): "" },
                ...
            ]

    Returns:
        List of objects
    """
    try:
        with open(manifest_file_path, "rt") as f:
            data = json.load(f, object_hook=lambda d: Namespace(**d))
            return data

    except Exception as e:
        logging.critical(f"Error in load manifest: {e}")
        return None


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
        output = requests.get(api_url, auth=self._auth_provider).text

        try:
            data = json.loads(output)
        except:
            return output
        return data

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

    def upload_file(self, file_name, authz=None, protocol=None, expires_in=None):
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

        headers = {"Content-Type": "application/json"}
        output = requests.post(
            api_url, auth=self._auth_provider, json=body, headers=headers
        ).text
        try:
            data = json.loads(output)
        except:
            return output

        return data

    async def _download_using_url(self, sem, entry, client, path, pbar):

        """
        Function to use object id of file to obtain its download url and use that download url
        to download required file asynchronously
        """

        successful = False
        try:
            await sem.acquire()
            if not entry.object_id:
                logging.critical("Wrong manifest entry, no object_id provided")

            url = self.get_presigned_url(entry.object_id)
            if not url:
                logging.critical("No url on retrial, try again later")

            async with client.get(url["url"], read_bufsize=4096) as response:
                if response.status != 200:
                    logging.error(f"Response code: {response.status_code}")
                    if response.status >= 500:
                        for _ in range(MAX_RETRIES):
                            logging.info("Retrying now...")
                            # NOTE could be updated with exponential backoff
                            await asyncio.sleep(1)
                            response = await client.get(url["url"], read_bufsize=4096)
                            if response.status == 200:
                                break
                        if response.status != 200:
                            logging.critical("Response status not 200, try again later")

                response.raise_for_status()

                if entry.file_name is None:
                    index = Gen3Index(self._auth_provider)
                    record = index.get_record(entry.object_id)
                    filename = record["file_name"]
                else:
                    filename = entry.file_name

                out_path = Gen3File._ensure_dirpath_exists(Path(path))

                total_size_in_bytes = int(response.headers.get("content-length"))
                total_downloaded = 0
                async with aiofiles.open(os.path.join(out_path, filename), "wb") as f:
                    with tqdm(
                        desc=f"File {entry.file_name}",
                        total=total_size_in_bytes,
                        position=1,
                        unit_scale=True,
                        unit_divisor=1024,
                        unit="B",
                        ncols=90,
                    ) as progress:
                        total_downloaded = 0
                        async with aiofiles.open(
                            os.path.join(path, filename), "wb"
                        ) as f:
                            with tqdm(
                                desc=f"File {entry.file_name}",
                                total=total_size_in_bytes,
                                position=1,
                                unit_scale=True,
                                unit_divisor=1024,
                                unit="B",
                                ncols=90,
                            ) as progress:
                                async for data in response.content.iter_chunked(4096):
                                    progress.update(len(data))
                                    total_downloaded += len(data)
                                    await f.write(data)

                if total_size_in_bytes == total_downloaded:
                    pbar.update()
                    successful = True

                if not successful:
                    logging.error(f"File {entry.file_name} not downloaded successfully")
                    self.unsuccessful_downloads.append(entry)
                sem.release()

                return successful

        except Exception as e:
            logging.critical(
                f"\nError in {entry.file_name}: {e} Type: {e.__class__.__name__}\n"
            )
            self.unsuccessful_downloads.append(entry)
            sem.release()
            return successful

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

        successful = True
        try:
            url = self.get_presigned_url(object_id)
            if not url:
                logging.critical("No url on retrial, try again later")
                successful = False

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
                        successful = False
                else:
                    successful = False

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
                successful = False

            return successful

        except Exception as e:
            logging.critical(
                f"\nError in {object_id}: {e} Type: {e.__class__.__name__}\n"
            )
            return False

    async def download_manifest(self, manifest_file_path, download_path, total_sem=10):

        """
        Asynchronouslt download all entries in the provided manifest.

        Args:
            manifest_file_path (str): path to the manifest file. The manifest should be a JSON file
                in the following format:
                [
                    { "object_id": "", "file_name"(optional): "" },
                    ...
                ]
            download_path (str): Path to store downloaded files at
            total_sem (int): Number of semaphores (default = 10)
        """

        start_time = time.perf_counter()
        logging.info(f"Start time: {start_time}")

        manifest_list = _load_manifest(manifest_file_path)
        if not manifest_list:
            raise Exception("Nothing to download")
        logging.info("Done loading manifest")

        tasks = []
        sem = asyncio.Semaphore(
            value=total_sem
        )  # semaphores to control number of requests to server at a particular moment
        connector = aiohttp.TCPConnector(force_close=True)

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(600), connector=connector, trust_env=True
        ) as client:
            with tqdm(
                desc="Manifest progress",
                total=len(manifest_list),
                unit_scale=True,
                position=0,
                unit_divisor=1024,
                unit="B",
                ncols=90,
            ) as pbar:
                # progress bar to show how many files in the manifest have been downloaded
                for entry in manifest_list:
                    # creating a task for each entry
                    tasks.append(
                        asyncio.create_task(
                            self._download_using_url(
                                sem, entry, client, download_path, pbar
                            )
                        )
                    )
                await asyncio.gather(*tasks)

        duration = time.perf_counter() - start_time
        logging.info(f"\nDuration = {duration}\n")
        if self.unsuccessful_downloads:
            logging.info(f"Unsuccessful downloads - {self.unsuccessful_downloads}\n")
