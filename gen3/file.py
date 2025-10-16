import json
import requests
import asyncio
import aiohttp
import aiofiles
import time
import multiprocessing as mp
import threading
from tqdm import tqdm
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, quote
from queue import Empty

from cdislogging import get_logger

from gen3.index import Gen3Index
from gen3.utils import raise_for_status_and_print_error

logging = get_logger("__name__")


MAX_RETRIES = 3
DEFAULT_NUM_PARALLEL = 3
DEFAULT_MAX_CONCURRENT_REQUESTS = 300
DEFAULT_QUEUE_SIZE = 1000


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

        return resp.json()

    def get_presigned_urls_batch(self, guids, protocol=None):
        """Get presigned URLs for multiple files efficiently.

        Args:
            guids (List[str]): List of GUIDs to get presigned URLs for
            protocol (str, optional): Protocol preference for URLs

        Returns:
            Dict[str, Dict]: Mapping of GUID to presigned URL response
        """
        results = {}
        for guid in guids:
            try:
                results[guid] = self.get_presigned_url(guid, protocol)
            except Exception as e:
                logging.error(f"Failed to get presigned URL for {guid}: {e}")
                results[guid] = None
        return results

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
        return resp.json()

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

    def download_single(
        self,
        object_id=None,
        path=None,
        guid=None,
        download_path=None,
        filename_format="original",
        protocol=None,
        skip_completed=True,
        rename=False,
    ):
        """
        Download a single file using its GUID.

        Args:
            object_id (str): The file's unique ID (legacy parameter)
            path (str): Path to store the downloaded file at (legacy parameter)
            guid (str): The file's unique ID (new parameter name)
            download_path (str): Path to store the downloaded file at (new parameter name)
            filename_format (str): Format for filename (original, guid, combined)
            protocol (str): Protocol for presigned URL
            skip_completed (bool): Skip files that already exist
            rename (bool): Rename file if it already exists

        Returns:
            dict: Status information about the download
        """
        # Handle both old and new parameter names
        if object_id is not None:
            file_guid = object_id
        elif guid is not None:
            file_guid = guid
        else:
            raise ValueError("Either object_id or guid must be provided")

        if path is not None:
            download_dir = path
        elif download_path is not None:
            download_dir = download_path
        else:
            download_dir = "."
        try:
            url = self.get_presigned_url(file_guid)
        except Exception as e:
            logging.critical(f"Unable to get a presigned URL for download: {e}")
            return {"status": "failed", "error": str(e)}

        # Get file metadata
        index = Gen3Index(self._auth_provider)
        record = index.get_record(file_guid)

        # Determine filename based on format
        if filename_format == "guid":
            filename = file_guid
        elif filename_format == "combined":
            original_filename = record.get("file_name", file_guid)
            filename = f"{original_filename}_{file_guid}"
        else:  # original
            filename = record.get("file_name", file_guid)

        # Check if file already exists and handle accordingly
        out_path = Gen3File._ensure_dirpath_exists(Path(download_dir))
        filepath = os.path.join(out_path, filename)

        if os.path.exists(filepath) and skip_completed:
            return {
                "status": "skipped",
                "reason": "File already exists",
                "filepath": filepath,
            }
        elif os.path.exists(filepath) and rename:
            counter = 1
            base_name, ext = os.path.splitext(filename)
            while os.path.exists(filepath):
                filename = f"{base_name}_{counter}{ext}"
                filepath = os.path.join(out_path, filename)
                counter += 1

        response = requests.get(url["url"], stream=True)
        if response.status_code != 200:
            logging.error(f"Response code: {response.status_code}")
            if response.status_code >= 500:
                for _ in range(MAX_RETRIES):
                    logging.info("Retrying now...")
                    # NOTE could be updated with exponential backoff
                    time.sleep(1)
                    response = requests.get(url["url"], stream=True)
                    if response.status_code == 200:
                        break
                if response.status_code != 200:
                    logging.critical("Response status not 200, try again later")
                    return {
                        "status": "failed",
                        "error": "Server error, try again later",
                    }
            else:
                return {"status": "failed", "error": f"HTTP {response.status_code}"}

        response.raise_for_status()

        total_size_in_bytes = int(response.headers.get("content-length", 0))
        total_downloaded = 0

        with open(filepath, "wb") as f:
            for data in response.iter_content(4096):
                total_downloaded += len(data)
                f.write(data)

        if total_size_in_bytes > 0 and total_size_in_bytes == total_downloaded:
            logging.info(f"File {filename} downloaded successfully")
            return {"status": "downloaded", "filepath": filepath}
        elif total_size_in_bytes == 0:
            logging.info(f"File {filename} downloaded successfully (unknown size)")
            return {"status": "downloaded", "filepath": filepath}
        else:
            logging.error(f"File {filename} not downloaded successfully")
            return {"status": "failed", "error": "Download incomplete"}

        return {"status": "downloaded", "filepath": filepath}

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

    async def async_download_multiple(
        self,
        manifest_data,
        download_path=".",
        filename_format="original",
        protocol=None,
        max_concurrent_requests=DEFAULT_MAX_CONCURRENT_REQUESTS,
        num_processes=DEFAULT_NUM_PARALLEL,
        queue_size=DEFAULT_QUEUE_SIZE,
        skip_completed=False,
        rename=False,
        no_progress=False,
    ):
        """Asynchronously download multiple files using multiprocessing and queues."""
        if not manifest_data:
            return {"succeeded": [], "failed": [], "skipped": []}

        guids = []
        for item in manifest_data:
            guid = item.get("guid") or item.get("object_id")
            if guid:
                if "/" in guid:
                    guid = guid.split("/")[-1]
                guids.append(guid)

        if not guids:
            logging.error("No valid GUIDs found in manifest data")
            return {"succeeded": [], "failed": [], "skipped": []}

        output_dir = Gen3File._ensure_dirpath_exists(Path(download_path))

        input_queue = mp.Queue(maxsize=queue_size)
        output_queue = mp.Queue()

        worker_config = {
            "endpoint": self._endpoint,
            "auth_provider": self._auth_provider,
            "download_path": str(output_dir),
            "filename_format": filename_format,
            "protocol": protocol,
            "max_concurrent": max_concurrent_requests,
            "skip_completed": skip_completed,
            "rename": rename,
        }

        processes = []
        producer_thread = None

        try:
            for i in range(num_processes):
                p = mp.Process(
                    target=self._async_worker_process,
                    args=(input_queue, output_queue, worker_config, i),
                )
                p.start()
                processes.append(p)

            producer_thread = threading.Thread(
                target=self._guid_producer,
                args=(guids, input_queue, num_processes),
            )
            producer_thread.start()

            results = {"succeeded": [], "failed": [], "skipped": []}
            completed_count = 0

            if not no_progress:
                pbar = tqdm(total=len(guids), desc="Downloading")

            while completed_count < len(guids):
                try:
                    batch_results = output_queue.get()

                    if not batch_results:
                        continue

                    for result in batch_results:
                        if result["status"] == "downloaded":
                            results["succeeded"].append(result["guid"])
                        elif result["status"] == "skipped":
                            results["skipped"].append(result["guid"])
                        else:
                            results["failed"].append(result["guid"])

                        completed_count += 1
                        if not no_progress:
                            pbar.update(1)

                except Empty:
                    logging.warning(
                        f"No more results available ({completed_count}/{len(guids)}): Queue is empty"
                    )
                    break
                except Exception as e:
                    logging.warning(
                        f"Error waiting for results ({completed_count}/{len(guids)}): {e}"
                    )

                    alive_processes = [p for p in processes if p.is_alive()]
                    if not alive_processes:
                        logging.error("All worker processes have died")
                        break

            if not no_progress:
                pbar.close()

            if producer_thread:
                producer_thread.join()

        except Exception as e:
            logging.error(f"Error in download: {e}")
            results = {"succeeded": [], "failed": [], "skipped": [], "error": str(e)}

        finally:
            for p in processes:
                if p.is_alive():
                    p.terminate()

                    p.join()
                    if p.is_alive():
                        p.kill()

        logging.info(
            f"Download complete: {len(results['succeeded'])} succeeded, "
            f"{len(results['failed'])} failed, {len(results['skipped'])} skipped"
        )
        return results

    def _guid_producer(self, guids, input_queue, num_processes):
        try:
            for guid in guids:
                input_queue.put(guid)

        except Exception as e:
            logging.error(f"Error in producer: {e}")

    @staticmethod
    def _async_worker_process(input_queue, output_queue, config, process_id):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(
                Gen3File._worker_main(input_queue, output_queue, config, process_id)
            )
        except Exception as e:
            logging.error(f"Error in worker process {process_id}: {e}")
        finally:
            try:
                loop.close()
            except Exception as e:
                logging.warning(f"Error closing event loop in worker {process_id}: {e}")

    @staticmethod
    async def _worker_main(input_queue, output_queue, config, process_id):
        endpoint = config["endpoint"]
        auth_provider = config["auth_provider"]
        download_path = Path(config["download_path"])
        filename_format = config["filename_format"]
        protocol = config["protocol"]
        max_concurrent = config["max_concurrent"]
        skip_completed = config["skip_completed"]
        rename = config["rename"]

        # Configure connector with optimized settings for large files
        timeout = aiohttp.ClientTimeout(total=None, connect=3600, sock_read=3600)
        connector = aiohttp.TCPConnector(
            limit=max_concurrent * 2,
            limit_per_host=max_concurrent,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=3600,
            enable_cleanup_closed=True,
        )
        semaphore = asyncio.Semaphore(max_concurrent)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            while True:
                try:
                    # Check if queue is empty with timeout
                    guid = input_queue.get()
                except Empty:
                    # If queue is empty (timeout), break the loop
                    break

                # Process single GUID as a batch of one
                try:
                    batch_results = await Gen3File._process_batch(
                        session,
                        [guid],
                        endpoint,
                        auth_provider,
                        download_path,
                        filename_format,
                        protocol,
                        semaphore,
                        skip_completed,
                        rename,
                    )
                    output_queue.put(batch_results)
                except Exception as e:
                    logging.error(
                        f"Worker {process_id}: Failed to process {guid} - {type(e).__name__}: {e}"
                    )
                    error_result = [{"guid": guid, "status": "failed", "error": str(e)}]
                    try:
                        output_queue.put(error_result)
                    except Exception as queue_error:
                        logging.error(
                            f"Worker {process_id}: Failed to send error result for {guid} - {type(queue_error).__name__}: {queue_error}"
                        )

    @staticmethod
    async def _process_batch(
        session,
        guids,
        endpoint,
        auth_provider,
        download_path,
        filename_format,
        protocol,
        semaphore,
        skip_completed,
        rename,
    ):
        """Process a batch of GUIDs for downloading."""
        batch_results = []
        for guid in guids:
            async with semaphore:
                result = await Gen3File._download_single_async(
                    session,
                    guid,
                    endpoint,
                    auth_provider,
                    download_path,
                    filename_format,
                    protocol,
                    semaphore,
                    skip_completed,
                    rename,
                )
                batch_results.append(result)
        return batch_results

    @staticmethod
    async def _download_single_async(
        session,
        guid,
        endpoint,
        auth_provider,
        download_path,
        filename_format,
        protocol,
        semaphore,
        skip_completed,
        rename,
    ):
        async with semaphore:
            try:
                metadata = await Gen3File._get_metadata(
                    session, guid, endpoint, auth_provider.get_access_token()
                )

                original_filename = metadata.get("file_name")
                filename = Gen3File._format_filename_static(
                    guid, original_filename, filename_format
                )
                filepath = download_path / filename

                if skip_completed and filepath.exists():
                    return {
                        "guid": guid,
                        "status": "skipped",
                        "filepath": str(filepath),
                        "reason": "File already exists",
                    }

                filepath = Gen3File._handle_conflict_static(filepath, rename)

                presigned_data = await Gen3File._get_presigned_url_async(
                    session, guid, endpoint, auth_provider.get_access_token(), protocol
                )

                url = presigned_data.get("url")
                if not url:
                    return {
                        "guid": guid,
                        "status": "failed",
                        "error": "No URL in presigned data",
                    }

                filepath.parent.mkdir(parents=True, exist_ok=True)

                success = await Gen3File._download_content(session, url, guid, filepath)
                if success:
                    return {
                        "guid": guid,
                        "status": "downloaded",
                        "filepath": str(filepath),
                        "size": filepath.stat().st_size if filepath.exists() else 0,
                    }
                else:
                    return {
                        "guid": guid,
                        "status": "failed",
                        "error": "Download failed",
                    }

            except Exception as e:
                logging.error(f"Error downloading {guid}: {e}")
                return {
                    "guid": guid,
                    "status": "failed",
                    "error": str(e),
                }

    @staticmethod
    async def _get_metadata(session, guid, endpoint, auth_token):
        encoded_guid = quote(guid, safe="")
        api_url = f"{endpoint}/index/{encoded_guid}"
        headers = {"Authorization": f"Bearer {auth_token}"}

        try:
            async with session.get(
                api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=3600)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                raise Exception(
                    f"Failed to get metadata for {guid}: HTTP {resp.status}"
                )
        except aiohttp.ClientError as e:
            raise Exception(f"Network error getting metadata for {guid}: {e}")
        except asyncio.TimeoutError:
            raise Exception(f"Timeout getting metadata for {guid}")
        except Exception as e:
            if "Failed to get metadata" not in str(e):
                raise Exception(f"Unexpected error getting metadata for {guid}: {e}")
            raise

    @staticmethod
    async def _get_presigned_url_async(
        session, guid, endpoint, auth_token, protocol=None
    ):
        encoded_guid = quote(guid, safe="")
        api_url = f"{endpoint}/user/data/download/{encoded_guid}"
        headers = {"Authorization": f"Bearer {auth_token}"}

        if protocol:
            api_url += f"?protocol={protocol}"

        try:
            async with session.get(
                api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=3600)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                raise Exception(
                    f"Failed to get presigned URL for {guid}: HTTP {resp.status}"
                )
        except aiohttp.ClientError as e:
            raise Exception(f"Network error getting presigned URL for {guid}: {e}")
        except asyncio.TimeoutError:
            raise Exception(f"Timeout getting presigned URL for {guid}")
        except Exception as e:
            if "Failed to get presigned URL" not in str(e):
                raise Exception(
                    f"Unexpected error getting presigned URL for {guid}: {e}"
                )
            raise

    @staticmethod
    async def _download_content(session, url, guid, filepath):
        """Download content directly to file with optimized streaming."""
        try:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=None)
            ) as resp:
                if resp.status == 200:
                    async with aiofiles.open(filepath, "wb") as f:
                        chunk_size = 1024 * 1024
                        async for chunk in resp.content.iter_chunked(chunk_size):
                            await f.write(chunk)
                    return True
                logging.error(f"Download failed for {guid}: HTTP {resp.status}")
                return False
        except aiohttp.ClientError as e:
            logging.error(f"Network error downloading {guid}: {e}")
            return False
        except asyncio.TimeoutError:
            logging.error(f"Timeout downloading {guid}")
            return False
        except OSError as e:
            logging.error(f"File system error downloading {guid} to {filepath}: {e}")
            return False
        except Exception as e:
            logging.error(
                f"Unexpected error downloading {guid}: {type(e).__name__}: {e}"
            )
            return False

    @staticmethod
    def _format_filename_static(guid, original_filename, filename_format):
        if filename_format == "guid":
            return guid
        elif filename_format == "combined":
            if original_filename:
                name, ext = os.path.splitext(original_filename)
                return f"{name}_{guid}{ext}"
            return guid
        else:
            return original_filename or guid

    @staticmethod
    def _handle_conflict_static(filepath, rename):
        if not rename:
            if filepath.exists():
                logging.warning(f"File will be overwritten: {filepath}")
            return filepath

        if not filepath.exists():
            return filepath

        counter = 1
        name = filepath.stem
        ext = filepath.suffix
        parent = filepath.parent

        while True:
            new_path = parent / f"{name}_{counter}{ext}"
            if not new_path.exists():
                return new_path
            counter += 1
