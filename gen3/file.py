import json
import requests
import asyncio
import aiohttp
import aiofiles
import time
import multiprocessing as mp
import threading
from tqdm import tqdm
from tqdm.asyncio import tqdm as async_tqdm
from types import SimpleNamespace as Namespace
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, quote
from queue import Empty

from cdislogging import get_logger

from gen3.index import Gen3Index
from gen3.utils import DEFAULT_BACKOFF_SETTINGS, raise_for_status_and_print_error

logging = get_logger("__name__")


MAX_RETRIES = 3
DEFAULT_NUM_PARALLEL = 3
DEFAULT_MAX_CONCURRENT_REQUESTS = 50  # Increased for better performance


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

    def _format_filename(self, guid, original_filename, filename_format):
        """Format filename based on the specified format option.

        Args:
            guid (str): The GUID of the file
            original_filename (str): Original filename from metadata
            filename_format (str): Format option - 'original', 'guid', or 'combined'

        Returns:
            str: Formatted filename
        """
        if filename_format == "guid":
            return guid
        elif filename_format == "combined":
            if original_filename:
                name, ext = os.path.splitext(original_filename)
                return f"{name}_{guid}{ext}"
            return guid
        else:
            return original_filename or guid

    def _handle_file_conflict(self, filepath, rename):
        """Handle file name conflicts.

        Args:
            filepath (Path): Proposed file path
            rename (bool): Whether to rename if conflict exists

        Returns:
            Path: Final filepath to use
        """
        if not filepath.exists():
            return filepath

        if not rename:
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

    def _download_file_worker(
        self, guid, protocol, output_dir, filename_format, rename, skip_completed
    ):
        """Worker function for downloading a single file.

        Args:
            guid (str): File GUID
            protocol (str): Protocol preference for download
            output_dir (Path): Output directory
            filename_format (str): Filename format option
            rename (bool): Whether to rename on conflict
            skip_completed (bool): Whether to skip existing files

        Returns:
            Dict: Download result with status and details
        """
        try:
            presigned_url_data = self.get_presigned_url(guid, protocol)
            if not presigned_url_data:
                return {
                    "guid": guid,
                    "status": "failed",
                    "error": "Failed to get presigned URL",
                }

            index = Gen3Index(self._auth_provider)
            record = index.get_record(guid)
            original_filename = record.get("file_name")

            filename = self._format_filename(guid, original_filename, filename_format)
            filepath = self._handle_file_conflict(output_dir / filename, rename)

            if skip_completed and filepath.exists():
                return {"guid": guid, "status": "skipped", "filepath": str(filepath)}

            response = requests.get(presigned_url_data["url"], stream=True)
            if response.status_code >= 500:
                for _ in range(MAX_RETRIES):
                    time.sleep(1)
                    response = requests.get(presigned_url_data["url"], stream=True)
                    if response.status_code < 500:
                        break

            response.raise_for_status()

            filepath.parent.mkdir(parents=True, exist_ok=True)

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(4096):
                    downloaded += len(chunk)
                    f.write(chunk)

            if total_size and total_size != downloaded:
                return {"guid": guid, "status": "failed", "error": "Size mismatch"}

            return {
                "guid": guid,
                "status": "downloaded",
                "filepath": str(filepath),
                "size": downloaded,
            }

        except Exception as e:
            return {"guid": guid, "status": "failed", "error": str(e)}

    def download_single(
        self,
        guid,
        download_path=".",
        filename_format="original",
        protocol=None,
        skip_completed=False,
        rename=False,
    ):
        """Download a single file with enhanced options.

        Args:
            guid (str): File GUID to download
            download_path (str): Directory to save file
            filename_format (str): Format for filename - 'original', 'guid', or 'combined'
            protocol (str, optional): Protocol preference for download
            skip_completed (bool): Skip if file already exists
            rename (bool): Rename file if conflict exists

        Returns:
            Dict: Download result with status and details
        """
        output_dir = Gen3File._ensure_dirpath_exists(Path(download_path))

        try:
            result = self._download_file_worker(
                guid, protocol, output_dir, filename_format, rename, skip_completed
            )
            return result
        except Exception as e:
            return {"status": "failed", "error": f"Download failed: {e}"}

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
        num_processes=4,
        queue_size=1000,
        batch_size=100,  
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
            "auth_token": self._auth_provider.get_access_token(),
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
                    target=self._worker_process,
                    args=(input_queue, output_queue, worker_config, i),
                )
                p.start()
                processes.append(p)

            producer_thread = threading.Thread(
                target=self._guid_producer,
                args=(guids, input_queue, batch_size, num_processes),
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

                except Exception as e:
                    logging.warning(f"Error waiting for results: {e}")

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

    def _guid_producer(self, guids, input_queue, batch_size, num_processes):
        try:
            for i in range(0, len(guids), batch_size):
                batch = guids[i : i + batch_size]
                
                input_queue.put(batch)

            for _ in range(num_processes):
                
                input_queue.put(None)

        except Exception as e:
            logging.error(f"Error in producer: {e}")

    @staticmethod
    def _worker_process(input_queue, output_queue, config, process_id):
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
            except:
                pass

    @staticmethod
    async def _worker_main(input_queue, output_queue, config, process_id):
        endpoint = config["endpoint"]
        auth_token = config["auth_token"]
        download_path = Path(config["download_path"])
        filename_format = config["filename_format"]
        protocol = config["protocol"]
        max_concurrent = config["max_concurrent"]
        skip_completed = config["skip_completed"]
        rename = config["rename"]

        # Configure connector with optimized settings for large files
        timeout = aiohttp.ClientTimeout(total=None, connect=300, sock_read=300)
        connector = aiohttp.TCPConnector(
            limit=max_concurrent * 2,
            limit_per_host=max_concurrent,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=120,
            enable_cleanup_closed=True,
        )
        semaphore = asyncio.Semaphore(max_concurrent)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            while True:
                try:
                    try:
                        guid_batch = input_queue.get()
                    except Exception:
                        await asyncio.sleep(0.1)
                        continue

                    if guid_batch is None:
                        break

                    batch_results = await Gen3File._process_batch(
                        session,
                        guid_batch,
                        endpoint,
                        auth_token,
                        download_path,
                        filename_format,
                        protocol,
                        semaphore,
                        skip_completed,
                        rename,
                    )

                    try:
                        # Remove timeout limit for queue operations
                        output_queue.put(batch_results)
                    except Exception as e:
                        logging.error(
                            f"Worker {process_id}: Failed to send results: {e}"
                        )

                except Exception as e:
                    logging.error(f"Worker {process_id}: Error processing batch: {e}")
                    try:
                        error_result = [
                            {"guid": "unknown", "status": "failed", "error": str(e)}
                        ]
                        
                        output_queue.put(error_result)
                    except:
                        pass
                    continue

    @staticmethod
    async def _process_batch(
        session,
        guid_batch,
        endpoint,
        auth_token,
        download_path,
        filename_format,
        protocol,
        semaphore,
        skip_completed,
        rename,
    ):
        async def download_guid(guid):
            async with semaphore:
                try:
                    metadata = await Gen3File._get_metadata(
                        session, guid, endpoint, auth_token
                    )
                    if not metadata:
                        return {
                            "guid": guid,
                            "status": "failed",
                            "error": "No metadata",
                        }

                    original_filename = metadata.get("file_name")
                    filename = Gen3File._format_filename_static(
                        guid, original_filename, filename_format
                    )
                    filepath = Gen3File._handle_conflict_static(
                        download_path / filename, rename
                    )

                    if skip_completed and filepath.exists():
                        return {
                            "guid": guid,
                            "status": "skipped",
                            "filepath": str(filepath),
                        }

                    presigned_data = await Gen3File._get_presigned_url(
                        session, guid, endpoint, auth_token, protocol
                    )
                    if not presigned_data or not presigned_data.get("url"):
                        return {
                            "guid": guid,
                            "status": "failed",
                            "error": "No presigned URL",
                        }

                     
                    filepath.parent.mkdir(parents=True, exist_ok=True)

                    success = await Gen3File._download_content(
                        session, presigned_data["url"], guid, filepath
                    )
                    if not success:
                        return {
                            "guid": guid,
                            "status": "failed",
                            "error": "Download failed",
                        }

                    return {
                        "guid": guid,
                        "status": "downloaded",
                        "filepath": str(filepath),
                        "size": filepath.stat().st_size,
                    }

                except Exception as e:
                    return {"guid": guid, "status": "failed", "error": str(e)}

        tasks = [download_guid(guid) for guid in guid_batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                processed_results.append(
                    {"guid": "unknown", "status": "failed", "error": str(result)}
                )
            else:
                processed_results.append(result)

        return processed_results

    @staticmethod
    async def _get_metadata(session, guid, endpoint, auth_token):
        encoded_guid = quote(guid, safe="")
        api_url = f"{endpoint}/index/{encoded_guid}"
        headers = {"Authorization": f"Bearer {auth_token}"}

        try:
            
            async with session.get(
                api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except:
            return None

    @staticmethod
    async def _get_presigned_url(session, guid, endpoint, auth_token, protocol=None):
        encoded_guid = quote(guid, safe="")
        api_url = f"{endpoint}/user/data/download/{encoded_guid}"
        headers = {"Authorization": f"Bearer {auth_token}"}

        if protocol:
            api_url += f"?protocol={protocol}"

        try:
            
            async with session.get(
                api_url, headers=headers, timeout=aiohttp.ClientTimeout(total=60)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except:
            return None

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
                return False
        except Exception as e:
            logging.warning(f"Download failed for {guid}: {e}")
            return False

    @staticmethod
    def _format_filename_static(guid, original_filename, filename_format):
        if filename_format == "guid":
            return guid
        elif filename_format == "combined" and original_filename:
            return f"{guid}_{original_filename}"
        elif original_filename:
            return original_filename
        else:
            return guid

    @staticmethod
    def _handle_conflict_static(filepath, rename):
        if not filepath.exists():
            return filepath

        if not rename:
            return filepath

        base = filepath.stem
        suffix = filepath.suffix
        parent = filepath.parent
        counter = 1

        while True:
            new_name = f"{base}_{counter}{suffix}"
            new_path = parent / new_name
            if not new_path.exists():
                return new_path
            counter += 1
