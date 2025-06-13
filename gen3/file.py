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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
import click

from cdislogging import get_logger

from gen3.index import Gen3Index
from gen3.utils import DEFAULT_BACKOFF_SETTINGS, raise_for_status_and_print_error
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

logging = get_logger("__name__")


MAX_RETRIES = 3
DEFAULT_NUM_PARALLEL = 3


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

    def _download_file_worker(self, guid, protocol, output_dir, filename_format, rename, skip_completed):
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
                return {"guid": guid, "status": "failed", "error": "Failed to get presigned URL"}
                
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

            # Ensure parent directories exist
            filepath.parent.mkdir(parents=True, exist_ok=True)
            
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0
            
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(4096):
                    downloaded += len(chunk)
                    f.write(chunk)
                    
            if total_size and total_size != downloaded:
                return {"guid": guid, "status": "failed", "error": "Size mismatch"}
                
            return {"guid": guid, "status": "downloaded", "filepath": str(filepath), "size": downloaded}
            
        except Exception as e:
            return {"guid": guid, "status": "failed", "error": str(e)}

    def download_single(self, guid, download_path=".", filename_format="original", protocol=None, skip_completed=False, rename=False):
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
            result = self._download_file_worker(guid, protocol, output_dir, filename_format, rename, skip_completed)
            return result
        except Exception as e:
            return {"status": "failed", "error": f"Download failed: {e}"}

    def download_multiple(self, manifest_data, download_path=".", filename_format="original", protocol=None, 
                         num_parallel=DEFAULT_NUM_PARALLEL, skip_completed=False, rename=False, 
                         no_prompt=False, no_progress=False):
        """Download multiple files from manifest data.
        
        Args:
            manifest_data (List[Dict]): List of file objects with 'guid' key
            download_path (str): Directory to save files
            filename_format (str): Format for filenames - 'original', 'guid', or 'combined'
            protocol (str, optional): Protocol preference for downloads
            num_parallel (int): Number of parallel download threads
            skip_completed (bool): Skip files that already exist
            rename (bool): Rename files on conflict
            no_prompt (bool): Skip confirmation prompts
            no_progress (bool): Disable progress display
            
        Returns:
            Dict: Results summary with succeeded/failed lists
        """
        if not manifest_data:
            return {"succeeded": [], "failed": [], "skipped": []}
            
        guids = [item.get("guid") or item.get("object_id") for item in manifest_data if item.get("guid") or item.get("object_id")]
        if not guids:
            logging.error("No valid GUIDs found in manifest data")
            return {"succeeded": [], "failed": [], "skipped": []}
            
        if not no_prompt:
            click.confirm(f"Download {len(guids)} files?", abort=True)
            
        output_dir = Gen3File._ensure_dirpath_exists(Path(download_path))
        
        results = {"succeeded": [], "failed": [], "skipped": []}
        
        with ThreadPoolExecutor(max_workers=num_parallel) as executor:
            futures = {
                executor.submit(
                    self._download_file_worker, 
                    guid, 
                    protocol, 
                    output_dir, 
                    filename_format, 
                    rename, 
                    skip_completed
                ): guid for guid in guids
            }
            
            if not no_progress:
                futures_iter = tqdm(as_completed(futures), total=len(futures), desc="Downloading")
            else:
                futures_iter = as_completed(futures)
                
            for future in futures_iter:
                result = future.result()
                if result["status"] == "downloaded":
                    results["succeeded"].append(result["guid"])
                elif result["status"] == "skipped":
                    results["skipped"].append(result["guid"])
                else:
                    results["failed"].append(result["guid"])
                    logging.error(f"Download failed for {result['guid']}: {result.get('error', 'Unknown error')}")
                    
        logging.info(f"Download complete: {len(results['succeeded'])} succeeded, {len(results['failed'])} failed, {len(results['skipped'])} skipped")
        return results

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
