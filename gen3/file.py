import json
import requests
import json
import asyncio
import aiohttp
import aiofiles
from cdislogging import get_logger
import time
from gen3.auth import Gen3Auth
from tqdm import tqdm
from types import SimpleNamespace as Namespace
import os
import requests
from gen3.index import Gen3Index

logging = get_logger("async")
no_retry = [400, 401, 402, 403, 405, 406, 407, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 421, 422, 423, 424, 426, 428, 429, 431, 451, 500, 501, 502, 505, 506, 507, 508, 510, 511]

class Gen3FileError(Exception):
    pass


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

    def __init__(self, endpoint=None, manifest_file_path = None, auth_provider=None):
        # auth_provider legacy interface required endpoint as 1st arg
        self._auth_provider = auth_provider or endpoint
        self._endpoint = self._auth_provider.endpoint
        self.manifest_file_path = manifest_file_path 
        self.unsuccessful = []
        
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

    def _load_manifest(self):

        """
        Function to convert manifest to python objects, stored in a list
        Manifest format - same as that accepted by cdis-data-client
        """
        try:
            with open(self.manifest_file_path, "rt") as f:
                data = json.load(f, object_hook=lambda d:Namespace(**d))
                return data

        except Exception as e:
            logging.critical(f"Error in load manifest: {e}")
            return None


    async def _download_using_url(self, sem, entry, client, path, pbar): 

        """
        Function to use object id of file to obtain its download url and use that download url 
        to download required file asynchronously
        """

        successful = False
        try:
            await sem.acquire()

            url = self.get_presigned_url(entry.object_id)
            if not url:
                logging.critical("No url on retrial, try again later")
                
            async with client.get(url['url'], read_bufsize = 4096) as response:
                if response.status!= 200:
                    logging.error(f"Response code: {response.status_code}")
                    if response.status not in no_retry:
                        for _ in range(3):
                            logging.info("Retrying now...")
                            response = await client.get(url['url'], read_bufsize=4096)
                            if response.status == 200:
                                break
                        if response.status!= 200:
                            logging.critical("Response status not 200, try again later")
                        
                response.raise_for_status()

                total_size_in_bytes = int(response.headers.get("content-length"))
                total_downloaded = 0
                filename = (entry.file_name if entry.file_name else entry.object_id)  
                async with aiofiles.open(os.path.join(path, filename), "wb") as f:
                    with tqdm(desc = f"File {entry.file_name}", total = total_size_in_bytes, position = 1, unit_scale = True, unit_divisor = 1024, unit = "B", ncols = 90) as progress:
                        async for data in response.content.iter_chunked(4096):
                            progress.update(len(data))
                            total_downloaded += len(data)
                            await f.write(data)

                if total_size_in_bytes == total_downloaded:
                    pbar.update()
                    successful = True
                     
                if successful:
                    sem.release()

                else:
                    logging.error(f"File {entry.file_name} not downloaded successfully")
                    sem.release()
                    self.unsuccessful.append(entry)
                    
                return successful 

        except Exception as e:
            logging.critical(f"\nError in {entry.file_name}: {e} Type: {e.__class__.__name__}\n")
            self.unsuccessful.append(entry)
            sem.release()
            return successful

        
    def _download_using_object_id(self, object_id, path):

        """
        Function only executing the download functionality of the async code for a single entry 
        """
    
        successful = True
        try:
            url = self.get_presigned_url(object_id)
            print(url)
            if not url:
                logging.critical("No url on retrial, try again later")
                successful = False

            response = requests.get(url['url'], stream = True)
            if response.status_code!= 200:
                    logging.error(f"Response code: {response.status_code}")
                    if response.status_code not in no_retry:
                        for _ in range(3):
                            logging.info("Retrying now...")
                            response = requests.get(url['url'], stream = True)
                            if response.status == 200:
                                break
                        if response.status!= 200:
                            logging.critical("Response status not 200, try again later")
                            successful = False
                    else:
                        successful = False

            response.raise_for_status()

            total_size_in_bytes = int(response.headers.get("content-length"))
            total_downloaded = 0

            index = Gen3Index(self._auth_provider)
            entry = index.get_record(object_id)
            filename = entry['file_name']  
            
            with open(os.path.join(path, filename), "wb") as f:
                for data in response.content.iter_content(4096):
                    total_downloaded += len(data)
                    f.write(data)
            
            if total_size_in_bytes == total_downloaded:
                logging.info(f"File {filename} downloaded successfully")

            else:
                logging.error(f"File {filename} not downloaded successfully")
                successful = False

            return successful

        except Exception as e:
            logging.critical(f"\nError in {object_id}: {e} Type: {e.__class__.__name__}\n")
            return False


async def download_manifest(auth, manifest_file_path, download_path, cred, total_sem):
        
    """
    Function calling download_using_url function for all entries in the manifest asynchronously as tasks,
    gathering all the tasks and logging which files were successful and which weren't
    """
    
    start_time = time.perf_counter()
    logging.info(f"Start time: {start_time}")

    auth = Gen3Auth(refresh_file = f'{cred}')  #obtaining authorisation from credentials 
    manifest = Gen3File(auth, manifest_file_path)
    manifest_list = manifest._load_manifest()
    if not manifest_list:
        logging.error("Nothing to download") 
    logging.info("Done loading manifest")

    tasks = []
    sem = asyncio.Semaphore(value = total_sem) #semaphores to control number of requests to server at a particular moment
    connector = aiohttp.TCPConnector(force_close = True)

    async with aiohttp.ClientSession(timeout = aiohttp.ClientTimeout(600), connector = connector, trust_env = True) as client:
        with tqdm(desc = "Manifest progress", total = len(manifest_list), unit_scale = True, position = 0, unit_divisor = 1024, unit = "B", ncols = 90) as pbar:
        #progress bar to show how many files in the manifest have been downloaded
            for entry in manifest_list:
            #creating a task for each entry 
                tasks.append(asyncio.create_task(manifest._download_using_url(sem, entry, client, download_path, pbar)))
            await asyncio.gather(*tasks)
    
    duration = time.perf_counter() - start_time
    logging.info(f"\nDuration = {duration}\n")
    logging.info(f"Unsuccessful downloads - {manifest.unsuccessful}\n")


def download_single(object_id, path, cred):

    """
    Function calling download_using_object_id function for downloading a single file when provided with the object-ID
    """

    start_time = time.perf_counter()
    logging.info(f"Start time: {start_time}")

    auth = Gen3Auth(refresh_file = f'{cred}')  #obtaining authorisation from credentials 
    file_download = Gen3File(auth)
    """ index = Gen3Index(auth)
    entry = index.get_record(object_id)
    logging.info(entry) """

    result = file_download._download_using_object_id(object_id, path)

    logging.info(f"Download - {result}")

    duration = time.perf_counter() - start_time
    logging.info(f"\nDuration = {duration}\n")
