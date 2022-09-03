import json
import asyncio
import aiohttp
import aiofiles
from cdislogging import get_logger
import time
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
from tqdm import tqdm
from types import SimpleNamespace as Namespace
from cdislogging import get_logger
import os

logging = get_logger("__name__")
unsuccessful = []

class manifest_downloader:

    def __init__(self, manifest_file, auth_provider):
        self.manifest_file = manifest_file
        self._auth_provider = auth_provider
        self.file = Gen3File(self._auth_provider)

    def load_manifest(self):
        """
        Function to convert manifest to python objects, stored in a list
        """
        try:
            with open(self.manifest_file, "rt") as f:
                data = json.load(f, object_hook=lambda d:Namespace(**d))
                #print(data) 
                return data
        except Exception as e:
            logging.critical(f"Error in load manifest: {e}")


    async def download_using_url(self, Sem, entry, client, path, pbar): #pbar
        """
        Function to use object id of file to obtain its download url and use that download url 
        to download required file 
        """
        try:
            await Sem.acquire()
            url = self.file.get_presigned_url(entry.object_id)
            print(url['url'])
            #retry-url
            if not url:
                logging.critical("No url on retrial, try again later")
                unsuccessful.append(entry)
            async with client.get(url['url'], read_bufsize = 4096) as response:
                if response.status!= 200:
                    logging.error(f"Response code: {response.status_code}")
                    for _ in range(3):
                        logging.info("Retrying now...")
                        response = await client.get(url['url'], read_bufsize=4096)
                        if response.status == 200:
                            break
                    if response.status!= 200:
                        logging.critical("Response status not 200, try again later")
                        unsuccessful.append(entry)
                response.raise_for_status()
                total_size_in_bytes = int(response.headers.get("content-length", 0))
                total_downloaded = 0
                filename = (entry.file_name if entry.file_name else entry.object_id)  
                async with aiofiles.open(os.path.join(path, filename), "wb") as f:
                    with tqdm(desc = f"File {entry.file_name}", total = total_size_in_bytes, position = 1, unit_scale = True, unit_divisor = 1024, unit = "B", ncols = 90) as progress:
                        async for data in response.content.iter_chunked(4096):
                            progress.update(len(data))
                            total_downloaded += len(data)
                            pbar.update()
                            await f.write(data)
                if total_size_in_bytes == total_downloaded:
                    Sem.release()
                    return True 
                else:
                    logging.error(f"File {entry.file_name} not downloaded successfully")
                    Sem.release()
                    unsuccessful.append(entry)
                    return False
        except Exception as e:
            logging.critical(f"\nError in {entry.file_name}: {e} Type: {e.__class__.__name__}\n")
            unsuccessful.append(entry)
            Sem.release()
            return False

    async def async_download(auth, manifest_file, download_path, cred):
        s=time.perf_counter()
        logging.info(f"Start time: {s}")
        auth = Gen3Auth(refresh_file = f'{cred}')  #obtaining authorisation from credentials 
        manifest = manifest_downloader(manifest_file, auth)
        manifest_list = manifest.load_manifest()
        logging.info("Done with manifest")
        tasks = []
        Sem = asyncio.Semaphore(value = 100) #semaphores to control number of requests to server at a particular moment
        connector = aiohttp.TCPConnector(force_close = True)
        async with aiohttp.ClientSession(timeout = aiohttp.ClientTimeout(600), connector = connector) as client:
            loop = asyncio.get_running_loop()
            with tqdm(desc = "Manifest progress", total = len(manifest_list), unit_scale = True, position = 0, unit_divisor = 1024, unit = "B", ncols = 90) as pbar:
                for entry in manifest_list:
                #creating tasks for each entry 
                    tasks.append(loop.create_task(manifest.download_using_url(Sem, entry, client, download_path, pbar)))#pbar
                await asyncio.gather(*tasks)
        
        duration = time.perf_counter()-s
        logging.info(f"\nDuration = {duration}\n")
        logging.info(f"Unsuccessful downloads - {unsuccessful}\n")

    