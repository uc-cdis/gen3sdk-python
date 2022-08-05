import json
import asyncio
import aiohttp
import aiofiles
from cdislogging import get_logger
import time
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
import httpx
from tqdm import tqdm
from types import SimpleNamespace as Namespace
from cdislogging import get_logger

logging=get_logger("__name__")
unsuccessful=[]

class manifest_downloader:

    def __init__(self,manifest_file):
        self.manifest_file=manifest_file

    def load_manifest(self):
        """
        Function to convert manifest to python objects, stored in a list
        """
        try:
            with open(self.manifest_file,"rt") as f:
                data=json.load(f,object_hook=lambda d:Namespace(**d)) 
                return data
        except Exception as e:
            logging.critical(f"Error in load manifest: {e}")


    async def download_using_url(self,Sem,entry,file,client,path):#,progress_bar_2
        """
        Function to use object id of file to obtain its download url and use that download url 
        to download required file 
        """
        try:
            await Sem.acquire()
            url=file.get_presigned_url(entry.object_id)
            #retry-url
            if not url:
                logging.error("Error in obtaining url")
                for _ in range(0,3):
                    logging.info("Retrying now - getting presigned url")
                    file.get_presigned_url(entry.object_id)
                    if url:
                        break
                if not url: 
                    logging.critical("No url on retrial, try again later")
                    unsuccessful.append(entry)
            async with client.get(url['url'],read_bufsize=4096) as response:
            #async with client.stream("GET",url['url']) as response:
                #retry-response
                #if response.status_code!=200 :
                if response.status!=200:
                    logging.error(f"Response code: {response.status_code}")
                    for _ in range(3):
                        logging.info("Retrying now...")
                        #response=await client.stream("GET",url['url'])
                        response=await client.get(url['url'],read_bufsize=4096)
                        #if response.status_code == 200:
                        if response.status==200:
                            break
                    #if response.status_code!=200:
                    if response.status!=200:
                        logging.critical("Response status not 200, try again later")
                        unsuccessful.append(entry)
                #logging.info(f"Starting download: {entry.file_name}")
                response.raise_for_status()
                total_size_in_bytes = int(response.headers.get("content-length", 0))
                total_downloaded=0
                filename=(entry.file_name if entry.file_name else entry.object_id)  
                async with aiofiles.open(f'{path}/{filename}',"wb") as f:
                    #with tqdm(desc=f"File {entry.file_name}",total=total_size_in_bytes, unit_scale=True, unit_divisor=1024, unit="B") as progress:
                        #num_bytes_downloaded = response.num_bytes_downloaded
                        #async for data in response.aiter_bytes():
                        async for data in response.content.iter_chunked(4096):
                            #progress.update(response.num_bytes_downloaded - num_bytes_downloaded)
                            #num_bytes_downloaded = response.num_bytes_downloaded
                            total_downloaded+=len(data)
                            await f.write(data)
                if total_size_in_bytes == total_downloaded:
                    #logging.info(f"File {entry.file_name} downloaded")
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

async def async_download(manifest_file,download_path,cred):
    s=time.perf_counter()
    logging.info(f"Start time: {s}")
    timeout=httpx.Timeout(600.0)
    manifest=manifest_downloader(manifest_file)
    manifest_list=manifest.load_manifest()
    logging.info("Done with manifest")
    auth = Gen3Auth(refresh_file=f'{cred}') #obtaining authorisation from credentials 
    file=Gen3File(auth)
    tasks=[]
    Sem=asyncio.Semaphore(value=5) #semaphores to control number of requests to server at a particular moment
    connector = aiohttp.TCPConnector(force_close=True)
    #async with httpx.AsyncClient(timeout=timeout) as client: #session for requests
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(600),connector=connector) as client:
        loop=asyncio.get_running_loop()
        for entry in manifest_list:
            #creating tasks for each entry 
            tasks.append(loop.create_task(manifest.download_using_url(Sem,entry,file,client,download_path)))#progress_bar_2
        pbar=tqdm(desc="Manifest progress",total=len(tasks),unit_scale=True,unit_divisor=1024,unit="B",ncols=90)
        for f in asyncio.as_completed(tasks):
            value=await f
            if value:
                pbar.update()
    
    duration=time.perf_counter()-s 
    logging.info(f"\nDuration = {duration}\n")
    logging.info(f"Unsuccessful downloads - {unsuccessful}\n")

