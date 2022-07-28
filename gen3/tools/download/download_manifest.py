import json
import asyncio
import aiofiles
from dataclasses import dataclass,field
from dataclasses_json import dataclass_json,LetterCase
from typing import Optional
import time
from gen3.auth import Gen3Auth
from gen3.file import Gen3File
import httpx

@dataclass_json(letter_case=LetterCase.SNAKE)
@dataclass
class manifest_downloader:
    
    object_id: str  # only required member
    file_name: Optional[str] = None
    file_size: Optional[int] = -1  # -1 indicated not set
    md5sum: Optional[str] = None
    commons_url: Optional[str] = None

    @staticmethod
    def load_manifest(manifest_path):
        with open(manifest_path,"rt") as f:
            data=json.load(f)
            return manifest_downloader.schema().load(data,many=True)

    @staticmethod
    async def download_using_url(Sem,entry,file,client):
        #urls=[]
        await Sem.acquire()
        url=file.get_presigned_url(entry.object_id)
        #url=await manifest_downloader.get_presigned_url(auth,entry,client)
        async with client.stream("GET",url['url']) as response:
            #chunk=8092
            #await response.content.read(chunk)
            print(f"Starting download: {entry.file_name}")
            print(f"Url for {entry.file_name} :{url}")
            #response = requests.get(url['url'],stream=True)
            response.raise_for_status()
            total_size_in_bytes = int(response.headers.get("content-length", 0))
            total_downloaded=0
            filename=entry.file_name    
            async with aiofiles.open(filename,"wb") as f:
                async for data in response.aiter_bytes():
                    total_downloaded+=len(data)
                    await f.write(data)
            if total_size_in_bytes == total_downloaded:
                print(f"File {entry.file_name} downloaded")
            Sem.release()

async def main():
    timeout=httpx.Timeout(60.0)
    manifest_list=manifest_downloader.load_manifest("manifest.json")
    print("Done with manifest")
    auth=Gen3Auth(refresh_file="credentials.json")
    file=Gen3File(auth)
    tasks=[]
    Sem=asyncio.Semaphore(value=5)    
    async with httpx.AsyncClient(timeout=timeout) as client:
        loop=asyncio.get_running_loop()
        for entry in manifest_list:
            tasks.append(loop.create_task(manifest_downloader.download_using_url(Sem,entry,file,client)))
        await asyncio.wait(tasks)
    
    print("Over")

s=time.perf_counter()
asyncio.run(main())
duration=time.perf_counter()-s
print("Duration = ",duration)  