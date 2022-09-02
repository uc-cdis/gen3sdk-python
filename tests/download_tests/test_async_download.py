from unittest.mock import MagicMock, Mock, patch
from unittest import mock
from unittest.mock import AsyncMock
import json
import gen3
import pytest
import requests
from requests import HTTPError
from gen3.file import Gen3File
from gen3.tools.download import download_manifest
from gen3.tools.download.download_manifest import manifest_downloader
import gen3
import aiohttp
import asyncio
from pathlib import Path
from tqdm import tqdm
import os

@pytest.fixture
def download_dir(tmpdir_factory):
    path = tmpdir_factory.mktemp("async_download")
    return path

@pytest.fixture
def download_test_files():
    data = {}
    DIR = Path(__file__).resolve().parent
    with open(Path(DIR, "resources/async_download_test_data.json")) as fin:
        data = json.load(fin)
        for item in data.values():
            item["content_length"] = str(len(item["content"]))
    return data

NO_DOWNLOAD_ACCESS_MESSAGE="""
You do not have access to download data.
You need read permissions on the files specified in the manifest provided
"""
class Test_Async_Download:
    manifest_file = 'resources/manifest_test_1.json' 
    
    def test_load_manifest(self):
        manifest_list = download_manifest.manifest_downloader.load_manifest(self)
        f = open(self.manifest_file)    
        data = json.load(f)
        assert len(data) == len(manifest_list)
    
    @pytest.mark.asyncio
    async def test_download_manifest(self, download_dir, gen3_download):
        with patch("aiohttp.ClientSession") as client:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "file_name": "TestDataSet1.sav",
                "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum."
            }
            client.get.return_value = mock_response
            with patch("gen3.file.requests") as mock_request:
                gen3_download.manifest_file = self.manifest_file
                manifest_list = gen3_download.load_manifest()
                Sem = asyncio.Semaphore(value = 1)
                mock_request.status_code = 200
                sample_url_1 = "http://test.commons.io/user/data/download/dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"
                gen3_download._auth_provider._refresh_token = {"api_key" : "123"}
                pbar = tqdm(desc = "Manifest progress", total = len(manifest_list), unit_scale = True, position = 0, unit_divisor = 1024, unit = "B", ncols = 90, disable = True)
                #for entry in manifest_list:
                mock_request.get().text = json.dumps({"url": sample_url_1})
                await gen3_download.download_using_url(Sem, manifest_list[0], client, download_dir, pbar)
                with open(os.path.join(download_dir, manifest_list[0].file_name), "rt") as fin:
                    assert fin.read() == download_test_files[id]["content"]
    
