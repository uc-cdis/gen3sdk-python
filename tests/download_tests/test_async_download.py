from unittest.mock import MagicMock, Mock, patch
from unittest import mock
import json
import gen3
import pytest
import requests
from requests import HTTPError
from gen3.file import Gen3File
from gen3.tools.download import download_manifest
from gen3.tools.download.download_manifest import manifest_downloader
import gen3
from pathlib import Path
import os

#function to create temporary directory to download test files in 
@pytest.fixture
def download_dir(tmpdir_factory):
    path = tmpdir_factory.mktemp("async_download")
    return path

#function to download test files, to compare with the download in download_manifest
@pytest.fixture
def download_test_files():
    data = {}
    DIR = Path(__file__).resolve().parent
    with open(Path(DIR, "resources/download_test_data.json")) as fin:
        data = json.load(fin)
        for item in data.values():
            item["content_length"] = str(len(item["content"]))
    return data


NO_DOWNLOAD_ACCESS_MESSAGE = """
You do not have access to download data.
You need read permissions on the files specified in the manifest provided
"""


class Test_Async_Download:
    """
    Class containing all test cases for class manifest_downloader
    """
    DIR = Path(__file__).resolve().parent
    manifest_file = Path(DIR, "resources/manifest_test_1.json")

    def iter_content(
        chunk_size=4096,
        content="Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
    ):
        rest = content
        while rest:
            chunk = rest[:chunk_size]
            rest = rest[chunk_size:]
        return chunk.encode("utf-8")

    def test_load_manifest(self):
        """
        Testing the load_manifest function, which converts the manifest provided to list of python objects
        Test passes if number of python objects created is equal to number of files specified in manifest 
        """
        manifest_list = download_manifest.manifest_downloader.load_manifest(self)
        f = open(self.manifest_file)
        data = json.load(f)
        assert len(data) == len(manifest_list)


    @patch("gen3.file.requests")
    @patch("gen3.tools.download.download_manifest.requests")
    def test_download_manifest(
        self, mock_get, mock_request, download_dir, gen3_download, download_test_files
    ):
        """
        Testing the download functionality (function - download_single) in manifest_downloader by comparing file 
        content of file to be downloaded and download performed by download_single

        Also checks if download_single function returns True; function returns true when 
        response content-length is equal to number of bytes downloaded
        """
        sample_url_1 = "http://test.commons.io/user/data/download/dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"

        content = {
            "file_name": "TestDataSet1.json",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
        }

        mock_get.get().content.iter_content = lambda size: [
            Test_Async_Download.iter_content(
                chunk_size=size, content=content["content"]
            )
        ]

        gen3_download.manifest_file = self.manifest_file

        manifest_list = gen3_download.load_manifest()

        mock_get.get().status_code = 200

        gen3_download._auth_provider._refresh_token = {"api_key": "123"}

        mock_get.get().headers = {'content-length': str(len(content['content']))}

        result = gen3_download.download_single(manifest_list[0], download_dir)

        DIR = Path(__file__).resolve().parent

        with open(
            os.path.join(DIR, download_dir, manifest_list[0].file_name), "r"
        ) as fin:

            assert (
                fin.read()
                == download_test_files["dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"][
                    "content"
                ]
            )

        assert result == True
    
    @patch("gen3.file.requests")
    @patch("gen3.tools.download.download_manifest.requests")
    def test_download_manifest_no_auth(
        self, mock_get, mock_request, download_dir, gen3_download, download_test_files
    ):

        """
        Testing how download_single function reacts when it is given no authorisation details
        Request(url) should return status_code = 403 and download function should return False
        """
        
        sample_url_1 = "http://test.commons.io/user/data/download/dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"

        content = {
            "file_name": "TestDataSet1.json",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
        }

        mock_get.get().content.iter_content = lambda size: [
            Test_Async_Download.iter_content(
                chunk_size=size, content=content["content"]
            )
        ]

        gen3_download.manifest_file = self.manifest_file

        manifest_list = gen3_download.load_manifest()

        mock_get.get().status_code = 403

        gen3_download._auth_provider._refresh_token = None

        mock_get.get().headers = {'content-length': str(len(content['content']))}

        result = gen3_download.download_single(manifest_list[0], download_dir)

        assert result == False

    @patch("gen3.file.requests")
    @patch("gen3.tools.download.download_manifest.requests")
    def test_download_manifest_wrong_auth(
        self, mock_get, mock_request, download_dir, gen3_download, download_test_files
    ):

        """
        Testing how download_single function reacts when it is given wrong authorisation details
        Request(url) should return status_code = 403 and download function should return False
        """

        sample_url_1 = "http://test.commons.io/user/data/download/dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"

        content = {
            "file_name": "TestDataSet1.json",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
        }

        mock_get.get().content.iter_content = lambda size: [
            Test_Async_Download.iter_content(
                chunk_size=size, content=content["content"]
            )
        ]

        gen3_download.manifest_file = self.manifest_file

        manifest_list = gen3_download.load_manifest()

        mock_get.get().status_code = 403

        gen3_download._auth_provider._refresh_token = {"api_key" : "wrong_auth"}

        mock_get.get().headers = {'content-length': str(len(content['content']))}

        result = gen3_download.download_single(manifest_list[0], download_dir)

        assert result == False
    
    @patch("gen3.file.requests")
    @patch("gen3.tools.download.download_manifest.requests")
    def test_download_bad_manifest_id(
        self, mock_get, mock_request, download_dir, gen3_download, download_test_files
    ):

        """
        Testing how download_single function reacts when it is given a manifest with bad id
        Request(url) should return status_code = 404 (File not found) and download function should return False
        """

        sample_url_1 = "http://test.commons.io/user/data/download/dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"

        content = {
            "file_name": "TestDataSet1.json",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
        }

        mock_get.get().content.iter_content = lambda size: [
            Test_Async_Download.iter_content(
                chunk_size=size, content=content["content"]
            )
        ]

        DIR = Path(__file__).resolve().parent 
        gen3_download.manifest_file = Path(DIR, "resources/manifest_test_bad_id.json")

        manifest_list = gen3_download.load_manifest()

        mock_get.get().status_code = 404

        gen3_download._auth_provider._refresh_token = {"api_key" : "123"}

        mock_get.get().headers = {'content-length': str(len(content['content']))}

        result = gen3_download.download_single(manifest_list[0], download_dir)

        assert result == False

    
    def test_download_bad_manifest_format(
        self, gen3_download
    ):
        
        """
        Testing how load_manifest function reacts when it is given a manifest with bad format as input
        Function should not load the manifest and should return an empty manifest_list
        """
        
        DIR = Path(__file__).resolve().parent
        gen3_download.manifest_file = Path(DIR, 'resources/bad_format.json')
        manifest_list = gen3_download.load_manifest()
        assert manifest_list == None
