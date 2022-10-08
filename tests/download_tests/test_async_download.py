from unittest.mock import patch
import json
import pytest
from pathlib import Path
import os
import shutil
from types import SimpleNamespace as Namespace

from gen3.file import Gen3File
from gen3.utils import get_or_create_event_loop_for_thread


DIR = Path(__file__).resolve().parent
NO_DOWNLOAD_ACCESS_MESSAGE = """
You do not have access to download data.
You need read permissions on the files specified in the manifest provided
"""


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

    def dict_to_entry(d):
        """
        Ensure the expected keys are in each manifest entry
        """
        if not d.get("object_id"):
            raise Exception(f"Manifest entry missing 'object_id': {d}")
        if "file_name" not in d:
            d["file_name"] = None
        return Namespace(**d)

    try:
        with open(manifest_file_path, "rt") as f:
            data = json.load(f, object_hook=dict_to_entry)
            return data

    except Exception as e:
        print(f"Error in load manifest: {e}")
        return None


# function to create temporary directory to download test files in
@pytest.fixture
def download_dir(tmpdir_factory):
    path = tmpdir_factory.mktemp("async_download")
    return path


# function to download test files
@pytest.fixture
def download_test_files():
    data = {}
    with open(Path(DIR, "resources/download_test_data.json")) as fin:
        data = json.load(fin)
        for item in data.values():
            item["content_length"] = str(len(item["content"]))
    return data


class Test_Async_Download:
    """
    Class containing all test cases for `Gen3File.download_single`
    """

    manifest_file_path = Path(DIR, "resources/manifest_test_1.json")

    def iter_content(
        chunk_size=4096,
        content="Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
    ):
        rest = content
        while rest:
            chunk = rest[:chunk_size]
            rest = rest[chunk_size:]
        return chunk.encode("utf-8")

    def test_load_manifest(self, mock_gen3_auth):
        """
        Testing the load_manifest function, which converts the manifest provided to list of python objects
        Test passes if number of python objects created is equal to number of files specified in manifest
        """
        manifest_list = _load_manifest(self.manifest_file_path)
        f = open(self.manifest_file_path)
        data = json.load(f)
        assert len(data) == len(manifest_list)

    @patch("gen3.file.requests")
    @patch("gen3.index.Gen3Index.get_record")
    @pytest.mark.parametrize("download_dir_overwrite", [None, "sub/path"])
    def test_download_single(
        self,
        mock_index,
        mock_get,
        download_dir,
        download_dir_overwrite,
        download_test_files,
        mock_gen3_auth,
    ):
        """
        Testing the download functionality (function - download_single) in manifest_downloader by comparing file
        content of file to be downloaded and download performed by download_single

        Also checks if download_single function returns True; function returns true when
        response content-length is equal to number of bytes downloaded

        Includes a test for downloading to a sub-directory that does not exist yet.
        """
        file_tool = Gen3File(mock_gen3_auth)
        download_path = (
            os.path.join(DIR, download_dir_overwrite)
            if download_dir_overwrite
            else download_dir
        )

        content = {
            "file_name": "TestDataSet1.json",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
        }

        mock_get.get().iter_content = lambda size: [
            Test_Async_Download.iter_content(
                chunk_size=size, content=content["content"]
            )
        ]

        file_tool.manifest_file_path = self.manifest_file_path
        manifest_list = _load_manifest(self.manifest_file_path)
        mock_get.get().status_code = 200
        file_tool._auth_provider._refresh_token = {"api_key": "123"}
        mock_get.get().headers = {"content-length": str(len(content["content"]))}
        mock_index.return_value = {"file_name": "TestDataSet1.sav"}

        result = file_tool.download_single(manifest_list[0].object_id, download_path)

        try:
            with open(
                os.path.join(DIR, download_path, (manifest_list[0].file_name)), "r"
            ) as fin:

                assert (
                    fin.read()
                    == download_test_files[
                        "dg.XXTS/b96018c5-db06-4af8-a195-28e339ba815e"
                    ]["content"]
                )

            assert result == True
        finally:
            if download_dir_overwrite and os.path.exists(download_path):
                shutil.rmtree(download_path)

    @patch("gen3.file.requests")
    def test_download_single_no_auth(self, mock_get, download_dir, mock_gen3_auth):

        """
        Testing how download_single function reacts when it is given no authorisation details
        Request(url) should return status_code = 403 and download function should return False
        """
        file_tool = Gen3File(mock_gen3_auth)

        content = {
            "file_name": "TestDataSet1.json",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
        }

        mock_get.get().content.iter_content = lambda size: [
            Test_Async_Download.iter_content(
                chunk_size=size, content=content["content"]
            )
        ]

        file_tool.manifest_file_path = self.manifest_file_path
        manifest_list = _load_manifest(self.manifest_file_path)
        mock_get.get().status_code = 403
        file_tool._auth_provider._refresh_token = None
        mock_get.get().headers = {"content-length": str(len(content["content"]))}

        result = file_tool.download_single(manifest_list[0].object_id, download_dir)

        assert result == False

    @patch("gen3.file.requests")
    def test_download_single_wrong_auth(self, mock_get, download_dir, mock_gen3_auth):

        """
        Testing how download_single function reacts when it is given wrong authorisation details
        Request(url) should return status_code = 403 and download function should return False
        """
        file_tool = Gen3File(mock_gen3_auth)

        content = {
            "file_name": "TestDataSet1.json",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
        }

        mock_get.get().content.iter_content = lambda size: [
            Test_Async_Download.iter_content(
                chunk_size=size, content=content["content"]
            )
        ]

        file_tool.manifest_file_path = self.manifest_file_path
        manifest_list = _load_manifest(self.manifest_file_path)
        mock_get.get().status_code = 403
        file_tool._auth_provider._refresh_token = {"api_key": "wrong_auth"}
        mock_get.get().headers = {"content-length": str(len(content["content"]))}

        result = file_tool.download_single(manifest_list[0].object_id, download_dir)

        assert result == False

    @patch("gen3.file.requests")
    def test_download_single_bad_id(self, mock_get, download_dir, mock_gen3_auth):

        """
        Testing how download_single function reacts when it is given a manifest with bad id
        Request(url) should return status_code = 404 (File not found) and download function should return False
        """
        file_tool = Gen3File(mock_gen3_auth)

        content = {
            "file_name": "TestDataSet1.json",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore etdolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquipex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum doloreeu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt inculpa qui officia deserunt mollit anim id est laborum.",
        }

        mock_get.get().content.iter_content = lambda size: [
            Test_Async_Download.iter_content(
                chunk_size=size, content=content["content"]
            )
        ]

        file_tool.manifest_file_path = Path(DIR, "resources/manifest_test_bad_id.json")

        manifest_list = _load_manifest(self.manifest_file_path)
        mock_get.get().status_code = 404
        file_tool._auth_provider._refresh_token = {"api_key": "123"}
        mock_get.get().headers = {"content-length": str(len(content["content"]))}

        result = file_tool.download_single(manifest_list[0].object_id, download_dir)

        assert result == False

    def test_load_manifest_bad_format(self):

        """
        Testing how load_manifest function reacts when it is given a manifest with bad format as input
        Function should not load the manifest and should return an empty manifest_list
        """

        manifest_list = _load_manifest(Path(DIR, "resources/bad_format.json"))
        assert manifest_list == None
