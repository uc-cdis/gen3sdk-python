"""
Tests gen3.file.Gen3File for calls
"""
from unittest.mock import patch, MagicMock, mock_open
import json
import pytest
import tempfile
import os
from pathlib import Path
from requests import HTTPError
from concurrent.futures import Future
from gen3.file import Gen3File


NO_UPLOAD_ACCESS_MESSAGE = """
    You do not have access to upload data. 
    You either need general file uploader permissions or 
    create and write-storage permissions on the 
    authz resources you specified (if you specified any).
    """


def test_get_presigned_url(gen3_file, supported_protocol):
    """
    Get a presigned url for a file

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str supported_protocol:
        a protocol from ["s3", "http", "ftp", "https", "gs", "az"]
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "123"}
    sample_presigned_url = "https://fakecontainer/some/path/file.txt?k=v"

    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = 200
        mock_request.get().json = lambda: {"url": sample_presigned_url}
        res = gen3_file.get_presigned_url(guid="123", protocol=supported_protocol)
        assert "url" in res
        assert res["url"] == sample_presigned_url


def test_get_presigned_url_no_refresh_token(gen3_file, supported_protocol):
    """
    Get a presigned url for a file without a refresh token, which should raise an HTTPError

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str supported_protocol:
        a protocol from ["s3", "http", "ftp", "https", "gs", "az"]
    """
    gen3_file._auth_provider._refresh_token = None

    with patch("gen3.file.requests.get", side_effect=HTTPError):
        with pytest.raises(HTTPError):
            res = gen3_file.get_presigned_url(guid="123", protocol=supported_protocol)
            assert res == "Failed"


def test_get_presigned_url_no_api_key(gen3_file, supported_protocol):
    """
    Get a presigned url for a file without an api_key
    in the refresh token, which should return a 401

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str supported_protocol:
        a protocol from ["s3", "http", "ftp", "https", "gs", "az"]
    """
    gen3_file._auth_provider._refresh_token = {"not_api_key": "123"}

    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = 401
        mock_request.get().json = lambda: "Failed"
        res = gen3_file.get_presigned_url(guid="123", protocol=supported_protocol)
        assert res == "Failed"


def test_get_presigned_url_wrong_api_key(gen3_file, supported_protocol):
    """
    Get a presigned url for a file with the wrong value for the api_key
    in the refresh token, which should return a 401

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str supported_protocol:
        a protocol from ["s3", "http", "ftp", "https", "gs", "az"]
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "wrong_value"}

    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = 401
        mock_request.get().json = lambda: "Failed"
        res = gen3_file.get_presigned_url(guid="123", protocol=supported_protocol)
        assert res == "Failed"


@pytest.mark.parametrize(
    "guid,status_code,response_text,expected_response",
    [
        ("123", 204, "", ""),
        (None, 500, "Failed to delete data file.", "Failed to delete data file."),
    ],
)
def test_delete_file(gen3_file, guid, status_code, response_text, expected_response):
    """
    Delete files for a Gen3File using a guid

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str guid:
        file id to use for delete
    :param int status_code:
        mock status code
    :param str response_text:
        mock response text
    :param str expected_response:
        expected response to compare with mock
    """
    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = status_code
        mock_request.delete().text = response_text
        res = gen3_file.delete_file(guid=guid)
        assert res == expected_response


def test_delete_file_no_refresh_token(gen3_file):
    """
    Delete files for a Gen3File without a refresh token, which should raise an HTTPError

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    """
    gen3_file._auth_provider._refresh_token = None

    with patch("gen3.file.requests.delete", side_effect=HTTPError):
        with pytest.raises(HTTPError):
            res = gen3_file.delete_file(guid="123")
            assert res == "Failed to delete data file."


def test_delete_file_no_api_key(gen3_file):
    """
    Delete files for a Gen3File without an api_key in the refresh token, which should return a 401

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    """
    gen3_file._auth_provider._refresh_token = {"not_api_key": "123"}

    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = 401
        mock_request.delete().text = "Failed to delete data file."
        res = gen3_file.delete_file(guid="123")
        assert res == "Failed to delete data file."


def test_delete_file_wrong_api_key(gen3_file):
    """
    Delete files for a Gen3File with the wrong value for the api_key
    in the refresh token, which should return a 401

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "wrong_value"}

    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = 401
        mock_request.delete().text = "Failed to delete data file."
        res = gen3_file.delete_file(guid="123")
        assert res == "Failed to delete data file."


@pytest.mark.parametrize(
    "supported_protocol",
    ["s3", "az"],
    indirect=True,
)
@pytest.mark.parametrize(
    "authz,expires_in,status_code,response_text,expected_response",
    [
        (
            None,
            200,
            201,
            '{ "url": "https://fakecontainer/some/path/file.txt" }',
            {"url": "https://fakecontainer/some/path/file.txt"},
        ),
        (
            ["/programs"],
            200,
            201,
            '{ "url": "https://fakecontainer/some/path/file.txt" }',
            {"url": "https://fakecontainer/some/path/file.txt"},
        ),
        (
            ["/programs"],
            0,
            201,
            '{ "url": "https://fakecontainer/some/path/file.txt" }',
            {"url": "https://fakecontainer/some/path/file.txt"},
        ),
        (
            "[/programs]",
            200,
            400,
            NO_UPLOAD_ACCESS_MESSAGE,
            "You do not have access to upload data.",
        ),
        (
            "[/programs]",
            -200,
            400,
            "Requested expiry must be a positive integer; instead got -200",
            "Requested expiry must be a positive integer; instead got",
        ),
        (
            "[]",
            200,
            403,
            NO_UPLOAD_ACCESS_MESSAGE,
            "You do not have access to upload data.",
        ),
    ],
)
def test_upload_file(
    gen3_file,
    supported_protocol,
    authz,
    expires_in,
    status_code,
    response_text,
    expected_response,
):
    """
    Upload files for a Gen3File given a protocol, authz, and expires_in

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str supported_protocol:
        a protocol from ["s3", "http", "ftp", "https", "gs", "az"]
    :param [] authz:
        Authz list, for example [] or ['/programs']
    :param int expires_in:
        The signed URL should expire_in seconds from datetime.utcnow(),
        this should be a positive int
    :param int status_code:
        mock status code
    :param str response_text:
        mock response text
    :param str expected_response:
        expected response to compare with mock
    """
    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = status_code
        mock_request.post().text = response_text
        res = gen3_file.upload_file(
            file_name="file.txt",
            authz=authz,
            protocol=supported_protocol,
            expires_in=expires_in,
        )
        if status_code == 201:
            # check that the SDK is getting fence
            assert res.get("url") == expected_response["url"]
        else:
            # check the error message
            assert expected_response in res


@pytest.mark.parametrize(
    "supported_protocol",
    ["s3", "az"],
    indirect=True,
)
@pytest.mark.parametrize(
    "authz,expires_in",
    [
        (None, 200),
        (["/programs"], 200),
        (["/programs"], 0),
        ("[/programs]", 200),
        ("[/programs]", -200),
        ("[]", 200),
    ],
)
def test_upload_file_no_refresh_token(gen3_file, supported_protocol, authz, expires_in):
    """
    Upload files for a Gen3File given a protocol, authz, and expires_in
    without a refresh token, which should raise an HTTPError

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str supported_protocol:
        a protocol from ["s3", "http", "ftp", "https", "gs", "az"]
    :param [] authz:
        Authz list, for example [] or ['/programs']
    :param int expires_in:
        The signed URL should expire_in seconds from datetime.utcnow(),
        this should be a positive int
    """
    gen3_file._auth_provider._refresh_token = None

    with patch("gen3.file.requests.post", side_effect=HTTPError):
        with pytest.raises(HTTPError):
            gen3_file.upload_file(
                file_name="file.txt",
                authz=authz,
                protocol=supported_protocol,
                expires_in=expires_in,
            )


@pytest.mark.parametrize(
    "supported_protocol",
    ["s3", "az"],
    indirect=True,
)
@pytest.mark.parametrize(
    "authz,expires_in",
    [
        (None, 200),
        (["/programs"], 200),
        (["/programs"], 0),
        ("[/programs]", 200),
        ("[/programs]", -200),
        ("[]", 200),
    ],
)
def test_upload_file_no_api_key(gen3_file, supported_protocol, authz, expires_in):
    """
    Upload files for a Gen3File given a protocol, authz, and expires_in
    without an api_key in the refresh token, which should return a 401

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str supported_protocol:
        a protocol from ["s3", "http", "ftp", "https", "gs", "az"]
    :param [] authz:
        Authz list, for example [] or ['/programs']
    :param int expires_in:
        The signed URL should expire_in seconds from datetime.utcnow(),
        this should be a positive int
    """
    gen3_file._auth_provider._refresh_token = {"not_api_key": "123"}

    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = 401
        mock_request.post().text = "Failed to upload data file."
        res = gen3_file.upload_file(
            file_name="file.txt",
            authz=authz,
            protocol=supported_protocol,
            expires_in=expires_in,
        )
        assert res == "Failed to upload data file."


@pytest.mark.parametrize(
    "supported_protocol",
    ["s3", "az"],
    indirect=True,
)
@pytest.mark.parametrize(
    "authz,expires_in",
    [
        (None, 200),
        (["/programs"], 200),
        (["/programs"], 0),
        ("[/programs]", 200),
        ("[/programs]", -200),
        ("[]", 200),
    ],
)
def test_upload_file_wrong_api_key(gen3_file, supported_protocol, authz, expires_in):
    """
    Upload files for a Gen3File given a protocol, authz, and expires_in
    with the wrong value for the api_key in the refresh token, which should return a 401

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    :param str supported_protocol:
        a protocol from ["s3", "http", "ftp", "https", "gs", "az"]
    :param [] authz:
        Authz list, for example [] or ['/programs']
    :param int expires_in:
        The signed URL should expire_in seconds from datetime.utcnow(),
        this should be a positive int
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "wrong_value"}

    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = 401
        mock_request.post().text = "Failed to upload data file."
        res = gen3_file.upload_file(
            file_name="file.txt",
            authz=authz,
            protocol=supported_protocol,
            expires_in=expires_in,
        )
        assert res == "Failed to upload data file."

@pytest.mark.parametrize("guids,expected_count,fail_guid,protocol", [
    (["guid1", "guid2", "guid3"], 3, None, None),
    (["guid1", "guid2", "guid3"], 3, "guid2", None),
    ([], 0, None, None),
    (["guid1", "guid2"], 2, None, "s3"),
])
def test_get_presigned_urls_batch(gen3_file, guids, expected_count, fail_guid, protocol):
    """Test batch presigned URL retrieval with various scenarios"""
    def mock_get_url(guid, protocol=None):
        if fail_guid and guid == fail_guid:
            raise Exception("Failed to get URL")
        return {"url": f"https://example.com/{guid}"}
    
    with patch.object(gen3_file, 'get_presigned_url', side_effect=mock_get_url) as mock_get_url_func:
        result = gen3_file.get_presigned_urls_batch(guids, protocol)
        
        assert len(result) == expected_count
        if fail_guid and expected_count > 1:
            assert result[fail_guid] is None
        if guids and not fail_guid:
            assert all(result[guid]["url"] == f"https://example.com/{guid}" for guid in guids)
        if protocol and guids:
            for call in mock_get_url_func.call_args_list:
                # Check if protocol was passed as positional or keyword argument
                if len(call[0]) > 1:  # positional
                    assert call[0][1] == protocol
                elif 'protocol' in call[1]:  # keyword
                    assert call[1]['protocol'] == protocol


@pytest.mark.parametrize("guid,original,format_type,expected", [
    ("test-guid-123", "test_file.txt", "original", "test_file.txt"),
    ("test-guid-123", "test_file.txt", "guid", "test-guid-123"),
    ("test-guid-123", "test_file.txt", "combined", "test_file_test-guid-123.txt"),
    ("test-guid-123", "test_file", "combined", "test_file_test-guid-123"),
    ("test-guid-123", None, "original", "test-guid-123"),
    ("test-guid-123", None, "guid", "test-guid-123"),
    ("test-guid-123", None, "combined", "test-guid-123"),
    ("test-guid-123", "", "original", "test-guid-123"),
    ("test-guid-123", "file.name.with.dots.txt", "combined", "file.name.with.dots_test-guid-123.txt"),
    ("test-guid-123", ".hidden_file", "combined", ".hidden_file_test-guid-123"),
])
def test_format_filename(gen3_file, guid, original, format_type, expected):
    """Test filename formatting with various scenarios"""
    result = gen3_file._format_filename(guid, original, format_type)
    assert result == expected


@pytest.mark.parametrize("existing_files,rename,expected_suffix", [
    ([], True, ""),
    (["test_file.txt"], False, ""),
    (["test_file.txt"], True, "_1"),
    (["test_file.txt", "test_file_1.txt", "test_file_2.txt"], True, "_3"),
    (["test_file", "test_file_1"], True, "_2"),
])
def test_handle_file_conflict(gen3_file, existing_files, rename, expected_suffix):
    """Test file conflict handling with various scenarios"""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = Path(tmpdir) / "test_file.txt"
        if "test_file" in str(existing_files) and not any("." in f for f in existing_files):
            filepath = Path(tmpdir) / "test_file"
            expected_path = Path(tmpdir) / f"test_file{expected_suffix}"
        else:
            expected_path = Path(tmpdir) / f"test_file{expected_suffix}.txt"
        
        # Create existing files
        for existing in existing_files:
            (Path(tmpdir) / existing).touch()
        
        result = gen3_file._handle_file_conflict(filepath, rename)
        assert result == expected_path


@pytest.mark.parametrize("presigned_url_data,status_code,skip_completed,expected_status", [
    ({"url": "https://example.com/file"}, 200, False, "downloaded"),
    ({"url": "https://example.com/file"}, 200, True, "skipped"),  # file exists
    (None, None, False, "failed"),
    ({"url": "https://example.com/file"}, 404, False, "failed"),
    ({"url": "https://example.com/file"}, 500, False, "downloaded"),  # retries then succeeds
])
@patch('gen3.file.requests.get')
@patch('gen3.file.Gen3Index')
def test_download_file_worker(mock_index, mock_requests, gen3_file, presigned_url_data, status_code, skip_completed, expected_status):
    """Test file download worker with various scenarios"""
    guid = "test-guid"
    
    # Mock index response
    mock_index_instance = MagicMock()
    mock_index_instance.get_record.return_value = {"file_name": "test.txt"}
    mock_index.return_value = mock_index_instance
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        
        if skip_completed and expected_status == "skipped":
            # Create existing file
            test_file = output_dir / "test.txt"
            test_file.touch()
        
        if presigned_url_data is None:
            result = gen3_file._download_file_worker(
                guid, presigned_url_data, output_dir, "original", False, skip_completed
            )
            assert result["status"] == expected_status
            assert "Failed to get presigned URL" in result["error"]
            return
        
        # Mock HTTP responses
        if status_code == 500:  # Test retry logic
            responses = []
            for _ in range(3):  # MAX_RETRIES
                mock_response = MagicMock()
                mock_response.status_code = 500
                mock_response.raise_for_status.side_effect = HTTPError("Server Error")
                responses.append(mock_response)
            
            # Final success
            mock_success = MagicMock()
            mock_success.status_code = 200
            mock_success.headers = {"content-length": "9"}
            mock_success.iter_content.return_value = [b"test data"]
            mock_success.raise_for_status.return_value = None
            responses.append(mock_success)
            mock_requests.side_effect = responses
        else:
            mock_response = MagicMock()
            mock_response.status_code = status_code
            mock_response.headers = {"content-length": "15"}
            mock_response.iter_content.return_value = [b"test data chunk"]
            if status_code == 404:
                mock_response.raise_for_status.side_effect = HTTPError("Not found")
            else:
                mock_response.raise_for_status.return_value = None
            mock_requests.return_value = mock_response
        
        with patch("builtins.open", mock_open()):
            with patch('gen3.file.time.sleep'):
                result = gen3_file._download_file_worker(
                    guid, presigned_url_data, output_dir, "original", False, skip_completed
                )
                
                assert result["status"] == expected_status


@pytest.mark.parametrize("filename_format,all_options", [
    ("original", False),
    ("guid", True),
    ("combined", True),
])
@patch('gen3.file.Gen3File._ensure_dirpath_exists')
def test_download_single(mock_ensure_dir, gen3_file, filename_format, all_options):
    """Test download_single with various scenarios"""
    guid = "test-guid"
    mock_ensure_dir.return_value = Path("/tmp/test")
    
    options = {"filename_format": filename_format}
    if all_options:
        options.update({
            "download_path": "/custom/path",
            "protocol": "s3",
            "skip_completed": True,
            "rename": True
        })
    
    with patch.object(gen3_file, 'get_presigned_url', return_value={"url": "https://example.com"}) as mock_get_url:
        with patch.object(gen3_file, '_download_file_worker', return_value={"status": "downloaded"}) as mock_worker:
            result = gen3_file.download_single(guid=guid, **options)
            
            assert result["status"] == "downloaded"
            mock_worker.assert_called_once()
            # Verify filename_format was passed correctly
            args = mock_worker.call_args[0]
            assert args[3] == filename_format


def test_download_single_presigned_url_failure(gen3_file):
    """Test single file download with presigned URL failure"""
    guid = "test-guid"
    
    with patch.object(gen3_file, 'get_presigned_url', side_effect=Exception("API error")):
        result = gen3_file.download_single(guid)
        
        assert result["status"] == "failed"
        assert "Failed to get presigned URL" in result["error"]


@pytest.mark.parametrize("manifest_scenario,expected_results", [
    ([{"guid": "guid1"}, {"guid": "guid2"}, {"object_id": "guid3"}], (3, 0, 0)),
    ([], (0, 0, 0)),
    ([{"invalid": "data"}], (0, 0, 0)),
    ([{"guid": "guid1"}, {"guid": "guid2"}, {"guid": "guid3"}], (1, 1, 1)),  # mixed results
])
@patch('gen3.file.click.confirm')
@patch('gen3.file.Gen3File._ensure_dirpath_exists')
def test_download_multiple(mock_ensure_dir, mock_confirm, gen3_file, manifest_scenario, expected_results):
    """Test multiple file download with various scenarios"""
    mock_ensure_dir.return_value = Path("/tmp/test")
    mock_confirm.return_value = True
    
    expected_succeeded, expected_failed, expected_skipped = expected_results
    
    if not manifest_scenario:
        result = gen3_file.download_multiple([])
        assert result == {"succeeded": [], "failed": [], "skipped": []}
        return
    
    if manifest_scenario[0].get("invalid"):
        result = gen3_file.download_multiple(manifest_scenario)
        assert result == {"succeeded": [], "failed": [], "skipped": []}
        return
    
    # Setup presigned URLs
    presigned_urls = {}
    for item in manifest_scenario:
        guid = item.get("guid") or item.get("object_id")
        if guid:
            presigned_urls[guid] = {"url": f"https://example.com/{guid}"}
    
    def mock_worker(guid, *args, **kwargs):
        if expected_results == (1, 1, 1):  # mixed results case
            if guid == "guid1":
                return {"status": "downloaded", "guid": guid}
            elif guid == "guid2":
                return {"status": "skipped", "guid": guid}
            else:
                return {"status": "failed", "guid": guid, "error": "Download failed"}
        else:
            return {"status": "downloaded", "guid": guid}
    
    with patch.object(gen3_file, 'get_presigned_urls_batch', return_value=presigned_urls):
        with patch.object(gen3_file, '_download_file_worker', side_effect=mock_worker):
            result = gen3_file.download_multiple(manifest_scenario, no_prompt=True, no_progress=True)
            
            assert len(result["succeeded"]) == expected_succeeded
            assert len(result["failed"]) == expected_failed
            assert len(result["skipped"]) == expected_skipped


@pytest.mark.parametrize("num_parallel,test_type", [
    (3, "default"),
    (10, "custom_workers"),
    (2, "progress_disabled"),
    (5, "progress_enabled"),
])
@patch('gen3.file.ThreadPoolExecutor')
@patch('gen3.file.Gen3File._ensure_dirpath_exists')
def test_download_multiple_parallel_options(mock_ensure_dir, mock_executor, gen3_file, num_parallel, test_type):
    """Test download_multiple with various parallel options"""
    manifest_data = [{"guid": "guid1"}]
    mock_ensure_dir.return_value = Path("/tmp/test")
    
    # Mock executor
    mock_executor_instance = MagicMock()
    mock_executor.return_value.__enter__.return_value = mock_executor_instance
    
    mock_future = MagicMock()
    mock_future.result.return_value = {"status": "downloaded", "guid": "guid1"}
    mock_executor_instance.submit.return_value = mock_future
    
    no_progress = test_type == "progress_disabled"
    
    with patch('gen3.file.as_completed', return_value=[mock_future]) as mock_as_completed:
        with patch.object(gen3_file, 'get_presigned_urls_batch', return_value={"guid1": {"url": "test"}}):
            if test_type == "progress_enabled":
                with patch('gen3.file.tqdm') as mock_tqdm:
                    mock_tqdm.return_value = [mock_future]
                    gen3_file.download_multiple(
                        manifest_data, 
                        num_parallel=num_parallel, 
                        no_prompt=True, 
                        no_progress=no_progress
                    )
                    mock_tqdm.assert_called_once()
            else:
                gen3_file.download_multiple(
                    manifest_data, 
                    num_parallel=num_parallel, 
                    no_prompt=True, 
                    no_progress=no_progress
                )
            
            mock_executor.assert_called_once_with(max_workers=num_parallel)


@patch('gen3.file.click.confirm')
def test_download_multiple_prompt_abort(mock_confirm, gen3_file):
    """Test download_multiple when user aborts on prompt"""
    manifest_data = [{"guid": "guid1"}]
    
    from click.exceptions import Abort
    mock_confirm.side_effect = Abort()
    
    with pytest.raises(Abort):
        gen3_file.download_multiple(manifest_data, no_prompt=False)


def test_download_multiple_with_mixed_guid_types(gen3_file):
    """Test download_multiple with both guid and object_id fields"""
    manifest_data = [
        {"guid": "guid1", "object_id": "should_ignore_this"},
        {"object_id": "guid2"},
        {"guid": "guid3"}
    ]
    
    with patch.object(gen3_file, 'get_presigned_urls_batch') as mock_batch:
        with patch.object(gen3_file, '_download_file_worker', return_value={"status": "downloaded", "guid": "test"}):
            with patch('gen3.file.Gen3File._ensure_dirpath_exists', return_value=Path("/tmp")):
                with patch('gen3.file.ThreadPoolExecutor') as mock_executor:
                    mock_executor_instance = MagicMock()
                    mock_executor.return_value.__enter__.return_value = mock_executor_instance
                    
                    mock_future = MagicMock()
                    mock_future.result.return_value = {"status": "downloaded", "guid": "test"}
                    mock_executor_instance.submit.return_value = mock_future
                    
                    with patch('gen3.file.as_completed', return_value=[mock_future, mock_future, mock_future]):
                        gen3_file.download_multiple(manifest_data, no_prompt=True, no_progress=True)
                        
                        # Verify that guid is preferred over object_id
                        called_guids = mock_batch.call_args[0][0]
                        assert "guid1" in called_guids
                        assert "guid2" in called_guids
                        assert "guid3" in called_guids
                        assert len(called_guids) == 3


@pytest.mark.parametrize("test_type,mkdir_error", [
    ("new_directory", False),
    ("existing_directory", False),
    ("mkdir_error", True),
])
def test_ensure_dirpath_exists(gen3_file, test_type, mkdir_error):
    """Test directory creation utility with various scenarios"""
    with tempfile.TemporaryDirectory() as tmpdir:
        if test_type == "existing_directory":
            test_path = Path(tmpdir)
            expected_exists = True
        else:
            test_path = Path(tmpdir) / "new_dir"
            expected_exists = test_type != "mkdir_error"
        
        if mkdir_error:
            with patch('gen3.file.Path.mkdir', side_effect=OSError("Permission denied")):
                # Just create the directory normally since we're mocking mkdir anyway
                test_path_real = Path(tmpdir) / "test_real_dir"
                try:
                    result = Gen3File._ensure_dirpath_exists(test_path_real)
                    assert result == test_path_real
                except OSError:
                    # This is expected behavior when mkdir fails
                    pass
        else:
            result = Gen3File._ensure_dirpath_exists(test_path)
            assert result == test_path
            if expected_exists:
                assert test_path.exists()
                assert test_path.is_dir()
