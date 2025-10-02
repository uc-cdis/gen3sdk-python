"""
Tests gen3.file.Gen3File for calls
"""
from unittest.mock import patch, MagicMock
import pytest
import tempfile
from pathlib import Path
from requests import HTTPError


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
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.text = response_text
        mock_response.json.return_value = expected_response if status_code == 201 else {}

        # Make raise_for_status() raise HTTPError for non-2xx status codes
        if status_code >= 400:
            mock_response.raise_for_status.side_effect = HTTPError()

        mock_request.post.return_value = mock_response

        if status_code == 201:
            res = gen3_file.upload_file(
                file_name="file.txt",
                authz=authz,
                protocol=supported_protocol,
                expires_in=expires_in,
            )
            # check that the SDK is getting fence
            assert res.get("url") == expected_response["url"]
        else:
            # For non-201 status codes, the method should raise an exception
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
    without an api_key in the refresh token, which should raise an HTTPError

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
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Failed to upload data file."
        mock_response.raise_for_status.side_effect = HTTPError()
        mock_request.post.return_value = mock_response

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
def test_upload_file_wrong_api_key(gen3_file, supported_protocol, authz, expires_in):
    """
    Upload files for a Gen3File given a protocol, authz, and expires_in
    with the wrong value for the api_key in the refresh token, which should raise an HTTPError

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
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Failed to upload data file."
        mock_response.raise_for_status.side_effect = HTTPError()
        mock_request.post.return_value = mock_response

        with pytest.raises(HTTPError):
            gen3_file.upload_file(
                file_name="file.txt",
                authz=authz,
                protocol=supported_protocol,
                expires_in=expires_in,
            )


@pytest.fixture
def mock_manifest_data():
    return [
        {"guid": "test-guid-1", "file_name": "file1.txt"},
        {"guid": "test-guid-2", "file_name": "file2.txt"},
        {"object_id": "test-guid-3", "file_name": "file3.txt"},
    ]


def test_download_single_success(gen3_file):
    """
    Test successful download of a single file via download_single method.

    Verifies that download_single correctly delegates to async_download_multiple
    and returns a success status with the filepath.
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "123"}

    with patch.object(gen3_file, 'async_download_multiple') as mock_async:
        mock_async.return_value = {"succeeded": ["test-guid"], "failed": [], "skipped": []}

        result = gen3_file.download_single(guid="test-guid", download_path="/tmp")

        assert result["status"] == "downloaded"
        assert "test-guid" in result["filepath"]
        mock_async.assert_called_once()


def test_download_single_failed(gen3_file):
    """
    Test failed download of a single file via download_single method.

    Verifies that download_single correctly handles failures from
    async_download_multiple and returns a failed status.
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "123"}

    with patch.object(gen3_file, 'async_download_multiple') as mock_async:
        mock_async.return_value = {"succeeded": [], "failed": ["test-guid"], "skipped": []}

        result = gen3_file.download_single(guid="test-guid")

        assert result["status"] == "failed"


@pytest.mark.asyncio
async def test_async_download_multiple_empty_manifest(gen3_file):
    """
    Test async_download_multiple with an empty manifest.

    Verifies that calling async_download_multiple with an empty manifest
    returns empty succeeded, failed, and skipped lists.
    """
    result = await gen3_file.async_download_multiple(manifest_data=[])
    assert result == {"succeeded": [], "failed": [], "skipped": []}


@pytest.mark.asyncio
async def test_async_download_multiple_success(gen3_file, mock_manifest_data):
    """
    Test successful async download of multiple files.

    Verifies that async_download_multiple correctly processes a manifest with
    multiple files and returns all downloads as successful.
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "123"}
    gen3_file._auth_provider.get_access_token = MagicMock(return_value="fake_token")

    with patch('gen3.file.mp.Process'), patch('gen3.file.mp.Queue') as mock_queue, patch('threading.Thread'):
        mock_input_queue = MagicMock()
        mock_output_queue = MagicMock()
        mock_queue.side_effect = [mock_input_queue, mock_output_queue]

        mock_output_queue.get.side_effect = [
            [{"guid": "test-guid-1", "status": "downloaded"}],
            [{"guid": "test-guid-2", "status": "downloaded"}],
            [{"guid": "test-guid-3", "status": "downloaded"}],
        ]

        result = await gen3_file.async_download_multiple(manifest_data=mock_manifest_data, download_path="/tmp")

        assert len(result["succeeded"]) == 3


def test_get_presigned_urls_batch(gen3_file):
    """
    Test batch retrieval of presigned URLs for multiple GUIDs.

    Verifies that get_presigned_urls_batch correctly calls get_presigned_url
    for each GUID and returns a mapping of results.
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "123"}

    with patch.object(gen3_file, 'get_presigned_url') as mock_get_url:
        mock_get_url.return_value = {"url": "https://example.com/presigned"}

        results = gen3_file.get_presigned_urls_batch(["guid1", "guid2"])

        assert len(results) == 2
        assert mock_get_url.call_count == 2


def test_format_filename_static():
    """
    Test the static _format_filename_static method with different filename formats.

    Verifies that files can be formatted as original, guid-only, or combined
    (filename_guidXXX.ext) based on the format parameter.
    """
    from gen3.file import Gen3File

    assert Gen3File._format_filename_static("guid123", "test.txt", "original") == "test.txt"
    assert Gen3File._format_filename_static("guid123", "test.txt", "guid") == "guid123"
    assert Gen3File._format_filename_static("guid123", "test.txt", "combined") == "test_guid123.txt"


def test_handle_conflict_static():
    """
    Test the static _handle_conflict_static method for file conflict resolution.

    Verifies that existing files can be either kept or renamed with a numeric
    suffix based on the rename parameter.
    """
    from gen3.file import Gen3File

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        existing_file = temp_path / "existing.txt"
        existing_file.write_text("test")

        result = Gen3File._handle_conflict_static(existing_file, rename=False)
        assert result == existing_file

        result = Gen3File._handle_conflict_static(existing_file, rename=True)
        assert result.name == "existing_1.txt"


@pytest.mark.parametrize("skip_completed,rename", [(True, False), (False, True)])
def test_download_single_options(gen3_file, skip_completed, rename):
    """
    Test download_single with various option combinations.

    Verifies that skip_completed and rename options are correctly passed
    to async_download_multiple, and no_progress is set to True.
    """
    gen3_file._auth_provider._refresh_token = {"api_key": "123"}

    with patch.object(gen3_file, 'async_download_multiple') as mock_async:
        mock_async.return_value = {"succeeded": ["test-guid"], "failed": [], "skipped": []}

        gen3_file.download_single(guid="test-guid", skip_completed=skip_completed, rename=rename)

        call_args = mock_async.call_args[1]
        assert call_args["skip_completed"] == skip_completed
        assert call_args["rename"] == rename
        assert call_args["no_progress"]
