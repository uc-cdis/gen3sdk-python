"""
Tests gen3.file.Gen3File for calls
"""
from unittest.mock import patch
import json
import pytest
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
        mock_request.get().text = json.dumps({"url": sample_presigned_url})
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
        mock_request.get().text = "Failed"
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
        mock_request.get().text = "Failed"
        res = gen3_file.get_presigned_url(guid="123", protocol=supported_protocol)
        assert res == "Failed"


@pytest.mark.parametrize(
    "guid,status_code,response_text,expected_response",
    [
        ("123", 204, "", ""),
        (None, 500, "Failed to delete data file.", "Failed to delete data file."),
    ],
)
def test_delete_files(gen3_file, guid, status_code, response_text, expected_response):
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
        res = gen3_file.delete_files(guid=guid)
        assert res == expected_response


def test_delete_files_no_refresh_token(gen3_file):
    """
    Delete files for a Gen3File without a refresh token, which should raise an HTTPError

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    """
    gen3_file._auth_provider._refresh_token = None

    with patch("gen3.file.requests.delete", side_effect=HTTPError):
        with pytest.raises(HTTPError):
            res = gen3_file.delete_files(guid="123")
            assert res == "Failed to delete data file."


def test_delete_files_no_api_key(gen3_file):
    """
    Delete files for a Gen3File without an api_key in the refresh token, which should return a 401

    :param gen3.file.Gen3File gen3_file:
        Gen3File object
    """
    gen3_file._auth_provider._refresh_token = {"not_api_key": "123"}

    with patch("gen3.file.requests") as mock_request:
        mock_request.status_code = 401
        mock_request.delete().text = "Failed to delete data file."
        res = gen3_file.delete_files(guid="123")
        assert res == "Failed to delete data file."


def test_delete_files_wrong_api_key(gen3_file):
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
        res = gen3_file.delete_files(guid="123")
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
            {"guid": "guid", "url": "https://fakecontainer/some/path/guid/file.txt"},
        ),
        (
            ["/programs"],
            200,
            201,
            '{ "url": "https://fakecontainer/some/path/file.txt" }',
            {"guid": "guid", "url": "https://fakecontainer/some/path/guid/file.txt"},
        ),
        (
            ["/programs"],
            0,
            201,
            '{ "url": "https://fakecontainer/some/path/file.txt" }',
            {"guid": "guid", "url": "https://fakecontainer/some/path/guid/file.txt"},
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
            # check that the response URL includes the extra guid for the uploaded file
            assert res.get("url") == expected_response["url"].replace(
                f"{expected_response['guid']}/", ""
            )
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
