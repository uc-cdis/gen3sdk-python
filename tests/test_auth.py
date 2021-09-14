import pytest
from unittest.mock import MagicMock, patch
import base64
import os
import requests
import time

import gen3.auth

test_endpoint = "https://localhost"
test_key = {
    "api_key": "whatever."
    + base64.urlsafe_b64encode(
        ('{"iss": "%s", "exp": %d }' % (test_endpoint, time.time() + 300)).encode(
            "utf-8"
        )
    ).decode("utf-8")
    + ".whatever"
}


def test_get_wts_endpoint():
    endpoint = gen3.auth.get_wts_endpoint(namespace="frickjack")
    assert endpoint == "https://frickjack/wts"


def test_endpoint_from_token():
    endpoint = gen3.auth.endpoint_from_token(test_key["api_key"])
    assert endpoint == test_endpoint


def test_token_cache():
    cache_file = gen3.auth.token_cache_file("whatever")
    expected = "{}/.cache/gen3/token_cache_008c5926ca861023c1d2a36653fd88e2".format(
        os.path.expanduser("~")
    )
    assert cache_file == expected


def test_refresh_access_token(mock_gen3_auth):
    """
    Make sure that access token ends up in header when refresh is called
    """
    with patch("gen3.auth.get_access_token_with_key") as mock_access_token:
        mock_access_token.return_value = "new_access_token"
        with patch("gen3.auth.decode_token") as mock_decode_token:
            mock_decode_token().return_value = {"aud": "123"}
            with patch("gen3.auth.Gen3Auth._write_to_file") as mock_write_to_file:
                mock_write_to_file().return_value = True
                with patch(
                    "gen3.auth.Gen3Auth.__call__",
                    return_value=MagicMock(
                        headers={"Authorization": "Bearer new_access_token"}
                    ),
                ) as mock_call:
                    access_token = mock_gen3_auth.refresh_access_token()
                    assert (
                        "Bearer " + access_token == mock_call().headers["Authorization"]
                    )


def test_refresh_access_token_no_cache_file(mock_gen3_auth):
    """
    Make sure that access token ends up in header when refresh is called after failing to write to cache file
    """
    with patch("gen3.auth.get_access_token_with_key") as mock_access_token:
        mock_access_token.return_value = "new_access_token"
        with patch("gen3.auth.decode_token") as mock_decode_token:
            mock_decode_token().return_value = {"aud": "123"}
            with patch("gen3.auth.Gen3Auth._write_to_file") as mock_write_to_file:
                mock_write_to_file().return_value = False
                with patch(
                    "gen3.auth.Gen3Auth.__call__",
                    return_value=MagicMock(
                        headers={"Authorization": "Bearer new_access_token"}
                    ),
                ) as mock_call:
                    access_token = mock_gen3_auth.refresh_access_token()
                    assert (
                        "Bearer " + access_token == mock_call().headers["Authorization"]
                    )


def test_write_to_file_success(mock_gen3_auth):
    """
    Make sure that you can write content to a file
    """
    with patch("builtins.open", create=True) as mock_open_file:
        mock_open_file.return_value = MagicMock()
        with patch("builtins.open.write") as mock_file_write:
            mock_file_write.return_value = True
            with patch("os.rename") as mock_os_rename:
                mock_os_rename.return_value = True
                result = mock_gen3_auth._write_to_file("some_file", "content")
                assert result == True


def test_write_to_file_permission_error(mock_gen3_auth):
    """
    Check that the file isn't written when there's a PermissionError
    """
    with patch("builtins.open", create=True) as mock_open_file:
        mock_open_file.return_value = MagicMock()
        with patch(
            "builtins.open.write", side_effect=PermissionError
        ) as mock_file_write:
            with pytest.raises(FileNotFoundError):
                result = mock_gen3_auth._write_to_file("some_file", "content")


def test_write_to_file_rename_permission_error(mock_gen3_auth):
    """
    Check that the file isn't written when there's a PermissionError for renaming
    """
    with patch("builtins.open", create=True) as mock_open_file:
        mock_open_file.return_value = MagicMock()
        with patch("builtins.open.write") as mock_file_write:
            mock_file_write.return_value = True
            with patch("os.rename", side_effect=PermissionError) as mock_os_rename:
                with pytest.raises(PermissionError):
                    result = mock_gen3_auth._write_to_file("some_file", "content")


def test_write_to_file_rename_file_not_found_error(mock_gen3_auth):
    """
    Check that the file isn't renamed when there's a FileNotFoundError
    """
    with patch("builtins.open", create=True) as mock_open_file:
        mock_open_file.return_value = MagicMock()
        with patch("builtins.open.write") as mock_file_write:
            mock_file_write.return_value = True
            with patch("os.rename", side_effect=FileNotFoundError) as mock_os_rename:
                with pytest.raises(FileNotFoundError):
                    result = mock_gen3_auth._write_to_file("some_file", "content")


def test_auth_init_outside_workspace():
    """
    Test that a Gen3Auth instance can be initialized when the
    required parameters are included.
    """
    # working initialization
    auth = gen3.auth.Gen3Auth(refresh_token=test_key)
    assert auth.endpoint == test_endpoint
    assert auth._refresh_token == test_key
    assert auth._use_wts == False


def test_auth_init_in_workspace(monkeypatch):
    """
    Test that a Gen3Auth instance can be initialized with no parameters
    when working inside a workspace ("NAMESPACE" environment variable),
    if the workspace-token-service is available.
    """
    monkeypatch.setenv("NAMESPACE", "sdk-tests")

    access_token = test_key["api_key"]

    def _mock_request(url, **kwargs):
        assert url.endswith("/token/")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {"token": access_token}
        return mocked_response

    with patch("gen3.auth.requests") as mock_request:
        # unable to communicate with the WTS
        mock_request.get().status_code = 403
        with pytest.raises(gen3.auth.Gen3AuthError):
            gen3.auth.Gen3Auth(idp="local")

    with patch("gen3.auth.requests.get") as mock_request:
        # can communicate with the WTS
        mock_request.side_effect = _mock_request
        auth = gen3.auth.Gen3Auth(idp="local")
        assert auth._use_wts == True
        assert auth.endpoint == test_endpoint
        assert auth._access_token == access_token
