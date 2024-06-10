import pytest
from unittest.mock import MagicMock, patch
import base64
import os
import requests
import time
import json
import gen3.auth

test_endpoint = "https://localhost"
test_external_endpoint = "https://test-commons.org"
test_key = {
    "api_key": "whatever."
    + base64.urlsafe_b64encode(
        ('{"iss": "%s", "exp": %d }' % (test_endpoint, time.time() + 300)).encode(
            "utf-8"
        )
    ).decode("utf-8")
    + ".whatever"
}

test_access_token = {
    "access_token": "whatever."
    + base64.urlsafe_b64encode(
        ('{"iss": "%s", "exp": %d }' % (test_endpoint, time.time() + 300)).encode(
            "utf-8"
        )
    ).decode("utf-8")
    + ".whatever"
}

test_key_wts = {
    "api_key": "wts."
    + base64.urlsafe_b64encode(
        (
            '{"iss": "%s", "exp": %d }' % (test_external_endpoint, time.time() + 300)
        ).encode("utf-8")
    ).decode("utf-8")
    + ".wts"
}

test_cred_file_name = "testCred.json"


def test_get_wts_endpoint():
    endpoint = gen3.auth.get_wts_endpoint(namespace="frickjack")
    assert endpoint == "http://workspace-token-service.frickjack.svc.cluster.local"


def test_endpoint_from_token():
    endpoint = gen3.auth.endpoint_from_token(test_key["api_key"])
    assert endpoint == test_endpoint


def test_token_cache():
    cache_file = gen3.auth.get_token_cache_file_name("whatever")
    expected = "{}/.cache/gen3/token_cache_85738f8f9a7f1b04b5329c590ebcb9e425925c6d0984089c43a022de4f19c281".format(
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


def test_refresh_access_token_when_no_refresh_token(mock_gen3_auth):
    """
    Ensure access token is not refreshed when it's impossible to do so
    """
    # simulate scenario where access token is provided but refresh/API key is not
    mock_gen3_auth._access_token = "foobar"
    mock_gen3_auth._refresh_token = None

    with patch("gen3.auth.get_access_token_with_key") as mock_access_token:
        # should not call this
        mock_access_token.side_effect = Exception()
        with patch("gen3.auth.decode_token") as mock_decode_token:
            mock_decode_token().return_value = {"aud": "123"}
            with patch("gen3.auth.Gen3Auth._write_to_file") as mock_write_to_file:
                mock_write_to_file().return_value = True
                with patch(
                    "gen3.auth.Gen3Auth.__call__",
                    return_value=MagicMock(
                        headers={
                            "Authorization": f"Bearer {mock_gen3_auth._access_token}"
                        }
                    ),
                ) as mock_call:
                    access_token = mock_gen3_auth.refresh_access_token()
                    assert (
                        "Bearer " + access_token == mock_call().headers["Authorization"]
                    )
                    assert mock_gen3_auth._access_token == "foobar"


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


def test_auth_init_with_both_endpoint_and_idp():
    """
    Test that a Gen3Auth instance CANNOT be initialized with both endpoint and idp
    """
    with pytest.raises(ValueError):
        auth = gen3.auth.Gen3Auth(endpoint="https://caninedc.org", idp="canine-google")


def test_auth_init_with_matching_endpoint_and_refresh_file():
    with open(test_cred_file_name, "w") as f:
        json.dump(test_key, f)

    auth = gen3.auth.Gen3Auth(endpoint=test_endpoint, refresh_file=test_cred_file_name)
    assert auth._use_wts == False
    assert auth.endpoint == test_endpoint
    if os.path.isfile(test_cred_file_name):
        os.remove(test_cred_file_name)


def test_auth_init_with_endpoint_that_matches_multiple_idp():
    """
    Test that a Gen3Auth instance will return error if user
    specified endpoint parameter matches with multiple idps
    """
    wts_token = test_key_wts["api_key"]

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)

        if url.endswith("?idp=test-google"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {"token": wts_token}
        elif url.endswith("/external_oidc/"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {
                "providers": [
                    {
                        "base_url": "https://test-commons.org",
                        "idp": "test-google",
                        "name": "test Google Login",
                        "refresh_token_expiration": None,
                        "urls": [
                            {
                                "name": "test Google Login",
                                "url": "https://test-commons.org/wts/oauth2/authorization_url?idp=test-google",
                            }
                        ],
                    },
                    {
                        "base_url": "https://test-commons.org",
                        "idp": "test-google-2",
                        "name": "test Google Login",
                        "refresh_token_expiration": None,
                        "urls": [
                            {
                                "name": "test Google Login",
                                "url": "https://test-commons.org/wts/oauth2/authorization_url?idp=test-google-2",
                            }
                        ],
                    },
                ]
            }
        elif url.endswith("/access_token"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = test_access_token
        else:
            mocked_response.status_code = 400

        return mocked_response

    with patch("gen3.auth.requests.post") as mock_request_post:
        with patch("gen3.auth.requests.get") as mock_request_get:
            mock_request_post.side_effect = _mock_request
            mock_request_get.side_effect = _mock_request

            with open(test_cred_file_name, "w") as f:
                json.dump(test_key, f)

            with pytest.raises(ValueError):
                auth = gen3.auth.Gen3Auth(
                    endpoint=test_external_endpoint, refresh_file=test_cred_file_name
                )
                # auth object should not initialize successfully
                assert not auth

            if os.path.isfile(test_cred_file_name):
                os.remove(test_cred_file_name)


def test_auth_init_with_endpoint_that_matches_no_idp():
    """
    Test that a Gen3Auth instance will return error if user
    specified endpoint parameter matches with no idp
    """
    wts_token = test_key_wts["api_key"]

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)

        if url.endswith("?idp=test-google"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {"token": wts_token}
        elif url.endswith("/external_oidc/"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {
                "providers": [
                    {
                        "base_url": "https://test-commons.org",
                        "idp": "test-google",
                        "name": "test Google Login",
                        "refresh_token_expiration": None,
                        "urls": [
                            {
                                "name": "test Google Login",
                                "url": "https://test-commons.org/wts/oauth2/authorization_url?idp=test-google",
                            }
                        ],
                    }
                ]
            }
        elif url.endswith("/access_token"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = test_access_token
        else:
            mocked_response.status_code = 400

        return mocked_response

    with patch("gen3.auth.requests.post") as mock_request_post:
        with patch("gen3.auth.requests.get") as mock_request_get:
            mock_request_post.side_effect = _mock_request
            mock_request_get.side_effect = _mock_request

            with open(test_cred_file_name, "w") as f:
                json.dump(test_key, f)

            with pytest.raises(ValueError):
                auth = gen3.auth.Gen3Auth(
                    endpoint="https://doesnt_exist.org",
                    refresh_file=test_cred_file_name,
                )
                # auth object should not initialize successfully
                assert not auth

            if os.path.isfile(test_cred_file_name):
                os.remove(test_cred_file_name)


def test_auth_init_with_non_existent_idp():
    """
    Test that a Gen3Auth instance will return error if user
    specified endpoint parameter matches with no idp
    """
    wts_token = test_key_wts["api_key"]

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)

        if url.endswith("?idp=test-google"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {"token": wts_token}
        elif url.endswith("/external_oidc/"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {
                "providers": [
                    {
                        "base_url": "https://test-commons.org",
                        "idp": "test-google",
                        "name": "test Google Login",
                        "refresh_token_expiration": None,
                        "urls": [
                            {
                                "name": "test Google Login",
                                "url": "https://test-commons.org/wts/oauth2/authorization_url?idp=test-google",
                            }
                        ],
                    }
                ]
            }
        elif url.endswith("/access_token"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = test_access_token
        else:
            mocked_response.status_code = 400

        return mocked_response

    with patch("gen3.auth.requests.post") as mock_request_post:
        with patch("gen3.auth.requests.get") as mock_request_get:
            mock_request_post.side_effect = _mock_request
            mock_request_get.side_effect = _mock_request

            with open(test_cred_file_name, "w") as f:
                json.dump(test_key, f)

            with pytest.raises(ValueError):
                auth = gen3.auth.Gen3Auth(
                    idp="doesnt-exist", refresh_file=test_cred_file_name
                )
                # auth object should not initialize successfully
                assert not auth

            if os.path.isfile(test_cred_file_name):
                os.remove(test_cred_file_name)


def test_auth_init_with_idp_and_external_wts():
    "Test initializing Gen3Auth with mocked external WTS"
    wts_token = test_key_wts["api_key"]

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)

        if url.endswith("?idp=test-google"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {"token": wts_token}
        elif url.endswith("/external_oidc/"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {
                "providers": [
                    {
                        "base_url": "https://test-commons.org",
                        "idp": "test-google",
                        "name": "test Google Login",
                        "refresh_token_expiration": None,
                        "urls": [
                            {
                                "name": "test Google Login",
                                "url": "https://test-commons.org/wts/oauth2/authorization_url?idp=test-google",
                            }
                        ],
                    }
                ]
            }
        elif url.endswith("/access_token"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = test_access_token
        else:
            mocked_response.status_code = 400

        return mocked_response

    with patch("gen3.auth.requests.post") as mock_request_post:
        with patch("gen3.auth.requests.get") as mock_request_get:
            mock_request_post.side_effect = _mock_request
            mock_request_get.side_effect = _mock_request

            with open(test_cred_file_name, "w") as f:
                json.dump(test_key, f)

            auth = gen3.auth.Gen3Auth(
                idp="test-google", refresh_file=test_cred_file_name
            )
            assert auth._use_wts == True
            assert auth.endpoint == test_external_endpoint
            assert auth._access_token == wts_token

            if os.path.isfile(test_cred_file_name):
                os.remove(test_cred_file_name)


def test_auth_init_with_endpoint_and_external_wts():
    "Test initializing Gen3Auth with mocked external WTS"
    wts_token = test_key_wts["api_key"]

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)

        if url.endswith("?idp=test-google"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {"token": wts_token}
        elif url.endswith("/external_oidc/"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = {
                "providers": [
                    {
                        "base_url": "https://test-commons.org",
                        "idp": "test-google",
                        "name": "test Google Login",
                        "refresh_token_expiration": None,
                        "urls": [
                            {
                                "name": "test Google Login",
                                "url": "https://test-commons.org/wts/oauth2/authorization_url?idp=test-google",
                            }
                        ],
                    }
                ]
            }
        elif url.endswith("/access_token"):
            mocked_response.status_code = 200
            mocked_response.json.return_value = test_access_token
        else:
            mocked_response.status_code = 400

        return mocked_response

    with patch("gen3.auth.requests.post") as mock_request_post:
        with patch("gen3.auth.requests.get") as mock_request_get:
            mock_request_post.side_effect = _mock_request
            mock_request_get.side_effect = _mock_request

            with open(test_cred_file_name, "w") as f:
                json.dump(test_key, f)

            auth = gen3.auth.Gen3Auth(
                endpoint=test_external_endpoint, refresh_file=test_cred_file_name
            )
            assert auth._use_wts == True
            assert auth.endpoint == test_external_endpoint
            assert auth._access_token == wts_token

            if os.path.isfile(test_cred_file_name):
                os.remove(test_cred_file_name)


def test_auth_init_with_client_credentials():
    """
    Test that a Gen3Auth instance can be initialized with client credentials and an endpoint.
    """
    client_id = "id"
    client_secret = "secret"

    def _mock_request(url, **kwargs):
        mocked_response = MagicMock(requests.Response)
        if "/user/oauth2/token" in url:
            mocked_response.status_code = 200
            mocked_response.json.return_value = test_access_token
        return mocked_response

    with patch("gen3.auth.requests.post") as mock_request_post:
        mock_request_post.side_effect = _mock_request
        auth = gen3.auth.Gen3Auth(
            endpoint=test_endpoint, client_credentials=(client_id, client_secret)
        )

    assert auth.endpoint == test_endpoint
    assert auth._client_credentials == (client_id, client_secret)
    assert auth._client_scopes == "user data openid"
    assert auth._use_wts == False
    assert auth._access_token == test_access_token["access_token"]


def test_auth_init_with_client_credentials_no_endpoint():
    """
    Test that a Gen3Auth instance CANNOT be initialized with client credentials and NO endpoint.
    """
    client_id = "id"
    client_secret = "secret"
    with pytest.raises(ValueError, match="'endpoint' must be specified"):
        gen3.auth.Gen3Auth(client_credentials=(client_id, client_secret))
