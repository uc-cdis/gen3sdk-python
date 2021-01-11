import pytest
from unittest.mock import MagicMock, patch
import base64
import os
import requests
import time

import gen3.auth
from gen3.auth import Gen3Auth

test_endpoint = "https://localhost"
test_key = {
    "api_key": "whatever." + 
        base64.urlsafe_b64encode(
            ('{"iss": "%s", "exp": %d }' % (test_endpoint, time.time() + 300)).encode('utf-8')
            ).decode('utf-8') + 
        ".whatever"
}

def test_get_wts_endpoint():
    endpoint = gen3.auth.get_wts_endpoint(namespace="frickjack")
    assert endpoint == "http://workspace-token-service.frickjack.svc.cluster.local"

def test_endpoint_from_token():
    endpoint = gen3.auth.endpoint_from_token(test_key["api_key"])
    assert endpoint == test_endpoint

def test_token_cache():
    cache_file = gen3.auth.token_cache_file("whatever")
    expected = "{}/.cache/gen3/token_cache_008c5926ca861023c1d2a36653fd88e2".format(os.path.expanduser("~"))
    assert cache_file == expected

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
        mocked_response.json.return_value = {
            "token": access_token
        }
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