import pytest
from unittest.mock import patch

from gen3.auth import Gen3Auth


def test_auth_init_outside_workspace():
    """
    Test that a Gen3Auth instance can be initialized when the
    required parameters are included.
    """
    # missing parameters
    with pytest.raises(ValueError):
        Gen3Auth()

    # working initialization
    endpoint = "localhost"
    refresh_token = "my-refresh-token"
    auth = Gen3Auth(endpoint=endpoint, refresh_token=refresh_token)
    assert auth._endpoint == endpoint
    assert auth._refresh_token == refresh_token
    assert auth._use_wts == False


def test_auth_init_in_workspace(monkeypatch):
    """
    Test that a Gen3Auth instance can be initialized with no parameters
    when working inside a workspace ("NAMESPACE" environment variable),
    if the workspace-token-service is available.
    """
    monkeypatch.setenv("NAMESPACE", "sdk-tests")

    with patch("gen3.auth.requests") as mock_request:
        # unable to communicate with the WTS
        mock_request.get().status_code = 403
        with pytest.raises(ValueError):
            Gen3Auth()

        # can communicate with the WTS
        mock_request.get().status_code = 200
        auth = Gen3Auth()
        assert auth._use_wts == True
