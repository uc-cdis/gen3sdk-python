import pytest
import uuid
from unittest.mock import patch
import json
import os
import gen3.configure as config_tool


def mock_endpoint(_):
    return "https://mock.planx-pla.net"


def mock_access_key(_):
    return "mock_access_key"


profile = "DummyProfile"
creds = {"key_id": "1234", "api_key": "abc"}
expected_credentials = {
    "key_id": creds['key_id'],
    "api_key": creds['api_key'],
    "api_endpoint": mock_endpoint(None),
    "access_token": mock_access_key(None),
}



@patch("gen3.auth.endpoint_from_token", mock_endpoint)
@patch("gen3.auth.get_access_token_with_key", mock_access_key)
def test_get_profile_from_creds(monkeypatch):
    test_file_name = str(uuid.uuid4()) + ".json"
    try:
        profile = "DummyProfile"
        creds = {"key_id": "1234", "api_key": "abc"}
        with open(test_file_name, "w+") as cred_file:
            json.dump(creds, cred_file)

        profile_name, credentials = config_tool.get_profile_from_creds(
            profile, test_file_name
        )
    finally:
        if os.path.exists(test_file_name):
            os.remove(test_file_name)

    assert profile_name == profile
    assert credentials == expected_credentials


def test_update_config_profile(monkeypatch):
    file_name = str(uuid.uuid4())
    monkeypatch.setattr(config_tool, "CONFIG_FILE_PATH", file_name)
    try:
        config_tool.update_config_profile(profile, expected_credentials)

        # Verify the config was written correctly using configparser
        import configparser
        config = configparser.ConfigParser()
        config.read(file_name)

        assert profile in config
        assert config[profile]['key_id'] == expected_credentials['key_id']
        assert config[profile]['api_key'] == expected_credentials['api_key']
        assert config[profile]['api_endpoint'] == expected_credentials['api_endpoint']
        assert config[profile]['access_token'] == expected_credentials['access_token']
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
