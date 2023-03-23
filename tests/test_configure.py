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
expected_profile_line = f"[{profile}]\n"
creds = {"key_id": "1234", "api_key": "abc"}
new_lines = [
    f"key_id={creds['key_id']}\n",
    f"api_key={creds['api_key']}\n",
    f"api_endpoint={mock_endpoint(None)}\n",
    f"access_key={mock_access_key(None)}\n",
    "use_shepherd=\n",
    "min_shepherd_version=\n",
]

lines_with_profile = [
    f"[{profile}]\n",
    f"key_id=random_key\n",
    f"api_key=random_api_key\n",
    f"api_endpoint=random_endpoint\n",
    f"access_key=random_access_key\n",
    "use_shepherd=random_boolean\n",
    "min_shepherd_version=random_version\n",
]


@patch("gen3.auth.endpoint_from_token", mock_endpoint)
@patch("gen3.auth.get_access_token_with_key", mock_access_key)
def test_get_profile_from_creds(monkeypatch):
    test_file_name = str(uuid.uuid4()) + ".json"
    try:
        profile = "DummyProfile"
        creds = {"key_id": "1234", "api_key": "abc"}
        with open(test_file_name, "w+") as cred_file:
            json.dump(creds, cred_file)

        profile_line, lines = config_tool.get_profile_from_creds(
            profile, test_file_name
        )
    finally:
        if os.path.exists(test_file_name):
            os.remove(test_file_name)

    assert profile_line == expected_profile_line
    for line, new_line in zip(lines, new_lines):
        assert line == new_line


@pytest.mark.parametrize("test_lines", [[], lines_with_profile])
def test_update_config_lines(test_lines, monkeypatch):
    file_name = str(uuid.uuid4())
    monkeypatch.setattr(config_tool, "CONFIG_FILE_PATH", file_name)
    try:
        config_tool.update_config_lines(test_lines, expected_profile_line, new_lines)
        with open(file_name, "r") as f:
            assert f.readlines() == [expected_profile_line] + new_lines
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
