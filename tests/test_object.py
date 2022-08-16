"""
Tests gen3.object.Gen3Object for calls
"""
from unittest.mock import MagicMock, patch
import requests
from httpx import delete
import pytest
from requests import HTTPError


@patch("gen3.object.requests.post")
def test_create_object_error(requests_mock, gen3_object):
    def _mock_request(url, **kwargs):
        assert url.endswith("/mds/objects")
        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.json.return_value = {"error": "blah"}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")
        return mocked_response

    requests_mock.side_effect = _mock_request
    with pytest.raises(HTTPError):
        gen3_object.create_object("abc.txt", authz=None)


@patch("gen3.object.requests.post")
def test_create_object_success(requests_mock, gen3_object):
    mock_guid = "abcd"
    mock_url = "https://example.com"

    def _mock_request(url, **kwargs):
        assert url.endswith("/mds/objects")
        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {"guid": mock_guid, "upload_url": mock_url}
        return mocked_response

    requests_mock.side_effect = _mock_request
    response_guid, response_upload_url = gen3_object.create_object(
        "abc.txt", authz=None
    )
    assert response_guid == mock_guid
    assert response_upload_url == mock_url


@patch("gen3.object.requests.delete")
def test_delete_object_error(requests_mock, gen3_object):
    mock_guid = "1234"

    def _mock_request(url, **kwargs):
        assert url.endswith(f"/mds/objects/{mock_guid}")
        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.json.return_value = {"error": "blah"}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")
        return mocked_response

    requests_mock.side_effect = _mock_request
    with pytest.raises(HTTPError):
        gen3_object.delete_object(mock_guid)


@pytest.mark.parametrize(
    "delete_file_locations",
    [True, False],
)
@patch("gen3.object.requests.delete")
def test_delete_object_success(requests_mock, gen3_object, delete_file_locations):
    mock_guid = "1234"

    def _mock_request(url, **kwargs):
        mock_url = f"/mds/objects/{mock_guid}"
        assert (
            url.endswith(f"{mock_url}?delete_file_locations")
            if delete_file_locations
            else url.endswith(mock_url)
        )

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {}
        return mocked_response

    requests_mock.side_effect = _mock_request
    gen3_object.delete_object(mock_guid, delete_file_locations)
    assert requests_mock.called
