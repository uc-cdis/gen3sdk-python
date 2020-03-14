import os
import glob
import sys
import shutil
import logging
from unittest.mock import MagicMock, patch
import pytest
import requests
from requests.exceptions import HTTPError

from gen3 import metadata
from gen3.metadata import Gen3Metadata
from gen3.utils import split_url_and_query_params

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


@patch("gen3.metadata.requests.get")
def test_is_healthy(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/_status")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "status": "OK",
            "timestamp": "2020-03-13T15:23:53.765568+00:00",
        }
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.is_healthy()

    assert response


@patch("gen3.metadata.requests.get")
def test_is_not_healthy(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/_status")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.text = "Not Healthy"
        mocked_response.json.return_value = {}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.is_healthy()

    assert not response


@patch("gen3.metadata.requests.get")
def test_get_version(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/version")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.text = "1.2.0"
        mocked_response.json.return_value = {}
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.get_version()

    assert response


@patch("gen3.metadata.requests.get")
def test_get_index_key_paths(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")
    expected_response = {}  # TODO

    def _mock_request(url, **kwargs):
        assert url.endswith("/metadata_index")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.get_index_key_paths()

    assert response == expected_response  # TODO


@patch("gen3.metadata.requests.get")
def test_get_index_key_paths_error(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/metadata_index")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.json.return_value = {"error": "blah"}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")

        return mocked_response

    requests_mock.side_effect = _mock_request

    with pytest.raises(Exception):
        response = metadata.get_index_key_paths()


@patch("gen3.metadata.requests.post")
def test_create_index_key_paths(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")
    expected_response = {}  # TODO
    path = "/blah"  # TODO

    def _mock_request(url, **kwargs):
        assert url.endswith(f"/metadata_index/{path}")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.create_index_key_path(path)

    assert response == expected_response  # TODO


@patch("gen3.metadata.requests.post")
def test_create_index_key_paths_error(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")
    path = "/blah"  # TODO

    def _mock_request(url, **kwargs):
        assert url.endswith(f"/metadata_index/{path}")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.json.return_value = {"error": "blah"}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")

        return mocked_response

    requests_mock.side_effect = _mock_request

    with pytest.raises(Exception):
        response = metadata.create_index_key_path(path)


@patch("gen3.metadata.requests.delete")
def test_delete_index_key_path(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")
    expected_response = {}
    path = "/blah"  # TODO

    def _mock_request(url, **kwargs):
        assert url.endswith(f"/metadata_index/{path}")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.delete_index_key_path(path)

    assert response == expected_response  # TODO


@patch("gen3.metadata.requests.delete")
def test_delete_index_key_paths_error(requests_mock):
    """
    """
    metadata = Gen3Metadata("https://example.com")
    path = "/blah"  # TODO

    def _mock_request(url, **kwargs):
        assert url.endswith(f"/metadata_index/{path}")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.json.return_value = {"error": "blah"}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")

        return mocked_response

    requests_mock.side_effect = _mock_request

    with pytest.raises(Exception):
        response = metadata.delete_index_key_path(path)


# @patch("gen3.metadata.requests.get")
# def test_query(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response


# @patch("gen3.metadata.requests.get")
# def test_query_full_metadata(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response


# @patch("gen3.metadata.requests.get")
# def test_query_limit(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response


# @patch("gen3.metadata.requests.get")
# def test_query_offset(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response


# @patch("gen3.metadata.requests.get")
# def test_get(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response


# @patch("gen3.metadata.requests.get")
# def test_batch_create(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response


# @patch("gen3.metadata.requests.get")
# def test_create(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response


# @patch("gen3.metadata.requests.get")
# def test_update(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response


# @patch("gen3.metadata.requests.get")
# def test_delete(requests_mock):
#     """
#     """
#     metadata = Gen3Metadata("https://example.com")


#     def _mock_request(url, **kwargs):
#         output = {}
#         if True:
#             output = {"blah": "blah"}
#         return output

#     requests_mock.side_effect = _mock_request

#     response = metadata.get()

#     assert response
