import os
import glob
import sys
import shutil
import logging
from unittest.mock import MagicMock, patch
import pytest
import requests
from requests.exceptions import HTTPError

from gen3.jobs import Gen3Jobs, DBGAP_METADATA_JOB
from gen3.utils import split_url_and_query_params

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


@patch("gen3.jobs.requests.get")
@patch("gen3.jobs.requests.post")
def test_full_job_flow(requests_post_mock, requests_get_mock):
    """
    Test whole flow of creating a job, polling status, and getting output
    """
    jobs = Gen3Jobs("https://example.com")
    job_input = {
        "phsid_list": "phs000956 phs000920",
        "indexing_manifest_url": "https://example.com/public_indexing_manifest.csv",
        "manifests_mapping_config": {
            "guid_column_name": "guid",
            "row_column_name": "submitted_sample_id",
            "smaller_file_column_name": "urls",
        },
        "partial_match_or_exact_match": "partial_match",
    }

    def _mock_create_request(url, **kwargs):
        assert "/dispatch" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "uid": "fcbdb87d-83fe-11ea-a95c-12dda9fc743b",
            "name": "get-dbgap-metadata-zjiio",
            "status": "Unknown",
        }

        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    def _mock_get_status_request(url, **kwargs):
        assert "/status" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "uid": "fcbdb87d-83fe-11ea-a95c-12dda9fc743b",
            "name": "get-dbgap-metadata-zjiio",
            "status": "Completed",
        }

        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    def _mock_get_output_request(url, **kwargs):
        assert "/output" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {"output": "foobar"}

        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_post_mock.side_effect = _mock_create_request

    create_job = jobs.create_job(job_name=DBGAP_METADATA_JOB, job_input=job_input)
    assert create_job.get("uid") == "fcbdb87d-83fe-11ea-a95c-12dda9fc743b"
    assert DBGAP_METADATA_JOB in create_job.get("name")
    assert create_job.get("status")

    requests_get_mock.side_effect = _mock_get_status_request

    status = "Running"
    while status == "Running":
        status = jobs.get_status(create_job.get("uid")).get("status")

    assert status == "Completed"

    requests_get_mock.side_effect = _mock_get_output_request

    get_output = jobs.get_output(create_job.get("uid"))
    assert get_output.get("output") == "foobar"


@patch("gen3.jobs.requests.get")
def test_is_healthy(requests_mock):
    """
    Test is healthy response
    """
    jobs = Gen3Jobs("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/_status")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.text = "Healthy"
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = jobs.is_healthy()

    assert response


@patch("gen3.jobs.requests.get")
def test_is_not_healthy(requests_mock):
    """
    Test is not healthy response
    """
    jobs = Gen3Jobs("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("/_status")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.text = "Not Healthy"
        mocked_response.json.return_value = {}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = jobs.is_healthy()

    assert not response


@patch("gen3.jobs.requests.get")
def test_get_version(requests_mock):
    """
    Test getting version
    """
    jobs = Gen3Jobs("https://example.com")

    def _mock_request(url, **kwargs):
        assert url.endswith("version")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "commit": "bf5df61e6cb031adb9914704f04b71c57d44747a",
            "version": "2020.02-1-gbf5df61",
        }
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = jobs.get_version()

    assert response == "2020.02-1-gbf5df61"
