import os
import glob
import sys
import shutil
import logging
from unittest.mock import MagicMock, patch
import pytest
import requests
from requests.exceptions import HTTPError

from gen3 import access
from gen3.access import UserManagementTool
from gen3.utils import split_url_and_query_params

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


@patch("gen3.access.requests.get")
def test_get_users(requests_mock):
    """
    Test get_users
    """
    access = UserManagementTool("https://example.com", auth_provider=None)

    def _mock_request(url, **kwargs):
        assert url.endswith("/users")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = [
            {
                "google_email": "test11@example.com",
                "organization": "test",
                "name": "Test User 11",
                "orcid": "N/A",
                "expiration": "2025-11-24",
                "datasets": ["phs000422.c1"],
                "username": "test11@example.com",
                "eracommons": "N/A",
                "contact_email": "test11@example.com",
            },
            {
                "google_email": "N/A",
                "organization": "test",
                "name": "testERA0",
                "orcid": "N/A",
                "expiration": "2000-01-01",
                "datasets": [
                    "phs001252.c1",
                    "phs000810.c2",
                    "phs000810.c1",
                ],
                "username": "TESTERA0",
                "eracommons": "TESTERA0",
                "contact_email": "N/A",
            },
            {
                "google_email": "foobar@example.com",
                "organization": "test",
                "name": "foobar@example.com",
                "orcid": "N/A",
                "expiration": "2000-01-01",
                "datasets": [
                    "phs001252.c1",
                    "phs000810.c2",
                    "phs000810.c1",
                ],
                "username": "foobar@example.com",
                "eracommons": "N/A",
                "contact_email": "N/A",
            },
        ]
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = access.get_users()

    assert response
