"""
Tests gen3.doi
"""
import json
import os
import pytest
import sys

import requests
from requests.auth import HTTPBasicAuth
from unittest.mock import MagicMock, patch

from gen3.doi import DataCite, DigitalObjectIdentifer, DataCiteDOIValidationError


@pytest.mark.parametrize(
    "expect_failure,doi_type,prefix,identifier,creators,title,publisher,publication_year,description",
    [
        # everything normal
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            "test",
            "test",
            2000,
            "this is a test description",
        ),
        # providing prefix but not ID
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            "test",
            "test",
            2000,
            "this is a test description",
        ),
        # providing ID but not prefix
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            "test",
            "test",
            2000,
            "this is a test description",
        ),
        # using non valid DOI Type
        (
            True,
            "Not a valid doi type",
            "10.12345",
            "ID1234",
            ["test"],
            "test",
            "test",
            2000,
            "this is a test description",
        ),
        # not providing prefix or ID
        (
            True,
            "Dataset",
            None,
            None,
            ["test"],
            "test",
            "test",
            2000,
            "this is a test description",
        ),
        # special chars and spaces in creators
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test", "!@#$% ^&*|\\O({@[< >?;:]})", "Name. MIDDLE Last"],
            "test",
            "test",
            2000,
            "this is a test description",
        ),
        # special chars and spaces in title
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            "Name. !@#$% ^&*|\\O({@[< >?;:]}) Last",
            "test",
            2000,
            "this is a test description",
        ),
        # special chars and spaces in publisher
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            "test",
            "Some Title with Numbers 123 and Symbols !@#$%^&*|\\O({@[<>?;:]})",
            2000,
            "this is a test description",
        ),
        # large publication_year
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            "test",
            "!@#$%^&*|\\O({@[<>?;:]})",
            5000,
            "this is a test description",
        ),
        # special chars and spaces in description
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            "test",
            "test",
            2000,
            "this is a test description !@#$%^&*|\\O({@[<>?;:]})",
        ),
    ],
)
@patch("gen3.doi.requests.post")
def test_create_doi(
    requests_mock,
    expect_failure,
    prefix,
    identifier,
    doi_type,
    creators,
    title,
    publisher,
    publication_year,
    description,
):
    """
    Test DOI creation under various inputs, ensure it still works even when
    special characters are used. Ensure it doesn't work when invalid input is
    provided (like not specify prefix OR id).
    """
    datacite = DataCite(
        auth_provider=HTTPBasicAuth("DATACITE_USERNAME", "DATACITE_PASSWORD")
    )
    if expect_failure:
        with pytest.raises(DataCiteDOIValidationError):
            doi = DigitalObjectIdentifer(
                prefix=prefix,
                identifier=identifier,
                creators=creators,
                title=title,
                publisher=publisher,
                publication_year=publication_year,
                doi_type=doi_type,
                url="https://example.com",
                version="0.1",
                description=description,
                foobar="test",
            )
    else:
        doi = DigitalObjectIdentifer(
            prefix=prefix,
            identifier=identifier,
            creators=creators,
            title=title,
            publisher=publisher,
            publication_year=publication_year,
            doi_type=doi_type,
            url="https://example.com",
            version="0.1",
            description=description,
            foobar="test",
        )

    def _mock_request(url, **kwargs):
        assert url.endswith("/dois")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "status": "OK",
        }
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    if not expect_failure:
        response = datacite.create_doi(doi)
        response.raise_for_status()
        assert requests_mock.call_count == 1

        data_in_request = (
            requests_mock.call_args.kwargs.get("json", {})
            .get("data", {})
            .get("attributes", {})
        )
        print(requests_mock.call_args.kwargs.get("json", {}).get("data", {}))
        print(data_in_request)

        for creator in creators:
            assert creator in [
                item.get("name") for item in data_in_request.get("creators")
            ]
        assert title in [item.get("name") for item in data_in_request.get("titles")]
        assert data_in_request.get("publisher") == publisher
        assert data_in_request.get("publicationYear") == publication_year
        assert data_in_request.get("types", {}).get("resourceTypeGeneral") == doi_type
        assert description in [
            item.get("description")
            for item in data_in_request.get("descriptions", {"description": "INVALID"})
        ]
