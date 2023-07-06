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

import gen3
from gen3.doi import (
    DataCite,
    DigitalObjectIdentifier,
    DataCiteDOIValidationError,
    DigitalObjectIdentifierCreator,
    DigitalObjectIdentifierTitle,
)


@pytest.mark.parametrize(
    "expect_failure,doi_type,prefix,identifier,creators,titles,publisher,publication_year,descriptions",
    [
        # everything normal
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["test title"],
            "test",
            2000,
            ["this is a test description"],
        ),
        # providing prefix but not ID
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["test title"],
            "test",
            2000,
            ["this is a test description"],
        ),
        # providing ID but not prefix
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["test title"],
            "test",
            2000,
            ["this is a test description"],
        ),
        # using non valid DOI Type
        (
            True,
            "Not a valid doi type",
            "10.12345",
            "ID1234",
            ["test"],
            ["test title"],
            "test",
            2000,
            ["this is a test description"],
        ),
        # not providing prefix or ID
        (
            True,
            "Dataset",
            None,
            None,
            ["test"],
            ["test title"],
            "test",
            2000,
            ["this is a test description"],
        ),
        # special chars and spaces in creators
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test", "!@#$% ^&*|\\O({@[< >?;:]})", "Name. MIDDLE Last"],
            ["test title"],
            "test",
            2000,
            ["this is a test description"],
        ),
        # special chars and spaces in titles
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["!@#$% ^&*|\\O({@[< >?;:]})"],
            "test",
            2000,
            ["this is a test description"],
        ),
        # special chars and spaces in publisher
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["test"],
            "Some publisher with Numbers 123 and Symbols !@#$%^&*|\\O({@[<>?;:]})",
            2000,
            ["this is a test description"],
        ),
        # large publication_year
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["test title"],
            "!@#$%^&*|\\O({@[<>?;:]})",
            5000,
            ["this is a test description"],
        ),
        # special chars and spaces in description
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["test title"],
            "test",
            2000,
            ["this is a test description !@#$%^&*|\\O({@[<>?;:]})"],
        ),
        # multiple creators
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test", "test2", "test3"],
            ["test title"],
            "test",
            2000,
            ["this is a test description !@#$%^&*|\\O({@[<>?;:]})"],
        ),
        # multiple titles
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["test title", "test title2", "test title 3"],
            "test",
            2000,
            ["this is a test description !@#$%^&*|\\O({@[<>?;:]})"],
        ),
        # multiple descriptions
        (
            False,
            "Dataset",
            "10.12345",
            "ID1234",
            ["test"],
            ["test title"],
            "test",
            2000,
            ["test description", "test description2", "test description 3"],
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
    titles,
    publisher,
    publication_year,
    descriptions,
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
            doi = DigitalObjectIdentifier(
                prefix=prefix,
                identifier=identifier,
                creators=creators,
                titles=titles,
                publisher=publisher,
                publication_year=publication_year,
                doi_type=doi_type,
                url="https://example.com",
                version="0.1",
                descriptions=descriptions,
                foobar="test",
            )
    else:
        doi = DigitalObjectIdentifier(
            prefix=prefix,
            identifier=identifier,
            creators=creators,
            titles=titles,
            publisher=publisher,
            publication_year=publication_year,
            doi_type=doi_type,
            url="https://example.com",
            version="0.1",
            descriptions=descriptions,
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

        for creator in creators:
            assert creator in data_in_request.get("creators")
        for title in titles:
            assert title in data_in_request.get("titles")
        assert data_in_request.get("publisher") == publisher
        assert data_in_request.get("publicationYear") == publication_year
        assert data_in_request.get("types", {}).get("resourceTypeGeneral") == doi_type
        for description in descriptions:
            assert description in data_in_request.get("descriptions")


@pytest.mark.parametrize(
    "doi_metadata_already_exists, existing_metadata",
    [
        # non Discovery, top-level metadata
        (True, {"alreadyexists": "stuff"}),
        # Discovery, non DOI metadata
        (
            True,
            {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": {"otherstuff": "foobar"},
            },
        ),
        # Discovery, DOI metadata
        (
            True,
            {
                "_guid_type": "discovery_metadata",
                "gen3_discovery": {"doi_version": 0},
            },
        ),
        (False, {}),
    ],
)
@patch("gen3.doi.requests.get")
@patch("gen3.doi.requests.post")
@patch("gen3.doi.requests.put")
def test_doi_metadata_persist(
    doi_requests_put_mock,
    doi_requests_post_mock,
    doi_requests_get_mock,
    gen3_auth,
    doi_metadata_already_exists,
    existing_metadata,
):
    """
    Test the DOI metadata persistance into Gen3's metadata service with and
    without existing metadata existing.
    """
    # Setup
    datacite = DataCite(
        api=DataCite.TEST_URL,
        auth_provider=HTTPBasicAuth(
            os.environ.get("DATACITE_USERNAME"),
            os.environ.get("DATACITE_PASSWORD"),
        ),
    )

    gen3_metadata_guid = "Example-Study-01"

    # Get DOI metadata (ideally from some external source)
    identifier = "10.12345/268Z-O151"
    creators = [
        DigitalObjectIdentifierCreator(
            name="Bar, Foo",
            name_type=DigitalObjectIdentifierCreator.NAME_TYPE_PERSON,
        ).as_dict()
    ]
    titles = [DigitalObjectIdentifierTitle("Some Example Study in Gen3").as_dict()]
    publisher = "Example Gen3 Sponsor"
    publication_year = 2023
    doi_type = "Dataset"
    version = 1

    doi_metadata = {
        "identifier": identifier,
        "creators": creators,
        "titles": titles,
        "publisher": publisher,
        "publication_year": publication_year,
        "doi_type": doi_type,
        "version": version,
    }

    # Create/Mint the DOI in DataCite
    doi = DigitalObjectIdentifier(root_url="foobar", **doi_metadata)

    def _mock_metadata_create_request(url, **kwargs):
        assert f"/metadata/{gen3_metadata_guid}" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {
            "status": "OK",
        }
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    def _mock_metadata_get_request(url, **kwargs):
        assert f"/metadata/{gen3_metadata_guid}" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200

        nonlocal existing_metadata
        mocked_response.json.return_value = existing_metadata

        mocked_response.raise_for_status.side_effect = lambda *args: None

        if f"/metadata/{gen3_metadata_guid}/aliases" in url:
            mocked_response.json.return_value = {"aliases": []}

        return mocked_response

    def _mock_metadata_get_request_doesnt_exist(url, **kwargs):
        assert f"/metadata/{gen3_metadata_guid}" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 404
        mocked_response.json.return_value = {}

        def _raise():
            error = requests.exceptions.HTTPError(mocked_response)
            error.status_code = 404
            raise error

        mocked_response.raise_for_status.side_effect = _raise

        if f"/metadata/{gen3_metadata_guid}/aliases" in url:
            mocked_response.json.return_value = {"aliases": []}

        return mocked_response

    doi_requests_put_mock.side_effect = _mock_metadata_create_request
    doi_requests_post_mock.side_effect = _mock_metadata_create_request

    if doi_metadata_already_exists:
        doi_requests_get_mock.side_effect = _mock_metadata_get_request
    else:
        doi_requests_get_mock.side_effect = _mock_metadata_get_request_doesnt_exist

    # Persist necessary DOI Metadata in Gen3 Discovery to support the landing page
    metadata = datacite.persist_doi_metadata_in_gen3(
        guid=gen3_metadata_guid,
        doi=doi,
        auth=gen3_auth,
        additional_metadata={
            "disclaimer": "disclaimer 1",
            "access_information": "access_information 1",
            "access_information_link": "access_information_link 1",
            "contact": "contact 1",
        },
        prefix="doi_",
    )
    assert doi_requests_get_mock.call_count == 1

    expected_output = {
        "_guid_type": "discovery_metadata",
        "gen3_discovery": {
            "doi_identifier": identifier,
            "doi_resolveable_link": f"https://doi.org/{identifier}",
            "doi_creators": "Bar, Foo",
            "doi_titles": "Some Example Study in Gen3",
            "doi_publisher": "Example Gen3 Sponsor",
            "doi_publication_year": 2023,
            "doi_resource_type": "Dataset",
            "doi_url": f"foobar/{identifier}/",
            "doi_version": 1,
            "doi_is_available": "Yes",
            "doi_disclaimer": "disclaimer 1",
            "doi_citation": f"Bar, Foo (2023). Some Example Study in Gen3 1. Example Gen3 Sponsor. Dataset. {identifier}",
            "doi_version_information": "This is version 1 of this Dataset.",
            "doi_access_information": "access_information 1",
            "doi_contact": "contact 1",
            "doi_access_information_link": "access_information_link 1",
        },
    }

    if doi_metadata_already_exists:
        # metadata created should prefer the new DOI information over existing
        expected_updated_output = existing_metadata
        expected_updated_output.update(expected_output)
        assert doi_requests_put_mock.call_count == 2

        metadata_call = doi_requests_put_mock.call_args_list[0]
        alias_call = doi_requests_put_mock.call_args_list[1]

        assert metadata_call.kwargs.get("json") == expected_updated_output
        assert f"{identifier}" in alias_call.kwargs.get("json", {}).get("aliases", [])
    else:
        # 1 for metadata, 1 for alias
        assert doi_requests_post_mock.call_count == 2

        metadata_call = doi_requests_post_mock.call_args_list[0]
        alias_call = doi_requests_post_mock.call_args_list[1]

        assert metadata_call.kwargs.get("json") == expected_output
        assert f"{identifier}" in alias_call.kwargs.get("json", {}).get("aliases", [])
