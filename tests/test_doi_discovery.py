import os
import requests
from requests.auth import HTTPBasicAuth
import pytest
from unittest.mock import MagicMock, patch

from gen3.auth import Gen3Auth
from gen3.discovery_dois import (
    DOI_DISCOVERY_METADATA_EXPORT_LIMIT,
    GetMetadataInterface,
    get_alternate_id_to_guid_mapping,
    mint_dois_for_discovery_datasets,
)
from gen3.utils import get_random_alphanumeric
from gen3.doi import DigitalObjectIdentifier

PREFIX = "10.12345"
PUBLISHER = "Example"
COMMONS_DISCOVERY_PAGE = "https://example.com/discovery"

METADATA_FIELD_FOR_ALTERNATE_ID = "dbgap_accession"
DOI_DISCLAIMER = "DOI_DISCLAIMER"
DOI_ACCESS_INFORMATION = "DOI_ACCESS_INFORMATION"
DOI_ACCESS_INFORMATION_LINK = "DOI_ACCESS_INFORMATION_LINK"
DOI_CONTACT = "DOI_CONTACT"


@patch("gen3.discovery_dois.output_expanded_discovery_metadata")
def test_get_alternate_id_to_guid_mapping_uses_increased_export_limit(
    mock_output_expanded_discovery_metadata, gen3_auth, tmp_path
):
    """
    Test to ensure DOI mapping requests an export with DOI_DISCOVERY_METADATA_EXPORT_LIMIT.
    """
    output_filename = tmp_path / "discovery_metadata.tsv"
    output_filename.write_text(
        "guid\tdbgap_accession\tdoi_identifier\n"
        "guid_one\tphs004546.v1.p1.c1\t10.12345/EXISTING-ONE\n"
        "guid_two\tphs000007.v1.p1.c1\t\n",
        encoding="utf-8",
    )
    mock_output_expanded_discovery_metadata.return_value = str(output_filename)

    alternate_id_to_guid, all_discovery_metadata = get_alternate_id_to_guid_mapping(
        auth=gen3_auth,
        metadata_field_for_alternate_id=METADATA_FIELD_FOR_ALTERNATE_ID,
    )

    assert alternate_id_to_guid == {
        "phs004546.v1.p1.c1": "guid_one",
        "phs000007.v1.p1.c1": "guid_two",
    }
    assert all_discovery_metadata["guid_one"] == {
        "dbgap_accession": "phs004546.v1.p1.c1",
        "doi_identifier": "10.12345/EXISTING-ONE",
        "guid": "guid_one",
    }
    assert all_discovery_metadata["guid_two"] == {
        "dbgap_accession": "phs000007.v1.p1.c1",
        "doi_identifier": "",
        "guid": "guid_two",
    }
    mock_output_expanded_discovery_metadata.assert_called_once_with(
        gen3_auth,
        endpoint=gen3_auth.endpoint,
        limit=DOI_DISCOVERY_METADATA_EXPORT_LIMIT,
    )


@pytest.mark.parametrize("exclude_datasets", [["guid_W"], ["alternate_id_0"]])
@pytest.mark.parametrize("does_datacite_have_dois_minted_already", [True, False])
@patch("gen3.discovery_dois._raise_exception_on_collision")
@patch("gen3.discovery_dois.DataCite.persist_doi_metadata_in_gen3")
@patch("gen3.discovery_dois.get_alternate_id_to_guid_mapping")
@patch("gen3.doi.requests.get")
@patch("gen3.doi.requests.post")
@patch("gen3.doi.requests.put")
@patch("gen3.doi.DigitalObjectIdentifier")
def test_mint_discovery_dois_first_time(
    mock_digital_object_identifier,
    update_doi_requests_mock,
    create_doi_requests_mock,
    get_doi_requests_mock,
    mock_get_alternate_id_to_guid_mapping,
    mock_persist_doi_metadata_in_gen3,
    mock_raise_exception_on_collision,
    does_datacite_have_dois_minted_already,
    gen3_auth,
    exclude_datasets,
):
    """
    Test that the right call to Datacite's API (create/update) happens and
    persist to Gen3 call happens when using the mint Discovery DOIs for
    existing Discovery Metadata. The discovery metadata have alternate ids and
    a mocked external metadata interface.

    There are 4 existing discovery metadata GUIDs:

        guid_W: We will specifically request to exclude this one. tests will use
                the GUID and the alternate ID to exclude
        guid_X: no DOI yet, need one minted
        guid_Y: no DOI yet, need one minted
        guid_Z: Has existing DOI minted and identified in `doi_identifier` in metadata
    """
    mock_get_alternate_id_to_guid_mapping.side_effect = (
        mock_function_get_alternate_id_to_guid_mapping
    )

    mock_raise_exception_on_collision.side_effect = lambda *_, **__: None

    def _mock_request_404(url, **kwargs):
        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 404

        return mocked_response

    def _mock_request_200(url, **kwargs):
        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200

        return mocked_response

    if does_datacite_have_dois_minted_already:
        get_doi_requests_mock.side_effect = _mock_request_200
    else:
        # this 404 means that Datacite responds with not having a DOI minted, so the
        # code should try to create one for the first time
        get_doi_requests_mock.side_effect = _mock_request_404

    mint_dois_for_discovery_datasets(
        gen3_auth=gen3_auth,
        datacite_auth=HTTPBasicAuth(
            "foo",
            "bar",
        ),
        metadata_field_for_alternate_id=METADATA_FIELD_FOR_ALTERNATE_ID,
        get_doi_identifier_function=get_doi_identifier,
        metadata_interface=MockMetadataInterface,
        doi_publisher=PUBLISHER,
        commons_discovery_page=COMMONS_DISCOVERY_PAGE,
        doi_disclaimer=DOI_DISCLAIMER,
        doi_access_information=DOI_ACCESS_INFORMATION,
        doi_access_information_link=DOI_ACCESS_INFORMATION_LINK,
        doi_contact=DOI_CONTACT,
        publish_dois=False,
        datacite_use_prod=False,
        exclude_datasets=exclude_datasets,
    )

    if does_datacite_have_dois_minted_already:
        # in this case, guid_Z has a `doi_identifier` and the Datacite
        # API is mocked to say that it already has that DOI minted, so
        # we need to update that one and create 2 new DOIs
        assert update_doi_requests_mock.call_count == 1
        assert create_doi_requests_mock.call_count == 2
    else:
        # in this case, even though guid_Z has a `doi_identifier`, the Datacite
        # API is mocked to say that it doesn't have a DOI minted for that, so
        # we actually need to create 3 new DOIs
        assert update_doi_requests_mock.call_count == 0
        assert create_doi_requests_mock.call_count == 3

    # check that persist is called with the right stuff
    assert mock_persist_doi_metadata_in_gen3.call_count == 3
    assert mock_digital_object_identifier.call_count == 3
    calls = {
        call.kwargs.get("guid"): call.kwargs
        for call in mock_persist_doi_metadata_in_gen3.call_args_list
    }
    assert "guid_X" in calls
    assert "guid_Y" in calls
    assert "guid_Z" in calls

    assert calls["guid_X"]["additional_metadata"] == {
        "disclaimer": "DOI_DISCLAIMER",
        "access_information": "DOI_ACCESS_INFORMATION",
        "access_information_link": "DOI_ACCESS_INFORMATION_LINK",
        "contact": "DOI_CONTACT",
    }
    assert calls["guid_X"]["prefix"] == "doi_"

    assert calls["guid_Y"]["additional_metadata"] == {
        "disclaimer": "DOI_DISCLAIMER",
        "access_information": "DOI_ACCESS_INFORMATION",
        "access_information_link": "DOI_ACCESS_INFORMATION_LINK",
        "contact": "DOI_CONTACT",
    }
    assert calls["guid_Y"]["prefix"] == "doi_"

    assert calls["guid_Z"]["additional_metadata"] == {
        "disclaimer": "DOI_DISCLAIMER",
        "access_information": "DOI_ACCESS_INFORMATION",
        "access_information_link": "DOI_ACCESS_INFORMATION_LINK",
        "contact": "DOI_CONTACT",
    }
    assert calls["guid_Z"]["prefix"] == "doi_"


def mock_function_get_alternate_id_to_guid_mapping(
    auth, metadata_field_for_alternate_id
):
    assert metadata_field_for_alternate_id == METADATA_FIELD_FOR_ALTERNATE_ID
    current_discovery_alternate_id_to_guid = {
        "alternate_id_0": "guid_W",
        "alternate_id_A": "guid_X",
        "alternate_id_B": "guid_Y",
        "alternate_id_C": "guid_Z",
    }
    all_discovery_metadata = {
        "guid_W": {
            METADATA_FIELD_FOR_ALTERNATE_ID: "alternate_id_0",
            "descriptions": "barW",
        },
        "guid_X": {
            METADATA_FIELD_FOR_ALTERNATE_ID: "alternate_id_A",
            "descriptions": "barX",
        },
        "guid_Y": {
            METADATA_FIELD_FOR_ALTERNATE_ID: "alternate_id_B",
            "descriptions": "barY",
        },
        "guid_Z": {
            "doi_identifier": "10.12345/TEST-asdf-jkl9",
            METADATA_FIELD_FOR_ALTERNATE_ID: "alternate_id_C",
            "descriptions": "barZ",
        },
    }
    return (
        current_discovery_alternate_id_to_guid,
        all_discovery_metadata,
    )


class MockMetadataInterface(GetMetadataInterface):
    def get_doi_metadata(self):
        return {
            "alternate_id_A": {"descriptions": "barX"},
            "alternate_id_B": {"descriptions": "barY"},
            "alternate_id_C": {
                "descriptions": "barZ",
            },
        }


def get_doi_identifier():
    return (
        PREFIX
        + "/TEST-"
        + get_random_alphanumeric(4)
        + "-"
        + get_random_alphanumeric(4)
    )
