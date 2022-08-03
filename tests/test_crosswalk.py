"""
These tests are heavily based off the docs/crosswalk.md example.
See the test data in /tests/test_data/crosswalk
"""
import asyncio
import os
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests.exceptions import HTTPError

from gen3.metadata import Gen3Metadata
from gen3.tools.metadata.crosswalk import (
    CROSSWALK_NAMESPACE,
    GUID_TYPE,
    publish_crosswalk_metadata,
)
from gen3.utils import get_or_create_event_loop_for_thread

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


@patch("gen3.metadata.requests.post")
def foooooo_test_create(requests_mock):
    """"""
    metadata = Gen3Metadata("https://example.com")
    guid = "95a41871-244c-48ae-8004-63f4ed1f0291"
    data = {"foo": "bar", "fizz": "buzz", "nested_details": {"key1": "value1"}}
    expected_response = data

    def _mock_request(url, **kwargs):
        assert f"/metadata/{guid}" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.create(guid=guid, metadata=data)

    assert response == expected_response


@patch("gen3.metadata.Gen3Metadata.async_update")
@patch("gen3.index.Gen3Index.get_valid_guids")
@patch("gen3.metadata.Gen3Metadata.async_get")
def test_publish(
    get_metadata_patch, get_valid_guids_patch, update_metadata_patch, gen3_auth
):
    """
    Test that publishing the examples from docs/crosswalk.md results in the
    expected calls to the MDS. Ensure that merging of existing crosswalk data with new
    data results in the expected outcome.
    """
    guid_1 = "11111111-aac4-11ed-861d-0242ac120002"
    guid_2 = "22222222-aac4-11ed-861d-0242ac120002"
    guid_3 = "33333333-aac4-11ed-861d-0242ac120002"
    guid_4 = "44444444-aac4-11ed-861d-0242ac120002"

    expected_crosswalk_data_1 = {
        "subject": {
            "https://gen3.biodatacatalyst.nhlbi.nih.gov": {
                "Subject.submitter_id": {
                    "value": "phs002363.v1_RC-1358",
                    "type": "gen3_node_property",
                    "description": "These identifiers are constructed as part of the data ingestion process in BDCat and concatenate the study and version with the study-provided subject ID (with a _ delimiting).",
                }
            },
            "https://data.midrc.org": {
                "Case.submitter_id": {
                    "value": "A01-00888",
                    "type": "gen3_node_property",
                    "description": "The uniquely assigned case identifier in MIDRC.",
                },
            },
            "mapping_methodologies": sorted(
                [
                    "NHLBI provided a file of subject IDs for the PETAL study that directly associate a PETAL ID with a BDCat Subject Identifier."
                ]
            ),
        }
    }

    # the second ingestion should maintain the above information as well
    expected_crosswalk_data_2 = {
        "subject": {
            "https://gen3.biodatacatalyst.nhlbi.nih.gov": {
                "Subject.submitter_id": {
                    "value": "phs002363.v1_RC-1358",
                    "type": "gen3_node_property",
                    "description": "These identifiers are constructed as part of the data ingestion process in BDCat and concatenate the study and version with the study-provided subject ID (with a _ delimiting).",
                }
            },
            "https://data.midrc.org": {
                "Case.submitter_id": {
                    "value": "A01-00888",
                    "type": "gen3_node_property",
                    "description": "The uniquely assigned case identifier in MIDRC.",
                },
                "Case.data_submission_guid": {
                    "value": "foobar",
                    "type": "gen3_node_property",
                    "description": "The identifier for this subject as provided by the site’s submission of Datavant tokens to MIDRC.",
                },
                "Masked N3C ID": {
                    "value": "123dfj4ia5oi*@a",
                    "type": "masked_n3c_id",
                    "description": "Masked National COVID Consortium ID provided by a Linkage Honest Broker to the MIDRC system.",
                },
            },
            "mapping_methodologies": sorted(
                [
                    "NHLBI provided a file of subject IDs for the PETAL study that directly associate a PETAL ID with a BDCat Subject Identifier.",
                    "A Linkage Honest Broker provided MIDRC with what Masked N3C IDs match MIDRC cases via a system-to-system handoff.",
                ]
            ),
        }
    }

    async def mock_async_update_metadata(guid, metadata, *_, **__):
        assert metadata["_guid_type"] == GUID_TYPE
        crosswalk_metadata = metadata[CROSSWALK_NAMESPACE]

        assert guid in [guid_1, guid_2, guid_3, guid_4]

        if guid == guid_1:
            assert crosswalk_metadata == expected_crosswalk_data_1
        elif guid in [guid_2, guid_3, guid_4]:
            assert crosswalk_metadata == expected_crosswalk_data_2

    get_valid_guids_patch.return_value = [guid_1]
    update_metadata_patch.side_effect = mock_async_update_metadata

    async def mock_async_get_metadata(guid, *_, **__):
        return {
            "_guid_type": GUID_TYPE,
            CROSSWALK_NAMESPACE: expected_crosswalk_data_1,
            "additional_metadata": "foobar",
        }

    get_metadata_patch.side_effect = mock_async_get_metadata

    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        publish_crosswalk_metadata(
            gen3_auth,
            file="tests/test_data/crosswalk/crosswalk_1.csv",
            info_file="tests/test_data/crosswalk/crosswalk_optional_info_1.csv",
            mapping_methodology="NHLBI provided a file of subject IDs for the PETAL study that directly associate a PETAL ID with a BDCat Subject Identifier.",
        )
    )
    assert get_valid_guids_patch.called
    assert update_metadata_patch.called

    # patch return of metadata GET to be the existing information to simulate
    # when existing crosswalk info already exists
    get_valid_guids_patch.return_value = [guid_2, guid_3, guid_4]

    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        publish_crosswalk_metadata(
            gen3_auth,
            file="tests/test_data/crosswalk/crosswalk_2.csv",
            info_file="tests/test_data/crosswalk/crosswalk_optional_info_2.csv",
            mapping_methodology="A Linkage Honest Broker provided MIDRC with what Masked N3C IDs match MIDRC cases via a system-to-system handoff.",
        )
    )
    assert get_valid_guids_patch.called
    assert update_metadata_patch.called


@pytest.mark.parametrize(
    "file,info",
    [
        (
            "tests/test_data/crosswalk/invalid_a_crosswalk_1.csv",
            "tests/test_data/crosswalk/crosswalk_optional_info_1.csv",
        ),
        (
            "tests/test_data/crosswalk/invalid_b_crosswalk_1.csv",
            "tests/test_data/crosswalk/crosswalk_optional_info_1.csv",
        ),
        (
            "tests/test_data/crosswalk/invalid_c_crosswalk_1.csv",
            "tests/test_data/crosswalk/crosswalk_optional_info_1.csv",
        ),
        (
            "tests/test_data/crosswalk/crosswalk_1.csv",
            "tests/test_data/crosswalk/invalid_a_crosswalk_optional_info_1.csv",
        ),
        (
            "tests/test_data/crosswalk/crosswalk_1.csv",
            "tests/test_data/crosswalk/invalid_b_crosswalk_optional_info_1.csv",
        ),
        (
            "tests/test_data/crosswalk/crosswalk_1.csv",
            "tests/test_data/crosswalk/invalid_c_crosswalk_optional_info_1.csv",
        ),
        (
            "tests/test_data/crosswalk/empty_file.csv",
            "tests/test_data/crosswalk/empty_file.csv",
        ),
        (
            "tests/test_data/crosswalk/empty_crosswalk_1.csv",
            "tests/test_data/crosswalk/empty_file.csv",
        ),
    ],
)
@patch("gen3.metadata.Gen3Metadata.async_update")
@patch("gen3.index.Gen3Index.get_valid_guids")
@patch("gen3.metadata.Gen3Metadata.async_get")
def test_publish_invalid_files(
    get_metadata_patch,
    get_valid_guids_patch,
    update_metadata_patch,
    file,
    info,
    gen3_auth,
):
    """
    Test that appropriate errors are raised when the crosswalk file input is invalid
    """
    get_valid_guids_patch.return_value = ["foobar"]

    async def mock_async_update_metadata(guid, metadata, *_, **__):
        # this should never be called
        assert False

    update_metadata_patch.side_effect = mock_async_update_metadata

    async def mock_async_get_metadata(guid, *_, **__):
        return None

    get_metadata_patch.side_effect = mock_async_get_metadata

    with pytest.raises(Exception):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            publish_crosswalk_metadata(
                gen3_auth,
                file=file,
                info_file=info,
                mapping_methodology="",
            )
        )
        assert not update_metadata_patch.called


@pytest.mark.parametrize(
    "file,info",
    [
        (
            "tests/test_data/crosswalk/empty_crosswalk_1.csv",
            "tests/test_data/crosswalk/crosswalk_optional_info_1.csv",
        ),
        (
            "tests/test_data/crosswalk/empty_crosswalk_1.csv",
            "tests/test_data/crosswalk/empty_crosswalk_optional_info_1.csv",
        ),
    ],
)
@patch("gen3.metadata.Gen3Metadata.async_update")
@patch("gen3.index.Gen3Index.get_valid_guids")
@patch("gen3.metadata.Gen3Metadata.async_get")
def test_publish_no_op(
    get_metadata_patch,
    get_valid_guids_patch,
    update_metadata_patch,
    file,
    info,
    gen3_auth,
):
    """
    Test that when the crosswalk file is empty, nothing happens - regardless of if
    there's valid info passed. No exception should be raised because everything is
    technically valid, but since there's nothing in the crosswalk file we should not
    every call MDS update.
    """
    get_valid_guids_patch.return_value = ["foobar"]

    async def mock_async_update_metadata(guid, metadata, *_, **__):
        # this should never be called
        assert False

    update_metadata_patch.side_effect = mock_async_update_metadata

    async def mock_async_get_metadata(guid, *_, **__):
        return None

    get_metadata_patch.side_effect = mock_async_get_metadata

    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        publish_crosswalk_metadata(
            gen3_auth,
            file=file,
            info_file=info,
            mapping_methodology="",
        )
    )
    assert not update_metadata_patch.called
