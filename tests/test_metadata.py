import asyncio
import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests.exceptions import HTTPError

from gen3.metadata import Gen3Metadata
from gen3.tools.metadata import async_ingest_metadata_manifest
from gen3.utils import get_or_create_event_loop_for_thread

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_METADATA = {
    "dbgap": {
        "submitted_sample_id": "NWD222747",
        "biosample_id": "SAMN03976569",
        "dbgap_sample_id": 1733482,
        "sra_sample_id": "SRS1083657",
        "submitted_subject_id": "BUR02266657B",
        "dbgap_subject_id": 1324130,
        "consent_code": 2,
        "consent_short_name": "DS-LD-IRB-COL",
        "sex": "female",
        "body_site": "Peripheral Blood",
        "analyte_type": "DNA",
        "sample_use": ["Seq_DNA_SNP_CNV", "WGS"],
        "repository": "GALAII",
        "dbgap_status": "Loaded",
        "sra_data_details": {
            "status": "public",
            "experiments": "1",
            "runs": "1",
            "bases": "188052444326",
            "size_Gb": "37",
            "experiment_type": "WGS",
            "platform": "ILLUMINA",
            "center": "New York Genome Center",
        },
        "study_accession": "phs000920.v4.p2",
        "study_accession_with_consent": "phs000920.v4.p2.c2",
        "study_with_consent": "phs000920.c2",
        "study_subject_id": "phs000920.v4_BUR02266657B",
    },
    "_guid_type": "metadata_object",
}


@patch("gen3.metadata.requests.get")
def test_is_healthy(requests_mock):
    """
    Test is healthy response
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
    Test is not healthy response
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
    Test getting version
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
    Test getting index key paths
    """
    metadata = Gen3Metadata("https://example.com")
    expected_response = ["abc"]

    def _mock_request(url, **kwargs):
        assert url.endswith("/metadata_index")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.get_index_key_paths()

    assert response == expected_response


@patch("gen3.metadata.requests.get")
def test_get_index_key_paths_error(requests_mock):
    """
    Test getting key paths error
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
    Test creating index key paths
    """
    metadata = Gen3Metadata("https://example.com")
    path = "/blah"
    expected_response = path

    def _mock_request(url, **kwargs):
        assert f"/metadata_index/{path}" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.create_index_key_path(path)

    assert response == expected_response


@patch("gen3.metadata.requests.post")
def test_create_index_key_paths_error(requests_mock):
    """
    Test create index key paths error
    """
    metadata = Gen3Metadata("https://example.com")
    path = "/blah"

    def _mock_request(url, **kwargs):
        assert f"/metadata_index/{path}" in url

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
    Test deleting the index key path
    """
    metadata = Gen3Metadata("https://example.com")
    path = "/blah"
    expected_response = {}

    def _mock_request(url, **kwargs):
        assert f"/metadata_index/{path}" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 204
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.delete_index_key_path(path)

    assert response.status_code == 204


@patch("gen3.metadata.requests.delete")
def test_delete_index_key_paths_error(requests_mock):
    """
    Test deleting the index key path error
    """
    metadata = Gen3Metadata("https://example.com")
    path = "/blah"
    expected_response = {}

    def _mock_request(url, **kwargs):
        assert f"/metadata_index/{path}" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 500
        mocked_response.json.return_value = {"error": "blah"}
        mocked_response.raise_for_status.side_effect = HTTPError("uh oh")

        return mocked_response

    requests_mock.side_effect = _mock_request

    with pytest.raises(Exception):
        response = metadata.delete_index_key_path(path)


@patch("gen3.metadata.requests.get")
def test_query(requests_mock):
    """
    Test querying for guids
    """
    metadata = Gen3Metadata("https://example.com")
    expected_response = ["1cfd6767-7775-4e0d-a4a7-d0fc9db02e1d"]

    def _mock_request(url, **kwargs):
        assert f"/metadata" in url
        assert f"foo.bar=fizzbuzz" in url
        assert f"data=False" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.query("foo.bar=fizzbuzz")

    assert response == expected_response


@patch("gen3.metadata.requests.get")
def test_query_full_metadata(requests_mock):
    """
    Test querying for guids with full data
    """
    metadata = Gen3Metadata("https://example.com")
    expected_response = {
        "1cfd6767-7775-4e0d-a4a7-d0fc9db02e1d": {
            "dbgap": {
                "sex": "male",
                "body_site": "Blood",
                "repository": "TOPMed_WGS_Amish",
                "sample_use": [],
                "analyte_type": "DNA",
                "biosample_id": "SAMN04109653",
                "consent_code": 2,
                "dbgap_status": "Loaded",
                "sra_sample_id": "SRS1305029",
                "dbgap_sample_id": 1784123,
                "study_accession": "phs000956.v3.p1",
                "dbgap_subject_id": 1360617,
                "sra_data_details": {
                    "runs": "1",
                    "bases": "145891962638",
                    "center": "UM-TOPMed",
                    "status": "public",
                    "size_Gb": "24",
                    "platform": "ILLUMINA",
                    "experiments": "1",
                    "experiment_type": "WGS",
                },
                "study_subject_id": "phs000956.v3_DBG00256",
                "consent_short_name": "HMB-IRB-MDS",
                "study_with_consent": "phs000956.c2",
                "submitted_sample_id": "NWD299344",
                "submitted_subject_id": "DBG00256",
                "study_accession_with_consent": "phs000956.v3.p1.c2",
            },
            "_guid_type": "indexed_file_object",
        }
    }

    def _mock_request(url, **kwargs):
        assert f"/metadata" in url
        assert f"foo.bar=fizzbuzz" in url
        assert f"data=True" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.query("foo.bar=fizzbuzz", return_full_metadata=True)

    assert response == expected_response


@patch("gen3.metadata.requests.post")
def test_batch_create(requests_mock):
    """
    Test batch creation
    """
    metadata = Gen3Metadata("https://example.com")
    metadata_list = [
        {"guid": "3c42c819-1dfe-4c3e-8d46-c3ec7eb99bf4", "data": {"foo": "bar"}},
        {"guid": "dfa1a1dc-98f4-46be-ba8f-ae9b42b0ee50", "data": {"foo": "bar"}},
    ]
    expected_response = {
        "created": [
            "3c42c819-1dfe-4c3e-8d46-c3ec7eb99bf4",
            "dfa1a1dc-98f4-46be-ba8f-ae9b42b0ee50",
        ],
        "updated": [],
        "conflict": [],
    }

    def _mock_request(url, **kwargs):
        assert f"/metadata" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.batch_create(metadata_list)

    assert response == expected_response


@pytest.mark.parametrize(
    "aliases",
    [None, [], ["1"], ["1a", "2b", "3c"], ["A!@SJD)(*@#Q&)AS(DF"]],
)
@patch("gen3.metadata.requests.post")
def test_create(requests_mock, aliases):
    """
    Test creating for guids
    """
    metadata = Gen3Metadata("https://example.com")
    guid = "95a41871-244c-48ae-8004-63f4ed1f0291"
    data = {"foo": "bar", "fizz": "buzz", "nested_details": {"key1": "value1"}}
    expected_response = data

    def _mock_request(url, **kwargs):
        assert f"/metadata/{guid}" in url

        if "alias" in url:
            for alias in aliases:
                assert alias in kwargs.get("json").get("aliases")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.create(guid=guid, metadata=data, aliases=aliases)

    assert response == expected_response


@pytest.mark.parametrize(
    "aliases",
    [None, [], ["1"], ["1a", "2b", "3c"], ["A!@SJD)(*@#Q&)AS(DF"]],
)
@patch("gen3.metadata.requests.put")
def test_update(requests_mock, aliases):
    """
    Test updating for guids
    """
    metadata = Gen3Metadata("https://example.com")
    guid = "95a41871-244c-48ae-8004-63f4ed1f0291"
    data = {"foo": "bar", "fizz": "buzz", "nested_details": {"key1": "value1"}}
    expected_response = data

    def _mock_request(url, **kwargs):
        assert f"/metadata/{guid}" in url

        if "alias" in url:
            for alias in aliases:
                assert alias in kwargs.get("json").get("aliases")

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.update(guid=guid, metadata=data, aliases=aliases)

    assert response == expected_response


@patch("gen3.metadata.requests.delete")
def test_delete(requests_mock):
    """
    Test deleting guids
    """
    metadata = Gen3Metadata("https://example.com")
    guid = "95a41871-244c-48ae-8004-63f4ed1f0291"
    expected_response = {}

    def _mock_request(url, **kwargs):
        assert f"/metadata/{guid}" in url

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = expected_response
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    requests_mock.side_effect = _mock_request

    response = metadata.delete(guid=guid)

    assert response == expected_response


@patch("gen3.metadata.requests.get")
@patch("gen3.metadata.requests.post")
@patch("gen3.metadata.requests.put")
@patch("gen3.metadata.requests.delete")
def test_aliases_creation(
    requests_mock_delete,
    requests_mock_put,
    requests_mock_post,
    requests_mock_get,
    gen3_auth,
):
    """
    Test that aliases get created and updated as expected given different inputs
    by mocking the low level function and asserting they're called with the right info.
    This test also holds a "global" for what we expect the current_alias and metadata
    to be, so that the mock GET can return that.
    """
    mds = Gen3Metadata("https://example.com", auth_provider=gen3_auth)
    guid = "dg.4503/0b2f6693-0936-4c94-8dc7-5e8caf3454c8"
    expected_response = {}

    current_aliases = []
    current_metadata = TEST_METADATA

    def _mock_create_request(url, **kwargs):
        assert f"/metadata/{guid}" in url

        nonlocal current_aliases
        nonlocal current_metadata

        if f"/metadata/{guid}/aliases" in url:
            if "merge=True" in url:
                current_aliases = current_aliases + kwargs.get("json", {}).get(
                    "aliases", []
                )
            else:
                current_aliases = kwargs.get("json", {}).get("aliases", [])
        else:
            if "merge=True" in url:
                current_metadata.update(kwargs.get("json", {}))
            else:
                current_metadata = kwargs.get("json", {})

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = current_metadata
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    def _mock_delete_request(url, **kwargs):
        assert f"/metadata/{guid}" in url

        nonlocal current_aliases

        if "aliases" in url:
            alias = url.strip().split("/")[-1]
            if alias == "aliases":
                # this is a request to delete all aliases
                current_aliases = []
            else:
                # delete a single alias
                current_aliases.remove(alias)

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = {}
        mocked_response.raise_for_status.side_effect = lambda *args: None

        return mocked_response

    def _mock_get_request(url, **kwargs):
        assert f"/metadata/{guid}" in url

        nonlocal current_aliases
        nonlocal current_metadata

        mocked_response = MagicMock(requests.Response)
        mocked_response.status_code = 200
        mocked_response.json.return_value = current_metadata
        mocked_response.raise_for_status.side_effect = lambda *args: None

        if f"/metadata/{guid}/aliases" in url:
            mocked_response.json.return_value = {"aliases": current_aliases}

        return mocked_response

    requests_mock_post.side_effect = _mock_create_request
    requests_mock_get.side_effect = _mock_get_request
    requests_mock_delete.side_effect = _mock_delete_request
    requests_mock_put.side_effect = _mock_create_request

    metadata = TEST_METADATA

    # Test creation with aliases
    mds.create(guid, metadata, aliases=["1", "2"])

    response = mds.get_aliases(guid)
    assert response.get("aliases") == sorted(["1", "2"])
    response = mds.get(guid)
    assert response == TEST_METADATA

    # Test update merging with existing aliases
    metadata["foo"] = "bar"
    mds.update(guid, metadata, merge=True, aliases=["3", "4"])

    response = mds.get_aliases(guid)
    assert response.get("aliases") == sorted(["1", "2", "3", "4"])

    response = mds.get(guid)
    assert response.get("foo") == "bar"
    assert response.get("_guid_type") == "metadata_object"

    # Test update NOT merging with existing aliases
    mds.update(guid, {"foo": "asdf"}, merge=False, aliases=["3", "4"])

    response = mds.get_aliases(guid)
    assert response.get("aliases") == sorted(["3", "4"])

    response = mds.get(guid)
    assert response.get("foo") == "asdf"
    assert response.get("_guid_type") != "metadata_object"

    # test deleting an alias
    mds.delete_alias(guid=guid, alias="3")

    response = mds.get_aliases(guid)
    assert response.get("aliases") == ["4"]

    # test deleting aliases
    mds.delete_aliases(guid)

    response = mds.get_aliases(guid)
    assert response.get("aliases") == []
