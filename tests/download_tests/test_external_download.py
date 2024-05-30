import pytest
import json
import requests_mock
from pathlib import Path
from typing import Dict
from unittest import mock

from unittest.mock import MagicMock

from gen3.auth import Gen3AuthError
from gen3.tools.download.drs_download import DownloadStatus, wts_get_token
from gen3.tools.download.external_file_download import (
    check_data_and_group_by_retriever,
    download_files_from_metadata,
    pull_files,
    extract_external_file_metadata,
    is_valid_external_file_metadata,
    load_all_metadata,
)

DIR = Path(__file__).resolve().parent


@pytest.fixture
def valid_external_file_metadata():
    with open(Path(DIR, "resources/gen3_metadata_external_file_metadata.json")) as fin:
        metadata = json.load(fin)
        return metadata["gen3_discovery"]["external_file_metadata"]


def test_load_all_metadata():
    gen3_metadata = load_all_metadata(
        Path(DIR, "resources/gen3_metadata_external_file_metadata.json")
    )
    assert "gen3_discovery" in gen3_metadata
    assert load_all_metadata(Path(DIR, "resources/non_existing_file.json")) is None
    assert load_all_metadata(Path(DIR, "resources/bad_format.json")) is None


def test_is_valid_external_file_metadata_invalid_input():
    expected = False
    # input is missing one of the required keys, 'external_oidc_idp' or 'file_retriever'
    external_file_metadata = {
        "external_oidc_idp": "test-external-idp",
        "file_id": "QDR_file_02",
    }
    assert is_valid_external_file_metadata(external_file_metadata) == expected
    external_file_metadata = {"file_retriever": "QDR", "file_id": "QDR_file_02"}
    assert is_valid_external_file_metadata(external_file_metadata) == expected
    # input is not a dict
    external_file_metadata = ["some", "list"]
    assert is_valid_external_file_metadata(external_file_metadata) == expected


def test_is_valid_external_file_metadata(valid_external_file_metadata):
    for file_metadata in valid_external_file_metadata:
        assert is_valid_external_file_metadata(file_metadata) == True


def test_check_data_and_group_by_retriever():
    file_metadata_list = [
        {
            "external_oidc_idp": "test-external-idp",
            "file_retriever": "QDR",
            "file_id": "QDR_file_02",
        },
        {
            "external_oidc_idp": "test-external-idp",
            "file_retriever": "QDR",
            "file_id": "QDR_file_03",
        },
        {
            "foo": "bar",
            "file_retriever": "QDR",
            "file_id": "QDR_file_03",
        },
        {
            "external_oidc_idp": "other-external-idp",
            "file_retriever": "Other",
            "file_id": "Other_file_02",
        },
    ]
    expected = {
        "QDR": [
            {
                "external_oidc_idp": "test-external-idp",
                "file_retriever": "QDR",
                "file_id": "QDR_file_02",
            },
            {
                "external_oidc_idp": "test-external-idp",
                "file_retriever": "QDR",
                "file_id": "QDR_file_03",
            },
        ],
        "Other": [
            {
                "external_oidc_idp": "other-external-idp",
                "file_retriever": "Other",
                "file_id": "Other_file_02",
            }
        ],
    }
    assert check_data_and_group_by_retriever(file_metadata_list) == expected


def test_extract_external_file_metadata(valid_external_file_metadata):
    # valid metadata
    metadata_file = "resources/gen3_metadata_external_file_metadata.json"
    metadata = load_all_metadata(Path(DIR, metadata_file))
    external_file_metadata = extract_external_file_metadata(metadata)

    assert type(external_file_metadata) is list
    assert len(external_file_metadata) == 2
    assert external_file_metadata == valid_external_file_metadata

    # invalid metadata - missing keys
    metadata_missing_keys = {
        "_guid_type": "discovery_metadata",
        "gen3_discovery": {"authz": ""},
    }
    assert extract_external_file_metadata(metadata_missing_keys) == None
    assert extract_external_file_metadata({"_guid_type": "discovery_metadata"}) == None
    assert extract_external_file_metadata({}) == None


def test_pull_files_bad_retriever_input():
    # retriever does not exist
    wts_hostname = "test.commons1.io"
    download_path = "."
    mock_external_file_metadata = [
        {
            "external_oidc_idp": "test-external-idp",
            "file_retriever": "DoesNotExist",
        }
    ]
    mock_auth = MagicMock()

    mock_retriever = MagicMock()
    mock_retriever.__name__ = "mock_retriever"
    mock_retriever.return_value = "some_status"
    retrievers = {"QDR": mock_retriever}

    result = pull_files(
        wts_server_name=wts_hostname,
        auth=mock_auth,
        file_metadata_list=mock_external_file_metadata,
        retrievers=retrievers,
        download_path=download_path,
    )
    assert result == None

    # retriever is not in name space
    mock_external_file_metadata = [
        {
            "external_oidc_idp": "test-external-idp",
            "file_retriever": "QDR",
        }
    ]
    mock_retriever.side_effect = NameError
    result = pull_files(
        wts_server_name=wts_hostname,
        auth=mock_auth,
        file_metadata_list=mock_external_file_metadata,
        retrievers=retrievers,
        download_path=download_path,
    )
    assert result == None


def test_pull_files():
    wts_hostname = "test.commons1.io"
    download_path = "."
    mock_external_file_metadata = [
        {
            "external_oidc_idp": "test-external-idp",
            "file_retriever": "QDR",
            "study_id": "QDR_study_01",
        }
    ]
    expected_download_status = {
        "QDR_study_01": DownloadStatus(filename="QDR_study_01", status="downloaded"),
    }

    mock_auth = MagicMock()

    mock_retriever = MagicMock()
    mock_retriever.__name__ = "mock_retriever"
    mock_retriever.return_value = expected_download_status
    retrievers = {"QDR": mock_retriever}

    result = pull_files(
        wts_server_name=wts_hostname,
        auth=mock_auth,
        file_metadata_list=mock_external_file_metadata,
        retrievers=retrievers,
        download_path=download_path,
    )
    assert result == expected_download_status


def test_pull_files_failures():
    wts_hostname = "test.commons1.io"
    download_path = "."

    mock_external_file_metadata = [
        {
            "external_oidc_idp": "test-external-idp",
            "file_retriever": "QDR",
            "study_id": "QDR_study_01",
        },
    ]

    mock_auth = MagicMock()

    mock_retriever = MagicMock()
    mock_retriever.__name__ = "mock_retriever"

    # failed download status is passed through
    expected_download_status = {
        "QDR_study_01": DownloadStatus(filename="QDR_study_01", status="failed")
    }
    mock_retriever.return_value = expected_download_status
    retrievers = {"QDR": mock_retriever}

    result = pull_files(
        wts_server_name=wts_hostname,
        auth=mock_auth,
        file_metadata_list=mock_external_file_metadata,
        retrievers=retrievers,
        download_path=download_path,
    )
    assert result == expected_download_status


def test_download_files_from_metadata_bad_input():
    wts_hostname = "test.commons1.io"
    mock_auth = MagicMock()

    with mock.patch("gen3.auth") as mock_auth, mock.patch(
        "gen3.tools.download.external_file_download.extract_external_file_metadata"
    ) as mock_extract_external_file_metadata:
        mock_retriever = MagicMock()
        mock_retriever.__name__ = "mock_retriever"
        mock_retrievers = {"QDR": mock_retriever}

        # empty retrievers
        result = download_files_from_metadata(
            hostname=wts_hostname,
            auth=mock_auth,
            metadata_file="some_file",
            retrievers={},
            download_path=".",
        )
        assert result == None
        assert not mock_extract_external_file_metadata.called

        # missing metadata file
        mock_auth.get_access_token.return_value = "some_token"
        result = download_files_from_metadata(
            hostname=wts_hostname,
            auth=mock_auth,
            metadata_file="missing_file",
            retrievers=mock_retrievers,
            download_path=".",
        )
        assert result == None
        assert not mock_extract_external_file_metadata.called

        # invalid metadata
        mock_extract_external_file_metadata.return_value = None
        result = download_files_from_metadata(
            hostname=wts_hostname,
            auth=mock_auth,
            metadata_file="tests/download_tests/resources/gen3_metadata_external_file_metadata.json",
            retrievers=mock_retrievers,
            download_path=".",
        )
        assert result == None
        assert mock_extract_external_file_metadata.called


def test_download_files_from_metadata():
    wts_hostname = "test.commons1.io"
    mock_auth = MagicMock()

    with mock.patch("gen3.auth") as mock_auth, mock.patch(
        "gen3.tools.download.external_file_download.pull_files"
    ) as mock_pull_files:
        mock_auth.get_access_token.return_value = "some_token"
        mock_retriever = MagicMock()
        mock_retriever.__name__ = "mock_retriever"
        mock_retrievers = {"QDR": mock_retriever}

        # valid metadata should trigger a call to pull_files
        mock_download_status = {
            "QDR_study_01": DownloadStatus(
                filename="QDR_study_01", status="downloaded"
            ),
            "QDR_file_01": DownloadStatus(filename="QDR_file_01", status="downloaded"),
        }
        mock_pull_files.return_value = mock_download_status
        result = download_files_from_metadata(
            hostname=wts_hostname,
            auth=mock_auth,
            metadata_file="tests/download_tests/resources/gen3_metadata_external_file_metadata.json",
            retrievers=mock_retrievers,
            download_path=".",
        )
        # result should match the value returned by pull_files.
        assert result == mock_download_status
        assert mock_pull_files.called
