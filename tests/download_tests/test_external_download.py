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
    is_valid_external_file_metadata,
    load_all_metadata,
)

DIR = Path(__file__).resolve().parent


@pytest.fixture
def wts_hostname():
    return "test.commons1.io"


@pytest.fixture
def valid_external_file_metadata():
    with open(Path(DIR, "resources/valid_external_file_metadata.json")) as fin:
        return json.load(fin)


def test_load_all_metadata():
    gen3_metadata = load_all_metadata(
        Path(DIR, "resources/gen3_metadata_external_file_metadata.json")
    )
    assert "gen3_discovery" in gen3_metadata
    assert load_all_metadata(Path(DIR, "resources/non_existing_file.json")) is None
    assert load_all_metadata(Path(DIR, "resources/bad_format.json")) is None


def test_is_valid_external_file_metadata_invalid_input():
    # input is missing one of the required key(s) 'file_retriever'
    external_file_metadata = {
        "external_oidc_idp": "test-external-idp",
        "file_id": "QDR_file_02",
    }
    assert is_valid_external_file_metadata(external_file_metadata) is False
    # input is not a dict
    external_file_metadata = ["some", "list"]
    assert is_valid_external_file_metadata(external_file_metadata) is False


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
            "file_id": "QDR_file_03",
        },
        {
            "file_retriever": "Dataverse",
            "file_id": "Dataverse_file_01",
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
        "Dataverse": [
            {
                "file_retriever": "Dataverse",
                "file_id": "Dataverse_file_01",
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


def test_pull_files(wts_hostname, valid_external_file_metadata):
    download_path = "."
    expected_download_status = {
        "QDR_study_01": DownloadStatus(filename="QDR_study_01", status="downloaded"),
        "QDR_file_02": DownloadStatus(filename="QDR_file_02", status="downloaded"),
    }
    mock_auth = MagicMock()

    mock_retriever = MagicMock()
    mock_retriever.__name__ = "mock_retriever"
    mock_retriever.return_value = expected_download_status
    retrievers = {"QDR": mock_retriever}

    result = pull_files(
        wts_server_name=wts_hostname,
        auth=mock_auth,
        file_metadata_list=valid_external_file_metadata,
        retrievers=retrievers,
        download_path=download_path,
    )
    assert result == expected_download_status


def test_pull_files_bad_retriever_input(wts_hostname):
    download_path = "."
    mock_auth = MagicMock()

    mock_retriever = MagicMock()
    mock_retriever.__name__ = "mock_retriever"
    mock_retriever.return_value = "some_status"
    retrievers = {"QDR": mock_retriever}

    # retriever does not exist
    mock_external_file_metadata = [
        {
            "external_oidc_idp": "test-external-idp",
            "file_retriever": "DoesNotExist",
        }
    ]
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


def test_pull_files_failures(wts_hostname, valid_external_file_metadata):
    download_path = "."

    mock_auth = MagicMock()
    mock_retriever = MagicMock()
    mock_retriever.__name__ = "mock_retriever"

    # failed download status from retriever is passed as return value from pull_files
    expected_download_status = {
        "QDR_study_01": DownloadStatus(filename="QDR_study_01", status="failed")
    }
    mock_retriever.return_value = expected_download_status
    retrievers = {"QDR": mock_retriever}

    result = pull_files(
        wts_server_name=wts_hostname,
        auth=mock_auth,
        file_metadata_list=valid_external_file_metadata,
        retrievers=retrievers,
        download_path=download_path,
    )
    assert result == expected_download_status


def test_download_files_from_metadata(valid_external_file_metadata):
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
            external_file_metadata=valid_external_file_metadata,
            retrievers=mock_retrievers,
            download_path=".",
        )
        # result should match the value returned by pull_files.
        assert result == mock_download_status
        assert mock_pull_files.called


def test_download_files_from_metadata_bad_input(
    wts_hostname, valid_external_file_metadata
):
    mock_auth = MagicMock()

    with mock.patch("gen3.auth") as mock_auth, mock.patch(
        "gen3.tools.download.external_file_download.pull_files"
    ) as mock_pull_files:
        # empty retrievers
        result = download_files_from_metadata(
            hostname=wts_hostname,
            auth=mock_auth,
            external_file_metadata=valid_external_file_metadata,
            retrievers={},
            download_path=".",
        )
        assert result == None
        assert not mock_pull_files.called
