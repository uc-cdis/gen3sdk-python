import pytest
import json
import requests_mock
from pathlib import Path
from typing import Dict
from unittest import mock

from unittest.mock import MagicMock

from gen3.auth import Gen3AuthError
from gen3.tools.download.drs_download import DownloadStatus
from gen3.tools.download.external_file_download import (
    download_files_from_metadata,
    pull_files,
    retriever_manager,
    extract_external_file_metadata,
    is_valid_external_file_metadata,
    load_metadata,
)

DIR = Path(__file__).resolve().parent


@pytest.fixture
def valid_external_file_metadata():
    with open(Path(DIR, "resources/gen3_metadata_external_file_metadata.json")) as fin:
        metadata = json.load(fin)
        return metadata["gen3_discovery"]["external_file_metadata"]


def test_load_metadata():
    gen3_metadata = load_metadata(
        Path(DIR, "resources/gen3_metadata_external_file_metadata.json")
    )
    assert "gen3_discovery" in gen3_metadata
    assert load_metadata(Path(DIR, "resources/non_existing_file.json")) is None
    assert load_metadata(Path(DIR, "resources/bad_format.json")) is None


@pytest.mark.parametrize(
    "external_file_metadata, expected",
    [
        (
            {
                "external_oidc_idp": "test-external-idp",
                "file_retriever": "QDR",
                "study_id": "QDR_study_01",
            },
            True,
        ),
        (
            {
                "external_oidc_idp": "test-external-idp",
                "file_retriever": "QDR",
                "file_id": "QDR_file_02",
            },
            True,
        ),
        (
            {
                "external_oidc_idp": "test-external-idp",
                "file_retriever": "QDR",
            },
            True,
        ),
        ({"external_oidc_idp": "test-external-idp", "file_id": "QDR_file_02"}, False),
        ({"file_retriever": "QDR", "file_id": "QDR_file_02"}, False),
    ],
)
def test_is_valid_external_file_metadata(external_file_metadata: Dict, expected: bool):
    assert is_valid_external_file_metadata(external_file_metadata) == expected


def test_extract_external_file_metadata(valid_external_file_metadata):
    # valid metadata
    metadata_file = "resources/gen3_metadata_external_file_metadata.json"
    metadata = load_metadata(Path(DIR, metadata_file))
    external_file_metadata = extract_external_file_metadata(metadata)
    expected_metadata = valid_external_file_metadata

    assert type(external_file_metadata) is list
    assert len(external_file_metadata) == 2
    assert external_file_metadata == expected_metadata

    # invalid metadata - missing keys
    assert (
        extract_external_file_metadata(
            {"_guid_type": "discovery_metadata", "gen3_discovery": {"authz": ""}}
        )
        == None
    )
    assert extract_external_file_metadata({"_guid_type": "discovery_metadata"}) == None
    assert extract_external_file_metadata({}) == None


def test_retriever_manager():
    mock_retriever = MagicMock()
    mock_retriever.__name__ = "mock_retriever"

    mock_token = "fake_token"
    retrievers = {"QDR": mock_retriever}
    external_file_metadata = {
        "external_oidc_idp": "test-external-idp",
        "file_retriever": "QDR",
        "study_id": "QDR_study_01",
    }
    download_path = "."
    mock_retriever.return_value = True

    result = retriever_manager(
        file_metadata=external_file_metadata,
        token=mock_token,
        retrievers=retrievers,
        download_path=download_path,
    )
    mock_retriever.assert_called_with(external_file_metadata, mock_token, download_path)
    # assert that manager has returned the result of retriever if successful
    assert result == mock_retriever.return_value


@pytest.mark.parametrize(
    "connected_status, returned_idp_token, mock_external_file_metadata, expected_download_status",
    [
        (
            200,
            "some_id_token",
            [
                {
                    "external_oidc_idp": "test-external-idp",
                    "file_retriever": "QDR",
                    "study_id": "QDR_study_01",
                },
                {
                    "external_oidc_idp": "test-external-idp",
                    "file_retriever": "QDR",
                    "file_id": "QDR_file_01",
                },
            ],
            {
                "QDR_study_01": DownloadStatus(
                    filename="QDR_study_01", status="downloaded"
                ),
                "QDR_file_01": DownloadStatus(
                    filename="QDR_file_01", status="downloaded"
                ),
            },
        ),
        (
            200,
            "some_id_token",
            [
                {
                    "external_oidc_idp": "test-external-idp",
                    "file_retriever": "QDR",
                    "study_id": "QDR_study_01",
                },
                {"foo": "bar"},
            ],
            {
                "QDR_study_01": DownloadStatus(
                    filename="QDR_study_01", status="downloaded"
                ),
                "1": DownloadStatus(filename="1", status="invalid file metadata"),
            },
        ),
        (
            200,
            "some_id_token",
            [
                {
                    "external_oidc_idp": "test-external-idp",
                    "file_retriever": "does not exist",
                    "study_id": "QDR_study_01",
                }
            ],
            {
                "QDR_study_01": DownloadStatus(
                    filename="QDR_study_01", status="invalid file retriever"
                )
            },
        ),
        (
            200,
            None,
            [
                {
                    "external_oidc_idp": "test-external-idp",
                    "file_retriever": "QDR",
                    "study_id": "QDR_study_01",
                }
            ],
            {"QDR_study_01": DownloadStatus(filename="QDR_study_01", status="failed")},
        ),
        (
            403,
            "some_id_token",
            [
                {
                    "external_oidc_idp": "test-external-idp",
                    "file_retriever": "QDR",
                    "study_id": "QDR_study_01",
                }
            ],
            {"QDR_study_01": DownloadStatus(filename="QDR_study_01", status="failed")},
        ),
    ],
)
def test_pull_files(
    connected_status,
    returned_idp_token,
    mock_external_file_metadata,
    expected_download_status,
):
    wts_hostname = "test.commons1.io"
    download_path = "."

    mock_auth = MagicMock()
    mock_auth.get_access_token.return_value = "some_token"

    # test with the same idp across all mock file_metadata
    test_idp = "test-external-idp"

    mock_retriever = MagicMock()
    mock_retriever.__name__ = "mock_retriever"
    retrievers = {"QDR": mock_retriever}

    with requests_mock.Mocker() as m:
        m.get(
            f"https://{wts_hostname}/wts/oauth2/connected",
            json={"foo": "bar"},
            status_code=connected_status,
        )
        m.get(
            f"https://{wts_hostname}/wts/token/?idp={test_idp}",
            json={"token": returned_idp_token},
        )

        with mock.patch(
            "gen3.tools.download.external_file_download.wts_get_token"
        ) as wts_get_token:
            wts_get_token.return_value = returned_idp_token

            result = pull_files(
                wts_server_name=wts_hostname,
                auth=mock_auth,
                external_file_metadata=mock_external_file_metadata,
                retrievers=retrievers,
                download_path=download_path,
            )

            assert result == expected_download_status
            # assert wts_get_token is called with correct idp value
            if (
                connected_status == 200
                and mock_external_file_metadata[0].get("file_retriever")
                != "does not exist"
            ):
                wts_get_token.assert_called_with(
                    hostname=wts_hostname, idp=test_idp, access_token="some_token"
                )


def test_download_files_from_metadata(valid_external_file_metadata):
    wts_hostname = "test.commons1.io"
    mock_auth = MagicMock()

    with mock.patch("gen3.auth") as mock_auth, mock.patch(
        "gen3.tools.download.external_file_download.pull_files"
    ) as mock_pull_files:
        with mock.patch(
            "gen3.tools.download.external_file_download.extract_external_file_metadata"
        ) as mock_extract_external_file_metadata:
            mock_auth.get_access_token.return_value = "some_token"
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
            assert not mock_pull_files.called

            # # auth cannot return wts access token
            # mock_auth.get_access_token.raiseError.side_effect = Gen3AuthError
            # pull_files.return_value = {}
            # result = download_files_from_metadata(
            #     hostname = wts_hostname,
            #     auth = mock_auth,
            #     metadata_file = "some_file",
            #     retrievers = {},
            #     download_path = "."
            # )
            # assert result == None
            # assert not pull_files.called

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
            assert not mock_pull_files.called

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
            assert not mock_pull_files.called

        # TODO: this case should be in test_pull_files
        # auth cannot return wts access token
        mock_auth.get_access_token.raiseError.side_effect = Gen3AuthError
        mock_pull_files.return_value = {}
        result = download_files_from_metadata(
            hostname=wts_hostname,
            auth=mock_auth,
            metadata_file="tests/download_tests/resources/gen3_metadata_external_file_metadata.json",
            retrievers=mock_retrievers,
            download_path=".",
        )
        assert result == {}
        assert mock_pull_files.called

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
