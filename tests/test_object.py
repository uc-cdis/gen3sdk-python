"""
Tests gen3.object.Gen3Object for calls
"""
from unittest.mock import MagicMock, patch
import json
from httpx import delete
import pytest
from requests import HTTPError


@pytest.mark.parametrize(
    "raise_metadata_exception, delete_record, status_code, expected_response",
    [
        (
            True,
            False,
            "",
            "Error in deleting object with 1234 from Metadata Service. Exception --",
        ),
        (
            False,
            True,
            500,
            "Error in deleting object with 1234 from Fence. Response --",
        ),
    ],
)
def test_delete_object(
    gen3_object, raise_metadata_exception, delete_record, status_code, expected_response
):
    with patch("gen3.object.metadata.Gen3Metadata.delete") as mock_request:
        with patch("gen3.object.metadata.Gen3Metadata.query") as mock_query:
            mock_query.return_value = {"guid": "1234"}
            if raise_metadata_exception:
                mock_request.side_effect = Exception("Delete Exception")
            with patch(
                "gen3.object.file.Gen3File.delete_file_locations"
            ) as mock_request:
                with patch("gen3.object.indexd.Gen3Index.get") as mock_indexd_get:
                    with patch(
                        "gen3.object.metadata.Gen3Metadata.create"
                    ) as mock_meta_create:
                        mock_indexd_get.return_value = {"did": "1234"}
                        mock_request.return_value = MagicMock()
                        mock_request.return_value.status_code = status_code
                        with pytest.raises(Exception) as exc:
                            gen3_object.delete_object(
                                guid="1234", delete_record=delete_record
                            )
