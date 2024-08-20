"""
Tests gen3.tools.fhir_loader
"""
import pytest
import json
from unittest.mock import patch, MagicMock
from gen3.tools.fhir_loader import FHIRLoader


class MockFHIRLoader(FHIRLoader):
    """
    Mock FHIRLoader subclass.
    """

    def convert_to_FHIR_object(self, gen3_object):
        # Mock return object
        return {"resourceType": "ResearchStudy", "id": gen3_object["id"]}


@pytest.fixture
def mock_loader():
    """
    Mock subclass instance.
    """
    return MockFHIRLoader(endpoint="https://fhir.example.com", auth_token="test_token")


def test_load_fhir_object(mock_loader):
    """
    Test that load_FHIR_object calls session.put with correct JSON payload.
    """
    fhir_object = {"resourceType": "ResearchStudy", "id": "12345"}
    with patch.object(
        mock_loader.session, "put", return_value=MagicMock(status_code=201)
    ) as mock_put:
        mock_loader.load_FHIR_object(fhir_object)
        mock_put.assert_called_once_with(
            "https://fhir.example.com/ResearchStudy", data=json.dumps(fhir_object)
        )


def test_load_fhir_object_failure(mock_loader):
    """
    Test that load_FHIR_object logs an error when the request fails.
    """
    fhir_object = {"resourceType": "ResearchStudy", "id": "12345"}
    with patch.object(
        mock_loader.session,
        "put",
        return_value=MagicMock(status_code=400, text="Bad Request"),
    ) as mock_put:
        with pytest.raises(Exception) as exc_info:
            mock_loader.load_FHIR_object(fhir_object)

        # Check that the exception contains the expected error message
        assert "Failed to PUT FHIR data" in str(exc_info.value)


def test_delete_fhir_object(mock_loader):
    """
    Test that delete_fhir_object calls session.delete with correct URL.
    """
    with patch.object(
        mock_loader.session, "delete", return_value=MagicMock(status_code=204)
    ) as mock_delete:
        mock_loader.delete_fhir_object("ResearchStudy", "12345")
        mock_delete.assert_called_once_with(
            "https://fhir.example.com/ResearchStudy/12345"
        )


def test_delete_fhir_object_failure(mock_loader, caplog):
    """
    Test that delete_fhir_object logs an error when the DELETE request fails.
    """
    with patch.object(
        mock_loader.session,
        "delete",
        return_value=MagicMock(status_code=404, text="Not Found"),
    ) as mock_delete:
        with pytest.raises(Exception) as exc_info:
            mock_loader.delete_fhir_object("ResearchStudy", "12345")

        # Check that the exception contains the expected error message
        assert "Failed to delete" in str(exc_info.value)


def test_load_metadata_into_fhir(mock_loader):
    """
    Test that load_metadata_into_fhir reads a file of metadata, converts the data, and loads it into a FHIR server.
    """
    fhir_object = {"resourceType": "ResearchStudy", "id": "12345"}
    mock_metadata_file = "tests/test_data/mock_metadata.tsv"
    with patch.object(
        mock_loader.session, "put", return_value=MagicMock(status_code=201)
    ) as mock_put:
        mock_loader.load_metadata_into_fhir(mock_metadata_file)
        mock_put.assert_called_once_with(
            "https://fhir.example.com/ResearchStudy", data=json.dumps(fhir_object)
        )
