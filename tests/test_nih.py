"""
Tests gen3.nih
"""
import json
import os
import pytest
import sys

import requests
from requests.auth import HTTPBasicAuth
from unittest.mock import MagicMock, patch

from gen3.nih import dbgapFHIR
from tests.utils_mock_fhir_response import (
    MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000007,
    MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000166,
)


def test_dbgap_fhir():
    """
    Test DOI creation under various inputs, ensure it still works even when
    special characters are used. Ensure it doesn't work when invalid input is
    provided (like not specify prefix OR id).
    """
    dbgap_fhir = dbgapFHIR(
        api="https://example.com/fhir/x1",
        auth_provider=HTTPBasicAuth("DATACITE_USERNAME", "DATACITE_PASSWORD"),
    )

    def _mock_request(path, **kwargs):
        assert "ResearchStudy" in path

        output = None

        if path == "ResearchStudy/phs000007":
            output = MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000007
        elif path == "ResearchStudy/phs000166":
            output = MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000166
        else:
            # should have requested these studies from the API,
            # if it didn't, something went wrong
            assert path in ["ResearchStudy/phs000007", "ResearchStudy/phs000166"]

        return output

    dbgap_fhir.fhir_client.server.request_json = MagicMock(side_effect=_mock_request)

    phsids = [
        "phs000007.v1.p1.c1",
        "phs000166.c3",
    ]

    metadata = dbgap_fhir.get_metadata_for_ids(phsids)

    assert metadata

    assert "phs000007" in metadata
    assert "phs000166" in metadata

    expected_phs000007_keys = [
        "StudyOverviewUrl",
        "ReleaseDate",
        "StudyConsents",
        "Citers",
        "NumPhenotypeDatasets",
        "NumMolecularDatasets",
        "NumVariables",
        "NumDocuments",
        "NumAnalyses",
        "NumSubjects",
        "NumSamples",
        "NumSubStudies",
        "Id",
        "Category",
        "Condition",
        "Description",
        "Enrollment",
        "Focus",
        "Identifier",
        "Keyword",
        "Sponsor",
        "Status",
        "Title",
        "ResourceType",
    ]

    expected_phs000166_keys = [
        "StudyOverviewUrl",
        "ReleaseDate",
        "StudyConsents",
        "Citers",
        "NumPhenotypeDatasets",
        "NumMolecularDatasets",
        "NumVariables",
        "NumDocuments",
        "NumSubjects",
        "NumSamples",
        "NumSubStudies",
        "Id",
        "Category",
        "Description",
        "Enrollment",
        "Identifier",
        "Sponsor",
        "Status",
        "Title",
        "ResourceType",
    ]

    for key in expected_phs000007_keys:
        assert key in metadata["phs000007"]

    for key in expected_phs000166_keys:
        assert key in metadata["phs000166"]

    # check a few values to ensure correct parsing and representation as string
    assert metadata["phs000007"]["NumSubjects"] == "15144"
    assert metadata["phs000166"]["NumSubjects"] == "4046"

    assert metadata["phs000007"]["Title"] == "Framingham Cohort"
    assert type(metadata["phs000166"]["Citers"]) == list

    # these should have been converted to a single string, not a list
    for item in dbgap_fhir.suspected_single_item_list_fields:
        capitalized_item = item[:1].upper() + item[1:]
        if capitalized_item in metadata["phs000007"]:
            assert type(metadata["phs000007"][capitalized_item]) != list
        if capitalized_item in metadata["phs000166"]:
            assert type(metadata["phs000166"][capitalized_item]) != list

    # ensure the custom fields got added
    assert "ResearchStudyURL" in metadata["phs000007"]
    assert "phs000007" in metadata["phs000007"]["ResearchStudyURL"]
    assert "ResearchStudyURL" in metadata["phs000166"]
    assert "phs000166" in metadata["phs000166"]["ResearchStudyURL"]

    assert "Disclaimer" in metadata["phs000007"]
    assert "Disclaimer" in metadata["phs000166"]
