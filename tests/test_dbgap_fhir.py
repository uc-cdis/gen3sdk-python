"""
Tests gen3.nih
"""
import json
import os
import pytest
import sys
from copy import deepcopy

import requests
from requests.auth import HTTPBasicAuth
from unittest.mock import MagicMock, patch

try:
    from gen3.external.nih.dbgap_fhir import dbgapFHIR
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "Missing some modules for optional external API parsing. Ensure you've "
        "installed all optional extras using `poetry install --all-extras`. "
        f"Original error: {exc}"
    )
from tests.test_discovery import _get_tsv_data
from tests.utils_mock_fhir_response import (
    MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000007,
    MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000166,
)


def test_dbgap_fhir(tmp_path):
    """
    Test dbGaP FHIR parsing works and outputs expected fields and values.

    Note that the dbGaP FHIR response is mocked, but the response provided
    is a real response from the dbGaP FHIR Server (to simulate current state).

    This does not integration test the dbGaP FHIR server. In other words,
    if they change format and it would break our code, this will not catch that
    (and it's not the intention to catch that here). This is intended to unit
    test our code to ensure we don't break specifically our parsing in the future.
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

    assert "phs000007.v1.p1.c1" in metadata
    assert "phs000166.c3" in metadata

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
        assert key in metadata["phs000007.v1.p1.c1"]

    for key in expected_phs000166_keys:
        assert key in metadata["phs000166.c3"]

    # check a few values to ensure correct parsing and representation as string
    assert metadata["phs000007.v1.p1.c1"]["NumSubjects"] == "15144"
    assert metadata["phs000166.c3"]["NumSubjects"] == "4046"

    assert metadata["phs000007.v1.p1.c1"]["Title"] == "Framingham Cohort"
    assert type(metadata["phs000166.c3"]["Citers"]) == list

    # these should have been converted to a single string, not a list
    for item in dbgap_fhir.suspected_single_item_list_fields:
        capitalized_item = item[:1].upper() + item[1:]
        if capitalized_item in metadata["phs000007.v1.p1.c1"]:
            assert type(metadata["phs000007.v1.p1.c1"][capitalized_item]) != list
        if capitalized_item in metadata["phs000166.c3"]:
            assert type(metadata["phs000166.c3"][capitalized_item]) != list

    # ensure the custom fields got added
    assert "ResearchStudyURL" in metadata["phs000007.v1.p1.c1"]
    assert "phs000007" in metadata["phs000007.v1.p1.c1"]["ResearchStudyURL"]
    assert "ResearchStudyURL" in metadata["phs000166.c3"]
    assert "phs000166" in metadata["phs000166.c3"]["ResearchStudyURL"]

    assert "Disclaimer" in metadata["phs000007.v1.p1.c1"]
    assert "Disclaimer" in metadata["phs000166.c3"]

    file_name = tmp_path / "fhir_metadata_file_TEST.tsv"
    dbgapFHIR.write_data_to_file(metadata, file_name)
    assert _get_tsv_data(file_name) == _get_tsv_data(
        "tests/test_data/fhir_metadata.tsv"
    )


def test_dbgap_fhir_sanitizes_unsafe_markdown_links():
    dbgap_fhir = dbgapFHIR(
        api="https://example.com/fhir/x1",
        auth_provider=HTTPBasicAuth("DATACITE_USERNAME", "DATACITE_PASSWORD"),
    )

    unsafe_response = deepcopy(MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000007)
    unsafe_response[
        "description"
    ] = "Description with [malformed JS](javascript:getPage(this, 'document.cgi', 2022 and also [unsafe link](javascript:getPage(this, 'document.cgi', 2022);return true;) and context"
    unsafe_response["keyword"][0][
        "text"
    ] = "[JS](javascript:getPage(this, 'document.cgi', 2022);return true;)"
    unsafe_response["keyword"][1]["text"] = "[VB](vbscript:msgbox(1))"
    unsafe_response["keyword"][2]["text"] = "[DATA](data:text/html;base64,PHNjcmlwdD4=)"

    clean_response = deepcopy(MOCK_NIH_DBGAP_FHIR_RESPONSE_FOR_PHS000166)

    def _mock_request(path, **kwargs):
        if path == "ResearchStudy/phs000007":
            return unsafe_response
        if path == "ResearchStudy/phs000166":
            return clean_response

        assert path in ["ResearchStudy/phs000007", "ResearchStudy/phs000166"]

    dbgap_fhir.fhir_client.server.request_json = MagicMock(side_effect=_mock_request)

    metadata = dbgap_fhir.get_metadata_for_ids(
        [
            "phs000007.v1.p1.c1",
            "phs000166.c3",
        ]
    )

    unsafe_metadata = metadata["phs000007.v1.p1.c1"]
    assert (
        unsafe_metadata["Description"]
        == "Description with [malformed JS](DEFUSED_javascript:getPage(this, 'document.cgi', 2022 and also unsafe link and context"
    )

    assert isinstance(unsafe_metadata["Keyword"], list)
    for item in unsafe_metadata["Keyword"]:
        assert "javascript:" not in item.lower()
        assert "vbscript:" not in item.lower()
        assert "data:text/html" not in item.lower()

    assert "JS" in unsafe_metadata["Keyword"]
    assert "VB" in unsafe_metadata["Keyword"]
    assert "DATA" in unsafe_metadata["Keyword"]

    clean_metadata = metadata["phs000166.c3"]
    assert clean_metadata["Description"] == clean_response["description"]
