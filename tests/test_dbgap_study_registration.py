from unittest.mock import patch, MagicMock

import pytest
import requests

from gen3.external.nih.dbgap_study_registration import dbgapStudyRegistration
from tests.utils_mock_dbgap_study_registration_response import (
    MOCK_PHS001173,
    MOCK_PHS001172,
    MOCK_BAD_RESPONSE,
    MOCK_PHS001174,
    MOCK_PHS000089,
)
from urllib.parse import urlparse, parse_qs


def _mock_requests_get(url):
    """
    Mocks responses for get requests, depending on the query parameters in the provided URL.
    """
    mock_response = MagicMock(spec=requests.Response)
    params = parse_qs(urlparse(url).query)
    output = None
    if params["phs"][0] == "001174":
        output = MOCK_PHS001174
    elif params["phs"][0] == "001173":
        output = MOCK_PHS001173
    elif params["phs"][0] == "001172":
        output = MOCK_PHS001172
    elif params["phs"][0] == "001171":
        output = MOCK_BAD_RESPONSE
    elif params["phs"][0] == "000089":
        output = MOCK_PHS000089
    else:
        # should have requested these studies from the API,
        # if it didn't, something went wrong
        assert "001173" in params or "001172" in params

    mock_response.status_code = 200
    mock_response.text = output
    return mock_response


@pytest.mark.parametrize("include_version", [True, False])
def test_dbgap_study_registration(include_version):
    """
    Test dbGaP study registration metadata parsing works and outputs expected fields and values.

    Note that the dbGaP response is mocked, but the response provided
    is a real response from the dbGaP Study Registration  Server (to simulate current state).

    This does not integration test the dbGaP Study Registration server. In other words,
    if they change format and it would break our code, this will not catch that
    (and it's not the intention to catch that here). This is intended to unit
    test our code to ensure we don't break specifically our parsing in the future.
    """
    dbgap_study_reg = dbgapStudyRegistration(api="https://example.com/ss/dbgapssws.cgi")
    version = "v1"
    phsid_1 = "phs001173"
    phsid_2 = "phs001172"
    phsid_1_req = phsid_1 + f"{('.' + version) if include_version else ''}"
    phsid_2_req = phsid_2 + f"{('.' + version) if include_version else ''}"
    with patch(
        "gen3.external.nih.dbgap_study_registration.requests.get",
        side_effect=_mock_requests_get,
    ):
        metadata = dbgap_study_reg.get_metadata_for_ids([phsid_1_req])
        assert metadata
        assert phsid_1_req in metadata
        assert "StudyInfo" in metadata[phsid_1_req]
        metadata = dbgap_study_reg.get_metadata_for_ids([phsid_2_req])
        assert metadata
        assert phsid_2_req in metadata
        assert "StudyInfo" in metadata[phsid_2_req]


@pytest.mark.parametrize("include_version", [True, False])
def test_dbgap_study_registration_multiple_reqs(include_version):
    """
    Tests response parsing for requests with multiple phsIds. See test_dbgap_study_registration for more info.
    """
    dbgap_study_reg = dbgapStudyRegistration(api="https://example.com/ss/dbgapssws.cgi")
    version = "v1"
    phsid_1 = "phs001173"
    phsid_2 = "phs001172"
    phsid_1_req = phsid_1 + f"{('.' + version) if include_version else ''}"
    phsid_2_req = phsid_2 + f"{('.' + version) if include_version else ''}"
    with patch(
        "gen3.external.nih.dbgap_study_registration.requests.get",
        side_effect=_mock_requests_get,
    ):
        metadata = dbgap_study_reg.get_metadata_for_ids([phsid_1_req, phsid_2_req])
        assert metadata
        assert phsid_2_req in metadata
        assert phsid_1_req in metadata
        assert "StudyInfo" in metadata[phsid_1_req]
        assert "StudyInfo" in metadata[phsid_2_req]


def test_dbgap_study_registration_bad_dbgap_resp():
    """
    Tests handling when attempting to parse an invalid response. See test_dbgap_study_registration for more info.
    """
    dbgap_study_reg = dbgapStudyRegistration(api="https://example.com/ss/dbgapssws.cgi")

    with patch(
        "gen3.external.nih.dbgap_study_registration.requests.get",
        side_effect=_mock_requests_get,
    ):
        bad_phsid = "phs001171"
        metadata = dbgap_study_reg.get_metadata_for_ids([bad_phsid])
        assert bad_phsid not in metadata


def test_dbgap_study_registration_multi_req_bad_dbgap_resp():
    """
    Tests handling when attempting to parse a mixture of valid responses and invalid responses. See test_dbgap_study_registration for more info.
    """
    dbgap_study_reg = dbgapStudyRegistration(api="https://example.com/ss/dbgapssws.cgi")
    phsid_1 = "phs001173"
    phsid_2 = "phs001172"
    bad_phsid = "phs001171"
    with patch(
        "gen3.external.nih.dbgap_study_registration.requests.get",
        side_effect=_mock_requests_get,
    ):
        metadata = dbgap_study_reg.get_metadata_for_ids([phsid_1, phsid_2, bad_phsid])
        assert metadata
        assert phsid_1 in metadata
        assert phsid_2 in metadata
        assert bad_phsid not in metadata
        assert "StudyInfo" in metadata[phsid_2]
        assert "StudyInfo" in metadata[phsid_1]


def test_dbgap_study_registration_multi_resp():
    """
    Tests responses where multiple studies are returned for a single phsId. See test_dbgap_study_registration for more info.
    """
    dbgap_study_reg = dbgapStudyRegistration(api="https://example.com/ss/dbgapssws.cgi")
    multi_resp_phs = "phs001174"
    with patch(
        "gen3.external.nih.dbgap_study_registration.requests.get",
        side_effect=_mock_requests_get,
    ):
        metadata = dbgap_study_reg.get_metadata_for_ids([multi_resp_phs])
        assert metadata
        assert multi_resp_phs in metadata
        assert "StudyInfo" in metadata[multi_resp_phs]


def test_get_child_studies_for_ids():
    """
    Test retrieving child accessions from dbGaP study registration metadata.

    test_dbgap_study_registration disclaimers also apply to this test.
    """
    dbgap_study_reg = dbgapStudyRegistration(api="https://example.com/ss/dbgapssws.cgi")

    with patch(
        "gen3.external.nih.dbgap_study_registration.requests.get",
        side_effect=_mock_requests_get,
    ):
        id_1 = "phs001173"
        id_2 = "phs001172"

        parent_to_child_ids = dbgap_study_reg.get_child_studies_for_ids([id_1])
        assert len(parent_to_child_ids[id_1]) == 0
        parent_to_child_ids = dbgap_study_reg.get_child_studies_for_ids([id_2])
        assert parent_to_child_ids[id_2] == [
            "phs000089.v4.p2",
            "phs001103.v1.p2",
        ]
        parent_to_child_ids = dbgap_study_reg.get_child_studies_for_ids(
            ["phs001173", "phs001172"]
        )
        assert len(parent_to_child_ids[id_1]) == 0
        assert parent_to_child_ids[id_2] == [
            "phs000089.v4.p2",
            "phs001103.v1.p2",
        ]


def test_get_parent_studies_for_id():
    """
    Test retrieving parent accessions from dbGaP study registration metadata.
    """
    dbgap_study_reg = dbgapStudyRegistration(api="https://example.com/ss/dbgapssws.cgi")

    with patch(
        "gen3.external.nih.dbgap_study_registration.requests.get",
        side_effect=_mock_requests_get,
    ):
        id_1 = "phs000089"
        id_2 = "phs001173"

        child_to_parent_ids = dbgap_study_reg.get_parent_studies_for_ids([id_1])
        assert child_to_parent_ids[id_1] == "phs001172.v1.p2"

        child_to_parent_ids = dbgap_study_reg.get_parent_studies_for_ids([id_2])
        import pdb

        pdb.set_trace()
        assert child_to_parent_ids[id_2] is None

        child_to_parent_ids = dbgap_study_reg.get_parent_studies_for_ids([id_1, id_2])
        assert child_to_parent_ids[id_1] == "phs001172.v1.p2"
        assert child_to_parent_ids[id_2] is None
