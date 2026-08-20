from unittest.mock import patch

import pytest

from gen3.external.nih.dbgap_doi import dbgapDOI


VALID_PHSID = "phs000007.v1.p1.c1"
INVALID_PHSID = "phs002909.v2.p1.c1"


@pytest.mark.parametrize("invalid_release_date", [None, "not-a-date"])
def test_get_metadata_for_ids_skips_invalid_release_dates(
    invalid_release_date,
):
    """
    Pre BDC-1248 behavior: a malformed upstream record (e.g. missing or
    unparseable ReleaseDate) raised an exception that aborted the entire batch.

    This test requests one valid accession and one invalid accession together
    and asserts:
    - the valid accession's DOI metadata is still returned,
    - the invalid accession is omitted rather than raising,
    - a warning naming the accession and the raw bad ReleaseDate is logged.

    Parametrized with a None value and a non-empty value that doesn't parse to a four digit year.
    """
    study_registration_metadata = {
        VALID_PHSID: {"Authority": {"Persons": {"Person": []}}},
        INVALID_PHSID: {"Authority": {"Persons": {"Person": []}}},
    }
    fhir_metadata = {
        VALID_PHSID: {
            "Title": "Valid study",
            "ReleaseDate": "2024-01-01",
            "Identifier": [VALID_PHSID],
            "Description": "A valid dbGaP study.",
            "Sponsor": "NHLBI",
        },
        INVALID_PHSID: {
            "Title": "Invalid release date study",
            "ReleaseDate": invalid_release_date,
            "Identifier": [INVALID_PHSID],
            "Description": "A dbGaP study without a valid release date.",
            "Sponsor": "NHLBI",
        },
    }

    with patch(
        "gen3.external.nih.dbgap_doi.dbgapStudyRegistration"
    ) as mock_study_registration, patch(
        "gen3.external.nih.dbgap_doi.dbgapFHIR"
    ) as mock_fhir, patch.object(
        dbgapDOI, "_get_doi_contributors", return_value=[]
    ), patch(
        "gen3.external.nih.dbgap_doi.logging.warning"
    ) as mock_warning:
        mock_study_registration.return_value.get_metadata_for_ids.return_value = (
            study_registration_metadata
        )
        mock_fhir.return_value.get_metadata_for_ids.return_value = fhir_metadata

        # Request both accessions
        doi_metadata = dbgapDOI(publisher="Example publisher").get_metadata_for_ids(
            [VALID_PHSID, INVALID_PHSID]
        )

    # The invalid accession must be skipped and not raised
    assert list(doi_metadata) == [VALID_PHSID]
    assert doi_metadata[VALID_PHSID]["publication_year"] == "2024"
    # Make sure the the warning names the accession and the raw
    # bad value so we can trace it back to the dbGaP source record
    assert any(
        INVALID_PHSID in call.args[0]
        and "ReleaseDate" in call.args[0]
        and repr(invalid_release_date) in call.args[0]
        for call in mock_warning.call_args_list
    )


def test_get_metadata_for_ids_normalizes_contributors_missing_type():
    """
    Pre BDC-1248 behavior: contributors lacking a DataCite-required
    contributorType caused DataCite to reject the payload with an HTTP 422.

    This test gives one contributor missing contributorType and one with an
    existing value, and asserts the missing type is defaulted to "Other" while
    the existing value is untouched.
    """
    study_registration_metadata = {
        VALID_PHSID: {"Authority": {"Persons": {"Person": []}}},
    }
    fhir_metadata = {
        VALID_PHSID: {
            "Title": "Valid study",
            "ReleaseDate": "2024-01-01",
            "Identifier": [VALID_PHSID],
            "Description": "A valid dbGaP study.",
            "Sponsor": "NHLBI",
        },
    }
    contributors = [
        {"name": "Untyped contributor"},
        {"name": "Typed contributor", "contributorType": "ProjectLeader"},
    ]

    with patch(
        "gen3.external.nih.dbgap_doi.dbgapStudyRegistration"
    ) as mock_study_registration, patch(
        "gen3.external.nih.dbgap_doi.dbgapFHIR"
    ) as mock_fhir, patch.object(
        dbgapDOI, "_get_doi_contributors", return_value=contributors
    ):
        mock_study_registration.return_value.get_metadata_for_ids.return_value = (
            study_registration_metadata
        )
        mock_fhir.return_value.get_metadata_for_ids.return_value = fhir_metadata

        doi_metadata = dbgapDOI(publisher="Example publisher").get_metadata_for_ids(
            [VALID_PHSID]
        )

    assert doi_metadata[VALID_PHSID]["contributors"] == [
        {"name": "Untyped contributor", "contributorType": "Other"},
        {"name": "Typed contributor", "contributorType": "ProjectLeader"},
    ]
