import pytest

from gen3.external.nih.utils import get_dbgap_accession_as_parts


@pytest.mark.parametrize("test_input, expected", [
    (
        'phs000123',
        {
            'phsid': 'phs000123',
            'phsid_number': '000123',
            'version': '',
            'version_number': '',
            'participant_set': '',
            'participant_set_number': '',
            'consent': '',
            'consent_number': ''
        }
    ),
    (
        'phs000123.p1.c3',
        {
            'phsid': 'phs000123',
            'phsid_number': '000123',
            'version': '',
            'version_number': '',
            'participant_set': 'p1',
            'participant_set_number': '1',
            'consent': 'c3',
            'consent_number': '3'
        }
    ),
    (
        'phs000123.v3.c3',
        {
            'phsid': 'phs000123',
            'phsid_number': '000123',
            'version': 'v3',
            'version_number': '3',
            'participant_set': '',
            'participant_set_number': '',
            'consent': 'c3',
            'consent_number': '3'
        }
    ),
    (
        'phs000123.v3.p1.c3',
        {
            'phsid': 'phs000123',
            'phsid_number': '000123',
            'version': 'v3',
            'version_number': '3',
            'participant_set': 'p1',
            'participant_set_number': '1',
            'consent': 'c3',
            'consent_number': '3'
        }
    ),
    # Unexpected order of v, p, and c
    (
        'phs000123.p3.v1.c3',
        {
            'phsid': 'phs000123',
            'phsid_number': '000123',
            'version': '',
            'version_number': '',
            'participant_set': 'p3',
            'participant_set_number': '3',
            'consent': '',
            'consent_number': ''
        }
    ),
    (
        'phs000123.v3.c1.p3',
        {
            'phsid': 'phs000123',
            'phsid_number': '000123',
            'version': 'v3',
            'version_number': '3',
            'participant_set': '',
            'participant_set_number': '',
            'consent': 'c1',
            'consent_number': '1'
        }
    ),
    (
        'phs000123.p3.v1',
        {
            'phsid': 'phs000123',
            'phsid_number': '000123',
            'version': '',
            'version_number': '',
            'participant_set': 'p3',
            'participant_set_number': '3',
            'consent': '',
            'consent_number': ''
        }
    ),
])
def test_get_dbgap_accession_as_parts(test_input, expected):
    """
    Test dbgap accession parsing works and outputs expected fields and values.
    """

    assert get_dbgap_accession_as_parts(test_input) == expected