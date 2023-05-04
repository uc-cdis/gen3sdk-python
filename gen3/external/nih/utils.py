import re

from cdislogging import get_logger

logging = get_logger("__name__")

# For more details about this regex, see the function that uses it
DBGAP_ACCESSION_REGEX = (
    "(?P<phsid>phs(?P<phsid_number>[0-9]+))"
    "(.(?P<participant_set>p(?P<participant_set_number>[0-9]+))){0,1}"
    "(.(?P<version>v(?P<version_number>[0-9]+))){0,1}"
    "(.(?P<consent>c(?P<consent_number>[0-9]+)+)){0,1}"
)


def get_dbgap_accession_as_parts(phsid):
    """
    Return a dictionary containing the various parts of the provided
    dbGaP Accession (AKA phsid).

    Uses a regex to match an assession number that has information in forms like:
      phs000123.c1
      phs000123.v3.p1.c3
      phs000123.c3
      phs000123.v3.p4.c1
      phs000123

    This separates out each part of the accession with named groups and includes
    parts that include only the numbered value (which is needed in some NIH APIs)

    A "picture" is worth a 1000 words:

    Example for `phs000123.c1`:
      Named groups
      phsid                   phs000123
      phsid_number            000123
      version                 None
      version_number          None
      participant_set         None
      participant_set_number  None
      consent                 c1
      consent_number          1

    Args:
        phsid (str): The dbGaP Accession (AKA phsid)

    Returns:
        dict[str]: A standardized dictionary (you can always expect these keys)
                   with the values parsed from the provided dbGaP Accession
                   Example if provided `phs000123.c1`: {
                        "phsid": "phs000123",
                        "phsid_number": "000123",
                        "version": "",
                        "version_number": "",
                        "participant_set": "",
                        "participant_set_number": "",
                        "consent": "c1",
                        "consent_number": "1",
                    }

                    NOTE: the "*_number" fields are still represented as strings.
                    NOTE2: the regex groups that return None will be represented
                           as empty strings (for easier upstream str concat-ing)
    """
    access_number_matcher = re.compile(DBGAP_ACCESSION_REGEX)
    raw_phs_match = access_number_matcher.match(phsid)
    phs_match = {}

    if raw_phs_match:
        phs_match = raw_phs_match.groupdict()

    standardized_phs_match = {}
    for key, value in phs_match.items():
        if value is None:
            standardized_phs_match[key] = ""
            continue

        standardized_phs_match[key] = value

    return standardized_phs_match
