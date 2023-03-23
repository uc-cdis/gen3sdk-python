import backoff
import collections
import copy
import csv
import time
import re  # sorry. But trust me, it makes things a bit cleaner

from cdislogging import get_logger
import fhirclient.models.researchstudy as researchstudy
from fhirclient import client
from fhirclient.models.fhirreference import FHIRReference
from fhirclient.models.identifier import Identifier
from fhirclient.models.meta import Meta

from gen3.tools.metadata.discovery import sanitize_tsv_row
from gen3.utils import DEFAULT_BACKOFF_SETTINGS

logging = get_logger("__name__")

# For more details about this regex, see the function that uses it
DBGAP_ACCESSION_REGEX = (
    "(?P<phsid>phs(?P<phsid_number>[0-9]+))"
    "(.(?P<version>v(?P<version_number>[0-9]+))){0,1}"
    "(.(?P<participant_set>p(?P<participant_set_number>[0-9]+))){0,1}"
    "(.(?P<consent>c(?P<consent_number>[0-9]+)+)){0,1}"
)


class dbgapFHIR(object):
    """
    Class for interacting with Database for Genotypes and Phenotypes (dbGaP) FHIR API.

    Example Usage:

        ```
        from gen3 import dbgapFHIR


        def main():
            studies = [
                "phs000007",
                "phs000166.c3",
                "phs000179.v1.p1.c1",
            ]
            dbgapfhir = dbgapFHIR()
            simplified_data = dbgapfhir.get_metadata_for_ids(phsids=studies)

            file_name = "data_file.tsv"
            dbgapFHIR.write_data_to_file(simplified_data, file_name)


        if __name__ == "__main__":
            main()
        ```

    This provides utilities for getting public study-level data using a third-party
    FHIR client library behind the scenes. This also provides some opinionated methods
    for transforming the raw dbGaP data into a "simplified" format.

    dbGaP's FHIR naming conventions and organization doesn't translate well to simple
    key-value pairs. We want to translate the info provided to simplify its understanding
    and allow ingest into our Metadata API as denormalized as possible, in other words, reduce
    the verbosity and nesting.

    This code attempts to remain the least "hard-coded" as possible, prefering to
    add special cases for known deviations of structure rather than writing code to literally
    parse every field we want.

    Note that either way, if dbGaP changes their structure or we want additional information,
    some of these functions may need to get reworked (or calling functions will need to do
    "hard-coded" parsing in addition to using the raw FHIR output objects / FHIR library).

    Known dbGaP FHIR fields interpretted as Lists:
        identifier
        keyword
        Citers
        condition
        StudyConsents
        partOf

    Known dbGaP FHIR fields interpretted as Strings:
        description
        id
        status
        resourceType
        title
        sponsor
        StudyOverviewUrl
        ReleaseDate
        focus
        enrollment
        category

    Known dbGaP FHIR fields interpretted as Strings but generally represent Integers:
        NumMolecularDatasets
        NumSubjects
        NumSubStudies
        NumPhenotypeDatasets
        NumSamples
        NumAnalyses
        NumVariables
        NumDocuments

    Attributes:
        api (str): URL to dbGaP's FHIR API
        DBGAP_SLEEP_TIME_TO_AVOID_RATE_LIMITS (float): Time to sleep in seconds between
            requests to dbGaP's FHIR server. This was tuned to avoid rate limiting
            experienced with smaller values.

            TODO: implement exponential backoff using backoff library used elsewhere
        fhir_client (fhirclient.client.FHIRClient): client class for interacting with FHIR
        SUSPECTED_SINGLE_ITEM_LIST_FIELDS (List[str]): dbGaP FHIR fields that are lists
            in the API, but in all testing only yield (and logicially should only yield)
            a single result. The single item in the list will be pulled out so that the
            final field is NOT a list.
        UNNECESSARY_FIELDS(List[str]): fields to remove in final result
    """

    # this is necessary in addition to exponential backoff to reduce the number
    # of forced backoff requests (too many requests in a timeframe seem to trip
    # some rate limiting on NIH's side)
    DBGAP_SLEEP_TIME_TO_AVOID_RATE_LIMITS = 0.6

    SUSPECTED_SINGLE_ITEM_LIST_FIELDS = [
        "sponsor",
        "StudyOverviewUrl",
        "ReleaseDate",
        "focus",
        "enrollment",
        "category",
    ]

    UNNECESSARY_FIELDS = [
        "meta",
        "StudyMarkersets",
        "security",
        "ComputedAncestry",
        "MolecularDataTypes",
    ]

    DISCLAIMER = (
        "This information was retrieved from dbGaP's FHIR API for "
        "discoverability purposes and may not contain fully up-to-date "
        "information. Please refer to the official dbGaP FHIR server for up-to-date "
        "FHIR data."
    )

    def __init__(
        self,
        api="https://dbgap-api.ncbi.nlm.nih.gov/fhir/x1",
        auth_provider=None,
        fhir_app_id="gen3_sdk",
        suspected_single_item_list_fields=None,
        unnecessary_fields=None,
        disclaimer=None,
    ):
        """
        Inializate an instance of the class

        Args:
            api (str, optional): URL to dbGaP's FHIR API
            auth_provider (None, optional): Description
            fhir_app_id (str, optional): name of app_id to pass into the FHIR client library
        """
        fhir_client_settings = {
            "app_id": fhir_app_id,
            "api_base": api,
        }
        self._auth_provider = auth_provider
        self.api = api
        self.fhir_client = client.FHIRClient(settings=fhir_client_settings)
        self.suspected_single_item_list_fields = (
            suspected_single_item_list_fields
            or dbgapFHIR.SUSPECTED_SINGLE_ITEM_LIST_FIELDS
        )
        self.unnecessary_fields = unnecessary_fields or dbgapFHIR.UNNECESSARY_FIELDS
        self.disclaimer = disclaimer or dbgapFHIR.DISCLAIMER

    def get_metadata_for_ids(self, phsids):
        """
        Return simplified FHIR metadata for each of the provided IDs.

        NOTE: This is NOT the raw metadata from the dbGaP FHIR server. This is a
              denormalized and simplified representation of that data intended
              to both reduce the nesting and ease the understanding of the
              information.

        Args:
            phsids (List[str]): list of IDs to query FHIR for

        Returns:
            Dict[dict]: metadata for each of the provided IDs (which
                        are the keys in the returned dict)
        """
        simplified_data = {}

        standardized_phsids = []
        for phsid in phsids:
            phsid_parts = get_dbgap_accession_as_parts(phsid)

            # allow form of ONLY "phs000123", dbGaP FHIR API cannot handle
            # isolating a specific version
            standardized_phsid = phsid_parts["phsid"]
            standardized_phsids.append(standardized_phsid)

        all_data = self.get_dbgap_fhir_objects_for_studies(standardized_phsids)
        for study_id, study in all_data.items():
            data = self.get_simplified_data_from_object(study)

            # custom fields
            data["ResearchStudyURL"] = (
                self.api.rstrip("/") + "/ResearchStudy/" + study_id
            )
            data["Disclaimer"] = self.disclaimer

            logging.debug(f"simplified {study_id} study data: {data}")
            simplified_data[study_id] = data

        return simplified_data

    def get_dbgap_fhir_objects_for_studies(self, phsids):
        """
        Return FHIR Objects

        Args:
            phsids (List[str]): list of IDs to query FHIR for

        Returns:
            Dict[fhirclient.models.ResearchStudy]: FHIR objects for each of the
                provided IDs (which are the keys in the returned dict)
        """
        all_data = {}
        for study_id in phsids:
            try:
                study = self._get_dbgap_fhir_objects_for_study(study_id)
            except Exception as exc:
                logging.error(f"unable to get {study_id}, skipping. Error: {exc}")
                continue

            # dbGaP API rate limits if we go too fast
            # with "429 Client Error: Too Many Requests for url"
            if dbgapFHIR.DBGAP_SLEEP_TIME_TO_AVOID_RATE_LIMITS > 0:
                logging.debug(
                    f"sleeping for {dbgapFHIR.DBGAP_SLEEP_TIME_TO_AVOID_RATE_LIMITS} seconds so we don't make dbGaP's API mad."
                )
                time.sleep(dbgapFHIR.DBGAP_SLEEP_TIME_TO_AVOID_RATE_LIMITS)

            logging.debug(f"raw study data: {study.as_json()}")
            all_data[study_id] = study

        return all_data

    @backoff.on_exception(
        backoff.expo,
        Exception,
        max_tries=7,
        **{
            key: value
            for key, value in DEFAULT_BACKOFF_SETTINGS.items()
            if key != "max_tries"
        },
    )
    def _get_dbgap_fhir_objects_for_study(self, phsid):
        logging.info(f"getting {phsid} from dbGaP FHIR API...")
        study = None

        try:
            study = researchstudy.ResearchStudy.read(phsid, self.fhir_client.server)
        except Exception as exc:
            logging.warning(f"unable to get {phsid}, skipping. Error: {exc}")
            raise

        return study

    @staticmethod
    def write_data_to_file(all_data, file_name):
        """
        Write the values of every key in the provided dictionary to a tabular file (TSV).

        This will convert any top level fields that are Dicts to a JSON representation
        such that a single all_data[key] becomes a single row in the outputed TSV.

        Args:
            all_data (Dict[Dict]): study metadata
            file_name (str): output file name
        """
        logging.info(f"beginning to write data to file: {file_name}")
        data_file = open(file_name, "w")

        headers = set()

        for study_id, study_data in all_data.items():
            for item in study_data.keys():
                headers.add(item)

        csv_writer = csv.DictWriter(
            data_file, delimiter="\t", fieldnames=sorted(headers), extrasaction="ignore"
        )
        csv_writer.writeheader()

        for study_id, study_data in all_data.items():
            # Writing data of CSV file
            csv_writer.writerow(sanitize_tsv_row(study_data))

        logging.info(f"done writing data to file: {file_name}")

    def get_simplified_data_from_object(self, fhir_object):
        """
        An attempt at simplifying the representation of dbGaP's FHIR data to something
        more easily readable and more flat.

        Args:
            fhir_object (fhirclient.models.*): Any FHIR object

        Returns:
            dict: dictionary of all simplified data

        Raises:
            Exception: If an extension has more than 1 value attributes.
        """
        all_data = {}

        # special handling for FHIR "extensions", which are terms in the response
        # that are not part of the standard FHIR model (in other words, they are
        # custom fields that dbGaP has added)
        for extension in getattr(fhir_object, "extension", None) or []:
            for potential_value_type in dir(extension):
                if potential_value_type == "extension" and extension.extension:
                    ext_name = extension.url.split("/")[-1].split("-")[-1]
                    if not all_data.get(ext_name):
                        all_data[ext_name] = []

                    all_data[ext_name].append(
                        self.get_simplified_data_from_object(extension)
                    )

            for potential_value_type in dir(extension):
                found_value = False
                if potential_value_type.startswith("value") and getattr(
                    extension, potential_value_type, None
                ):
                    if found_value:
                        raise Exception(
                            f"Found 2 value attributes for extension: {extension.url}, can only "
                            f"handle one. First: {all_data[extension.url]} "
                            f"Second: {potential_value_type}: {getattr(extension, potential_value_type, None)}"
                        )
                    value = getattr(extension, potential_value_type, None)
                    # it's possible the value is another FHIR class, so try to represent it as json
                    try:
                        value = value.as_json()
                        if value.get("value"):
                            value = value["value"]
                        elif value.get("display"):
                            value = value["display"]
                        elif value.get("code"):
                            value = value["code"]
                    except AttributeError:
                        # assume we're dealing with a default Python type and continue
                        pass

                    ext_name = extension.url.split("/")[-1].split("-")[-1]
                    # if it's a Mapping, we weren't able to simplify the value,
                    # so just leave the existing entry in all_data
                    if not isinstance(value, collections.abc.Mapping):
                        if not all_data.get(ext_name):
                            all_data[ext_name] = []

                        all_data[ext_name].append(value)

                    found_value = True

        # use libraries built-in parsing for standard FHIR fields
        non_extension_data = fhir_object.as_json()

        # The library attempts to parse extensions, but we already handled
        # the dbGaP extension data above, so remove the raw info from the library
        # and replace with our parsing
        del non_extension_data["extension"]
        all_data.update(non_extension_data)

        # simplify and clean up everything
        simplified_data = dbgapFHIR._get_simplified_data(fhir_object)
        all_data.update(simplified_data)
        self._flatten_relevant_fields(all_data)
        self._remove_unecessary_fields(all_data)
        self._capitalize_top_level_keys(all_data)

        return all_data

    @staticmethod
    def _get_simplified_data(fhir_object):
        simplified_data = {}

        variables_to_simplify = set(
            [
                "category",
                "condition",
                "keyword",
                "focus",
                "identifier",
                "security",
                "sponsor",
                "security",
            ]
        )

        # per docs, .elementProperties() returns:
        #       Returns a list of tuples, one tuple for each property that should be
        #       serialized, as: ("name", "json_name", type, is_list, "of_many", not_optional)
        for item in fhir_object.elementProperties():
            variables_to_simplify.add(item[0])

        for var in variables_to_simplify:
            simplified_var = dbgapFHIR._get_simple_text_from_fhir_object(
                fhir_object=fhir_object, list_variable=var
            )
            if simplified_var:
                simplified_data[var] = simplified_var

        return simplified_data

    @staticmethod
    def _get_simple_text_from_fhir_object(fhir_object, list_variable):
        output = []
        if type(getattr(fhir_object, list_variable, None)) is list:
            for item in getattr(fhir_object, list_variable, None) or []:
                simple_representation = getattr(item, "display", {})
                simple_representation = simple_representation or getattr(
                    item, "value", {}
                )
                simple_representation = simple_representation or getattr(
                    item, "text", {}
                )
                simple_representation = simple_representation or getattr(
                    item, "reference", {}
                )
                simple_representation = (
                    simple_representation
                    or dbgapFHIR._get_simple_text_from_fhir_object(
                        getattr(item, "coding", []), "coding"
                    )
                )
                if simple_representation:
                    output.append(simple_representation)
        # ignore non lists unless it's a FHIR object we know how to parse
        elif type(getattr(fhir_object, list_variable, None)) == FHIRReference:
            simple_representation = getattr(fhir_object, list_variable, None).display
            if simple_representation:
                output.append(simple_representation)
        elif type(getattr(fhir_object, list_variable, None)) == Meta:
            # ignore meta info
            pass
        elif type(getattr(fhir_object, list_variable, None)) == Identifier:
            simple_representation = dbgapFHIR._get_simple_text_from_fhir_object(
                getattr(fhir_object, list_variable, None), "identifier"
            )
            if simple_representation:
                output.append(simple_representation)
        # elif ... add any more FHIR objects we can simplify and parse here
        else:
            logging.warning(f"could not parse FHIR object to simple text, ignoring")

        return output

    def _flatten_relevant_fields(self, dictionary):
        for item in dictionary.get("Content", []):
            for item in dictionary["Content"]:
                for key, value in item.items():
                    if (
                        isinstance(value, collections.abc.Iterable)
                        and not type(value) is str
                    ):
                        if len(value) > 1:
                            logging.warning(
                                f"Content field {key} contains more than 1 value: {value} "
                                "(which is unexpected), using 1st item ONLY."
                            )
                        dictionary[key] = str(value[0])

        try:
            del dictionary["Content"]
        except KeyError:
            pass

        if dictionary.get("StudyConsents", []):
            for item in dictionary["StudyConsents"]:
                consents = item.get("StudyConsent")
                if consents:
                    dictionary["StudyConsents"] = consents

        if dictionary.get("Citers", []):
            citers = []
            for item in dictionary.get("Citers", []):
                for citer in item.get("Citer", []):
                    """
                    ex:

                    {'Title': ['A novel variational Bayes multiple locus Z-statistic '
                           'for genome-wide association studies with Bayesian '
                           'model averaging'],
                    'Url': 'https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3381972'}
                    """
                    citer_sanitized = {
                        "Title": citer.get("Title", [""])[0],
                        "Url": citer.get("Url", [""]),
                    }
                    citers.append(citer_sanitized)
            dictionary["Citers"] = citers

        for item in self.suspected_single_item_list_fields:
            if dictionary.get(item) and len(dictionary.get(item, [])) == 1:
                dictionary[item] = dictionary[item][0]
            elif dictionary.get(item) and len(dictionary.get(item, [])) > 1:
                logging.warning(
                    f"expected {item} in dict to have single "
                    f"item list. it has more: {dictionary.get(item)}"
                )

    def _remove_unecessary_fields(self, all_data):
        # remove some fields deemed unnecessary or not typically populated
        for field in self.unnecessary_fields:
            try:
                del all_data[field]
            except KeyError:
                pass

    @staticmethod
    def _capitalize_top_level_keys(all_data):
        for key, value in copy.deepcopy(all_data).items():
            capitalized_key = key[:1].upper() + key[1:]
            all_data[capitalized_key] = all_data[key]

            if key != capitalized_key:
                del all_data[key]


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
