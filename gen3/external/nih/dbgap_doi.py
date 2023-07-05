from cdislogging import get_logger

from gen3.doi import (
    DigitalObjectIdentifierCreator,
    DigitalObjectIdentifierFundingReference,
    DigitalObjectIdentifierDescription,
    DigitalObjectIdentifierTitle,
    DigitalObjectIdenfitierAlternateID,
)
from gen3.external import ExternalMetadataSourceInterface
from gen3.external.nih.dbgap_study_registration import dbgapStudyRegistration
from gen3.external.nih.dbgap_fhir import dbgapFHIR
from gen3.external.nih.utils import get_dbgap_accession_as_parts

logging = get_logger("__name__")


class dbgapDOI(ExternalMetadataSourceInterface):
    """
    Class for interacting with various National Center for
    Biotechnology Information (NCBI)'s database of Genotypes and Phenotypes (dbGaP)
    APIs to get necessary information for a DOI.

    Example Usage:

        ```
        from gen3.external.nih.dbgap_doi import dbgapDOI


        def main():
            studies = [
                "phs000007.v32",
                "phs000179.v1.p1.c1",
            ]

            dbgap_doi = dbgapDOI(publisher="Gen3 Foobar Instance")
            all_doi_data = dbgap_doi.get_metadata_for_ids(ids=studies)

            print(metadata)


        if __name__ == "__main__":
            main()
        ```

    See doi.py module for more information on how to use this DOI metadata to
    actually interact with Datacite's API and mint DOIs.

    """

    def __init__(
        self,
        publisher,
    ):
        """
        Initiatilize the class for interacting with dbgap APIs to get
        DOI information.

        Args:
            publisher (str): The DOI Publisher that should be added as the
                'Publisher' in the DOI Metadata. Per the Datacite docs, this is:
                    "The name of the entity that holds, archives, publishes prints,
                    distributes, releases, issues, or produces the resource. This
                    property will be used to formulate the citation, so consider the
                    prominence of the role. For software, use Publisher for the code
                    repository. If there is an entity other than a code repository,
                    that "holds, archives, publishes, prints, distributes, releases,
                    issues, or produces" the code, use the property
                    Contributor/contributorType/ hostingInstitution for the code
                    repository."
        """
        self.publisher = publisher

    def get_metadata_for_ids(self, ids):
        """
        Return DOI metadata for each of the provided IDs.

        Args:
            ids (List[str]): list of IDs to query for

        Returns:
            Dict[dict]: metadata for each of the provided IDs (which
                        are the keys in the returned dict)
        """
        logging.info("Getting dbGaP DOI metadata...")
        logging.debug(f"Provided ids for dbGaP DOI metadata: {ids}")

        dbgap_study_registration = dbgapStudyRegistration()
        dbgap_fhir = dbgapFHIR()

        dbgap_study_registration_metadata = (
            dbgap_study_registration.get_metadata_for_ids(ids)
        )
        dbgap_fhir_metadata = dbgap_fhir.get_metadata_for_ids(ids)

        all_doi_metadata = {}

        for phsid in ids:
            if not dbgap_fhir_metadata.get(phsid):
                logging.error(
                    f"{phsid} is missing dbGaP FHIR metadata. Cannot "
                    f"continue creating a DOI without it. Skipping..."
                )
                continue

            if not dbgap_study_registration_metadata.get(phsid):
                logging.error(
                    f"{phsid} is missing dbGaP Study Registration "
                    f"metadata. Cannot continue creating a DOI without it. Skipping..."
                )
                continue

            doi_metadata = {}

            # 1) required fields
            doi_metadata["creators"] = dbgapDOI._get_doi_creators(
                phsid, dbgap_study_registration_metadata
            )
            doi_metadata["titles"] = dbgapDOI._get_doi_title(phsid, dbgap_fhir_metadata)
            doi_metadata["publication_year"] = dbgapDOI._get_doi_publication_year(
                phsid, dbgap_fhir_metadata
            )
            doi_metadata["doi_type"] = "Dataset"

            # publisher is provided
            doi_metadata["publisher"] = self.publisher

            # NOTE: This does NOT include the required landing page URL
            #       b/c this requires the final ID (which should be generated
            #       elsewhere).
            # doi_metadata["url"] = None

            # 2) optional fields
            doi_metadata["version"] = dbgapDOI._get_doi_version(
                phsid, dbgap_fhir_metadata
            )
            doi_metadata["contributors"] = dbgapDOI._get_doi_contributors(
                phsid, dbgap_study_registration_metadata, dbgap_fhir_metadata
            )
            doi_metadata["descriptions"] = dbgapDOI._get_doi_descriptions(
                phsid, dbgap_fhir_metadata
            )
            doi_metadata[
                "alternateIdentifiers"
            ] = dbgapDOI._get_doi_alternate_identifiers(phsid, dbgap_fhir_metadata)
            doi_metadata["fundingReferences"] = dbgapDOI._get_doi_funding(
                phsid, dbgap_fhir_metadata
            )

            all_doi_metadata[phsid] = doi_metadata

        return all_doi_metadata

    @staticmethod
    def _get_doi_creators(phsid, dbgap_study_registration_metadata):
        result = []

        persons = (
            dbgap_study_registration_metadata.get(phsid, {})
            .get("Authority", {})
            .get("Persons", {})
            .get("Person", [])
        )
        for person in persons:
            # dbgap's API is inconsistent. Sometimes this is represented
            # like this: {'@allow_direct_access': 'true', '#text': 'PI'} and sometimes
            # it's just a string like "PI". This handles both cases to determine
            # the PI
            if person.get("Role") == "PI" or (
                "#text" in person.get("Role")
                and person.get("Role").get("#text") == "PI"
            ):
                creator = DigitalObjectIdentifierCreator(
                    name=person.get("@lname", "") + ", " + person.get("@fname", ""),
                    name_type=DigitalObjectIdentifierCreator.NAME_TYPE_PERSON,
                    given_name=person.get("@fname", ""),
                    family_name=person.get("@lname", ""),
                )
                result.append(creator.as_dict())

        return result

    @staticmethod
    def _get_doi_title(phsid, dbgap_fhir_metadata):
        result = []

        title = DigitalObjectIdentifierTitle(
            dbgap_fhir_metadata.get(phsid, {}).get("Title")
        )

        result.append(title.as_dict())

        return result

    @staticmethod
    def _get_doi_publication_year(phsid, dbgap_fhir_metadata):
        date = dbgap_fhir_metadata.get(phsid, {}).get("ReleaseDate")

        if not date:
            logging.debug(f"dbgap_fhir_metadata: {dbgap_fhir_metadata}")
            raise Exception(
                f"ReleaseDate from dbgap FHIR does not match expected pattern "
                f"YYYY-MM-DD: '{date}'. Unable to parse."
            )

        if date:
            date = date.split("-")[0]

            if len(date) != 4:
                logging.debug(f"dbgap_fhir_metadata: {dbgap_fhir_metadata}")
                raise Exception(
                    f"ReleaseDate from dbgap FHIR does not match expected pattern "
                    f"YYYY-MM-DD: '{date}'. Unable to parse."
                )

        return date

    @staticmethod
    def _get_doi_contributors(
        phsid, dbgap_study_registration_metadata, dbgap_fhir_metadata
    ):
        result = []

        persons = (
            dbgap_study_registration_metadata.get(phsid, {})
            .get("Authority", {})
            .get("Persons", {})
            .get("Person", [])
        )
        for person in persons:
            # dbgap's API is inconsistent. Sometimes this is represented
            # like this: {'@allow_direct_access': 'true', '#text': 'PI'} and sometimes
            # it's just a string like "PI". This handles both cases to determine
            # the PI and PI_ASSIST persons
            if (
                person.get("Role") == "PI"
                or person.get("Role") == "PI_ASSIST"
                or (
                    "#text" in person.get("Role")
                    and (
                        person.get("Role").get("#text") == "PI"
                        or person.get("Role").get("#text") == "PI_ASSIST"
                    )
                )
            ):
                creator = DigitalObjectIdentifierCreator(
                    name=person.get("@lname", "") + ", " + person.get("@fname", ""),
                    name_type=DigitalObjectIdentifierCreator.NAME_TYPE_PERSON,
                    given_name=person.get("@fname", ""),
                    family_name=person.get("@lname", ""),
                )
                result.append(creator.as_dict())

                if person.get("Organization"):
                    creator = DigitalObjectIdentifierCreator(
                        name=person.get("Organization"),
                        name_type=DigitalObjectIdentifierCreator.NAME_TYPE_ORGANIZATION,
                    )
                    result.append(creator.as_dict())

        return result

    @staticmethod
    def _get_doi_version(phsid, dbgap_fhir_metadata):
        version = 1
        ids = dbgap_fhir_metadata.get(phsid, {}).get("Identifier")
        for phsid in ids:
            standardized_phsid_parts = get_dbgap_accession_as_parts(phsid)

            # if there is a phsid parsed, then this Identifier matches
            # the pattern of the full phsid, so we can parse the version out
            if standardized_phsid_parts["phsid"]:
                version = standardized_phsid_parts["version_number"]

        return version

    @staticmethod
    def _get_doi_descriptions(phsid, dbgap_fhir_metadata):
        result = []

        description = DigitalObjectIdentifierDescription(
            description=dbgap_fhir_metadata.get(phsid, {}).get("Description")
        )
        result.append(description.as_dict())

        return result

    @staticmethod
    def _get_doi_alternate_identifiers(phsid, dbgap_fhir_metadata):
        result = []

        for alternate_id in dbgap_fhir_metadata.get(phsid, {}).get("Identifier"):
            alternate_id_obj = DigitalObjectIdenfitierAlternateID(
                alternate_id=alternate_id, alternate_id_type="dbGaP"
            )
            result.append(alternate_id_obj.as_dict())

        return result

    @staticmethod
    def _get_doi_funding(phsid, dbgap_fhir_metadata):
        return DigitalObjectIdentifierFundingReference(
            name=dbgap_fhir_metadata.get(phsid, {}).get("Sponsor")
        ).as_dict()
