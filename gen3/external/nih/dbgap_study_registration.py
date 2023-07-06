import requests

from cdislogging import get_logger
import xmltodict

from gen3.external import ExternalMetadataSourceInterface
from gen3.external.nih.utils import get_dbgap_accession_as_parts
from gen3.utils import append_query_params

logging = get_logger("__name__")


class dbgapStudyRegistration(ExternalMetadataSourceInterface):
    """
    Class for interacting with National Center for
    Biotechnology Information (NCBI)'s database of Genotypes and Phenotypes (dbGaP)
    Study Registration API. The Study Registration API contains
    public information about studies that are registered in dbGaP with things like
    who the PI is, what is the funding institution, what child studies are there,
    etc.

    Example Usage:

        ```
        from gen3.external.nih.dbgap_study_registration import dbgapStudyRegistration


        def main():
            studies = [
                "phs000007.v32",
                "phs000179.v1.p1.c1",
            ]
            dbgap_study_registration = dbgapStudyRegistration()
            metadata = dbgap_study_registration.get_metadata_for_ids(ids=studies)
            print(metadata)


        if __name__ == "__main__":
            main()
        ```
    """

    def __init__(
        self,
        api="https://dbgap.ncbi.nlm.nih.gov/ss/dbgapssws.cgi",
        auth_provider=None,
    ):
        super(dbgapStudyRegistration, self).__init__(api, auth_provider)

    def get_metadata_for_ids(self, ids):
        """
        Return dbGaP Study Registration metadata for each of the provided IDs.

        Args:
            ids (List[str]): list of IDs to query for

        Returns:
            Dict[dict]: metadata for each of the provided IDs (which
                        are the keys in the returned dict)
        """
        logging.info("Getting dbGaP Study Registration metadata...")
        logging.debug("Provided ids for dbGaP Study Registration metadata: {ids}")

        metadata_for_ids = {}

        for phsid in ids:
            standardized_phsid_parts = get_dbgap_accession_as_parts(phsid)

            if not standardized_phsid_parts["version_number"]:
                logging.warning(
                    f"ID provided '{phsid}' does not specify a version. This will "
                    f"still return data without specifying the version. However, "
                    f"ensure the version returned is appropriate. The default "
                    f"version when not specified is usually the latest version in dbGaP."
                )

            request_url = append_query_params(
                self.api,
                request="Study",
                phs=standardized_phsid_parts["phsid_number"],
            )
            if standardized_phsid_parts["version_number"]:
                request_url = append_query_params(
                    request_url,
                    v=standardized_phsid_parts["version_number"],
                )

            logging.debug(f"GET: {request_url}")
            raw_xml_response = requests.get(request_url)
            logging.debug(
                f"Got XML response: {raw_xml_response.text}, converting to JSON..."
            )

            # this library has an opinionated way to convert XML to JSON that
            # preserves attributes on tags by appending them with '@'.
            # The parsing follows this standard:
            #   https://www.xml.com/pub/a/2006/05/31/converting-between-xml-and-json.html
            try:
                response = xmltodict.parse(raw_xml_response.text)
            except Exception as exc:
                logging.error(
                    f"{phsid} has an invalid dbGaP Study Registration metadata "
                    "response. Cannot continue without it. Skipping..."
                )
                continue

            logging.debug(f"Got full JSON: {response}")

            study = response.get("dbgapss", {}).get("Study", [])
            if type(study) == list:
                if len(study) > 1:
                    logging.warning(
                        f"Multiple studies found from {request_url}, using first one ONLY..."
                    )
                    study = study[0]
                elif len(study) == 0:
                    logging.warning(f"No study found from {request_url}. Skipping...")
                    continue

            logging.debug(f"Got study JSON: {study}")

            metadata_for_ids[phsid] = study

        return metadata_for_ids
