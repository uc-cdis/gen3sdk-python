import requests

from cdislogging import get_logger

from gen3.external import ExternalMetadataSourceInterface
from gen3.external.nih.utils import get_dbgap_accession_as_parts
from gen3.utils import append_query_params

logging = get_logger("__name__")


class dbgapStudyMetadata(ExternalMetadataSourceInterface):
    """
    Class for interacting with National Center for
    Biotechnology Information (NCBI)'s database of Genotypes and Phenotypes (dbGaP)
    Study Metadata API. The Study Metadata API contains
    public information about studies that are registered in dbGaP with things like
    who the PI is, what is the funding institution, what child studies are there,
    etc.

    This API also enables WRITE with credentials for the study owners. We don't
    need this functionality, but that feature I think drove this API to begin
    with. The public ability to READ metadata is a side-effect.

    It appears that most if not all of this data mirrors what's in the dbGaP
    FHIR server, except this API supports past versions and FHIR does NOT
    (as of June 2024).

    Example Usage:

        ```
        from gen3.external.nih.dbgap_study_metadata import dbgapStudyMetadata


        def main():
            studies = [
                "phs000007.v32",
                "phs000179.v1.p1.c1",
            ]
            dbgap_study_metadata = dbgapStudyMetadata()
            metadata = dbgap_study_metadata.get_metadata_for_ids(ids=studies)
            print(metadata)


        if __name__ == "__main__":
            main()
        ```
    """

    def __init__(
        self,
        api="https://submit.ncbi.nlm.nih.gov/dbgap/api/v1/study_config/",
        auth_provider=None,
    ):
        super(dbgapStudyMetadata, self).__init__(api, auth_provider)

    def get_metadata_for_ids(self, ids):
        """
        Return dbGaP Study Metadata metadata for each of the provided IDs.

        Args:
            ids (List[str]): list of IDs to query for

        Returns:
            Dict[dict]: metadata for each of the provided IDs (which
                        are the keys in the returned dict)
        """
        logging.info("Getting dbGaP Study Metadata metadata...")
        logging.debug("Provided ids for dbGaP Study Metadata metadata: {ids}")

        metadata_for_ids = {}

        for phsid in ids:
            phsid_parts = get_dbgap_accession_as_parts(phsid)

            if not phsid_parts["version_number"]:
                logging.warning(
                    f"ID provided '{phsid}' does not specify a version. This will "
                    f"still return data without specifying the version. However, "
                    f"ensure the version returned is appropriate. The default "
                    f"version when not specified is usually the latest version in dbGaP."
                )

            request_url = self.api.rstrip("/") + (
                f"/phs{phsid_parts['phsid_number']}.v{phsid_parts['version_number']}"
            )

            logging.debug(f"GET: {request_url}")
            response = requests.get(request_url)
            logging.debug(f"Got full JSON: {response}")

            response_json = None
            try:
                response_json = response.json()
            except JSONDecodeError:
                print("Response could not be serialized")
                logging.error(
                    f"Could not get metadata for {phsid}. "
                    f"Response could not be serialized into JSON. "
                    "Cannot continue without this metadata. Skipping..."
                )
                continue

            study = response_json.get("data")
            if not study:
                error = response.json().get("error")
                logging.error(
                    f"Could not get metadata for {phsid}. "
                    f"Error from response: {error}. "
                    "Cannot continue without this metadata. Skipping..."
                )
                continue

            logging.debug(f"Got study JSON: {study}")

            metadata_for_ids[phsid] = study

        return metadata_for_ids
