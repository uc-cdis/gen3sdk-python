import requests
import json
import csv
from cdislogging import get_logger

logging = get_logger("__name__")


class FHIRLoader(object):
    """
    Class to load study level metadata from a commons into a FHIR server.
    Extend this class and implement the convert_to_FHIR_object() method, which
    will specify how Gen3 Metadata is transformed into FHIR data for the subclass.

    Example subclass:

    from gen3.external import FHIRLoader
    class BDCFHIRLoader(FHIRLoader):
        def convert_to_FHIR_object(self, gen3_object):
            # define how a dict of BDC metadata is transformed into FHIR object here

    Example usage:

    bdc_loader = BDCFHIRLoader(endpoint, auth)
    other_loader = otherFHIRLoader(endpoint2, auth2)
    bdc_loader.load_metadata_into_fhir(BDC_metadata_file)
    other_loader.load_metadata_into_fhir(other_metadata_file)
    """

    def __init__(self, endpoint, auth_token):
        """
        Initiatilize loader
        """
        self.endpoint = endpoint
        self.auth_token = auth_token
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.auth_token}",
                "Content-Type": "application/fhir+json",
                "Accept": "application/fhir+json",
            }
        )

    def load_metadata_into_fhir(self, metadata_file):
        """
        Takes in a .tsv file of Gen3 metadata, converts into FHIR objects, then loads FHIR objects into a FHIR server
        """
        logging.info(f"Loading metadata from file: {metadata_file}")
        with open(metadata_file, "r") as gen3_metadata:
            reader = csv.DictReader(gen3_metadata, delimiter="\t")
            for gen3_object in reader:
                fhir_object = self.convert_to_FHIR_object(gen3_object)
                self.load_FHIR_object(fhir_object)

    def convert_to_FHIR_object(self, gen3_object):
        """
        Takes a Gen3 object (dict) and creates a FHIR object (dict) to return.
        Classes extending FHIRLoader should implement this to specify the how data should be transformed
        from Gen3 metadata to a FHIR object containing Gen3 metadata.
        Different FHIR servers may enforce different versions of the FHIR spec.
        """
        raise NotImplementedError()

    def load_FHIR_object(self, fhir_object):
        """
        Takes a fhir_object dict as an input to create / update.
        Default implementation assumes sending a PUT request to:

        {self.endpoint}/{fhir_object["resourceType"]}

        Assumes receiving a 200 or 201 status code is a success.
        Subclasses should override this method if needed.
        """
        logging.info(json.dumps(fhir_object))
        resource_endpoint = f'{self.endpoint}/{fhir_object["resourceType"]}'
        logging.info(f"Got resource endpoint: {resource_endpoint}")
        response = self.session.put(resource_endpoint, data=json.dumps(fhir_object))
        if response.status_code == 200 or response.status_code == 201:
            logging.info("Successfully PUT FHIR data.")
            logging.info(response.text)
        else:
            error_message = (
                f"Failed to PUT FHIR data. Status code: {response.status_code}. "
            )
            logging.error(error_message)
            logging.info(response.text)
            raise Exception(error_message)

    def delete_fhir_object(self, resource_type, resource_id):
        """
        Deletes a FHIR object given its resource type and ID.
        Default implementation assumes sending a DELETE request to:

        {self.endpoint}/{resource_type}/{resource_id}

        Assumes receiving a 200 or 201 status code is a success.
        Subclasses should override this method if needed.
        """
        delete_endpoint = f"{self.endpoint}/{resource_type}/{resource_id}"
        logging.info(f"Deleting FHIR Object at {delete_endpoint}")
        response = self.session.delete(delete_endpoint)
        if response.status_code == 200 or response.status_code == 204:
            logging.info(f"Successfully deleted {resource_type} with ID {resource_id}.")
            logging.info(response.text)
        else:
            error_message = f"Failed to delete {resource_type} with ID {resource_id}. Status code: {response.status_code}."
            logging.error(error_message)
            logging.error(response.text)
            raise Exception(error_message)
