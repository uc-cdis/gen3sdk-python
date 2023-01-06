import requests
import os
from gen3.utils import raise_for_status_and_print_error
from cdislogging import get_logger

logging = get_logger("__name__")


class DataCiteDOIValidationError(Exception):
    pass


class DataCite(object):
    """
    For interacting with DataCite's API.
    https://support.datacite.org/reference/introduction

    Example usage:

    ```
    import os
    from requests.auth import HTTPBasicAuth
    import sys

    from gen3.doi import DataCite, DigitalObjectIdentifer

    PREFIX = "10.12345"

    def main():
        datacite = DataCite(
            auth_provider=HTTPBasicAuth(
                os.environ["DATACITE_USERNAME"], os.environ["DATACITE_PASSWORD"]
            )
        )

        doi = DigitalObjectIdentifer(
            prefix=PREFIX,
            # identifier=f"{PREFIX}/test1",
            creators=["test"],
            titles=["test"],
            publisher="test",
            publication_year=2023,
            doi_type="Dataset",
            url="https://example.com",
            version="0.1",
            descriptions=[{"description": "this is a test resource"}],
            foobar="test",
        )

        datacite.create_doi(doi)


    if __name__ == "__main__":
        main()
    ```
    """

    def __init__(self, api="https://api.test.datacite.org", auth_provider=None):
        self._auth_provider = auth_provider
        self.api = api

    def create_doi(self, doi):
        """
        Create DOI provided via DataCite's API

        Args:
            doi (gen3.doi.DigitalObjectIdentifer): DOI to create
        """
        payload = doi.as_dict()
        headers = {
            "Content-type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }

        endpoint = self.api.rstrip("/") + "/dois"
        logging.info(f"POST-ing to {endpoint}...")
        logging.info(f"Data payload to {endpoint}: {payload}")
        response = requests.post(
            endpoint, json=payload, auth=self._auth_provider, headers=headers
        )
        logging.info(f"{response.text}")
        raise_for_status_and_print_error(response)
        return response


class DigitalObjectIdentifer(object):
    """
    Object representation of a DOI (including it's metadata).
    Convert to a DataCite-compatible payload by using `as_dict()`

    Example return:
        {
          "data": {
            "id": "10.5438/0012",
            "type": "dois",
            "attributes": {
              "event": "publish",
              "doi": "10.5438/0012",
              "creators": [{
                "name": "DataCite Metadata Working Group"
              }],
              "titles": [{
                "title": "DataCite Metadata Schema Documentation for the Publication and Citation of Research Data v4.0"
              }],
              "publisher": "DataCite e.V.",
              "publicationYear": 2016,
              "types": {
                "resourceTypeGeneral": "Text"
              },
              "url": "https://schema.datacite.org/meta/kernel-4.0/index.html",
              "schemaVersion": "http://datacite.org/schema/kernel-4"
            }
          }
        }
    """

    RESOURCE_GENERAL_TYPES = [
        "Audiovisual",
        "Book",
        "BookChapter",
        "Collection",
        "ComputationalNotebook",
        "ConferencePaper",
        "ConferenceProceeding",
        "DataPaper",
        "Dataset",
        "Dissertation",
        "Event",
        "Image",
        "InteractiveResource",
        "Journal",
        "JournalArticle",
        "Model",
        "OutputManagementPlan",
        "PeerReview",
        "PhysicalObject",
        "Preprint",
        "Report",
        "Service",
        "Software",
        "Sound",
        "Standard",
        "Text",
        "Workflow",
        "Other",
    ]

    OPTIONAL_FIELDS = [
        "subjects",
        "contributors",
        "dates",
        "relatedIdentifiers",
        "descriptions",
        "geoLocations",
        "language",
        "identifiers",
        "sizes",
        "formats",
        "version",
        "rightsList",
        "fundingReferences",
    ]

    def __init__(
        self,
        prefix=None,
        identifier=None,
        creators=None,
        titles=None,
        publisher=None,
        publication_year=None,
        doi_type=None,
        url=None,
        event=None,
        schema_version="http://datacite.org/schema/kernel-4",
        _type="dois",
        **kwargs,
    ):
        """
        To create a DOI you must specify identifier OR prefix.

        Mandatory fields for registration are defaulted the init.

        Supply any additional reccomended/optional as keyword args

        Event field logic per DataCite API:
            publish - Triggers a state move from draft or registered to findable
            register - Triggers a state move from draft to registered
            hide - Triggers a state move from findable to registered

        Arg definitions summarized from the official schema for v4.4:
        https://schema.datacite.org/meta/kernel-4.4/doc/DataCite-MetadataKernel_v4.4.pdf

        Args:
            prefix (str, optional): DOI Prefix
            identifier (str, optional): a unique string that identifies the resource (the DOI)
            creators (List[str], optional): the main researcher(s) involved in producing the data,
                or the author(s) of the publication
            titles (List[str], optional): a name or title by which a resource is known
            publisher (str, optional): The name of the entity that holds, archives, publishes
                prints, distributes, releases, issues, or produces the resource.
                This property will be used to formulate the citation, so consider the
                prominence of the role. For software, use Publisher for the code
                repository. If there is an entity other than a code repository,
                that "holds, archives, publishes, prints, distributes, releases,
                issues, or produces" the code, use the property
                Contributor/contributorType/ hostingInstitution for the code repository.
            publication_year (int, optional): the year the data or resource was or will be made
                publicly available
            doi_type (str, optional): a description of the resource (free-format text)
            url (str, optional): the web address of the landing page for the resource
            schema_version (str, optional): TODO
            event (str, optional): TODO
            _type (str, optional): TODO
        """
        self.prefix = prefix
        self.identifier = identifier
        self.creators = creators or []
        self.titles = titles or []
        self.publisher = publisher
        self.publication_year = publication_year
        self.doi_type = doi_type
        self.url = url
        self.schema_version = schema_version
        self.event = event
        self._type = _type

        # any additional kwargs get interpretted as optional fields
        # TODO improve handling of object fields like `descriptions`
        self.optional_fields = {}

        for key, value in kwargs.items():
            if key in DigitalObjectIdentifer.OPTIONAL_FIELDS:
                self.optional_fields[key] = value
            else:
                logging.warning(
                    f"Skipping '{key}={value}' because '{key}' "
                    f"is not a supported optional DOI metadata field."
                )
                logging.debug(
                    f"Supported optional DOI metadata fields: "
                    f"{DigitalObjectIdentifer.OPTIONAL_FIELDS}"
                )

        if not (self.identifier or self.prefix):
            raise DataCiteDOIValidationError(
                "You must either specify a) `identifier` which is a full DOI with prefix "
                "or b) `prefix` (which will let Datacite autogenerate the rest of "
                "the ID. You can't leave both blank."
            )

        if doi_type and doi_type not in DigitalObjectIdentifer.RESOURCE_GENERAL_TYPES:
            logging.error(f"{doi_type} is not an accepted resourceTypeGeneral")
            raise DataCiteDOIValidationError(
                f"{doi_type} is not an accepted resourceTypeGeneral"
            )

    def as_dict(self):
        data = {
            "data": {
                "id": self.identifier,
                "type": self._type,
                "attributes": {
                    "doi": self.identifier,
                },
            }
        }

        if self.creators:
            data["data"]["attributes"]["creators"] = [
                {"name": item} for item in self.creators
            ]
        if self.titles:
            data["data"]["attributes"]["titles"] = [
                {"name": item} for item in self.titles
            ]
        if self.publisher:
            data["data"]["attributes"]["publisher"] = self.publisher
        if self.publication_year:
            data["data"]["attributes"]["publicationYear"] = self.publication_year
        if self.doi_type:
            data["data"]["attributes"]["types"] = {"resourceTypeGeneral": self.doi_type}
        if self.url:
            data["data"]["attributes"]["url"] = self.url
        if self.event:
            data["data"]["attributes"]["event"] = self.event
        if self.schema_version:
            data["data"]["attributes"]["schemaVersion"] = self.schema_version

        for key, value in self.optional_fields.items():
            data["data"]["attributes"][key] = value

        # per DataCite API's spec, if you don't specify a full DOI you need
        # to specify the prefix at least
        if not self.identifier:
            del data["data"]["attributes"]["doi"]
            del data["data"]["id"]
            data["data"]["attributes"]["prefix"] = self.prefix

        return data
