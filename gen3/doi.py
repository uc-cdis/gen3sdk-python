"""
This is a lower-level DOI class for interacting with Datacite and actually
minting the DOIs.

For collecting DOI Metadata, other classes (outside of the ones in this module)
can interact with different APIs to gather the necessary metadata.
"""
import backoff
import requests
import os

from cdislogging import get_logger

from gen3.tools.metadata.discovery import get_discovery_metadata
from gen3.metadata import Gen3Metadata
from gen3.utils import (
    raise_for_status_and_print_error,
    is_status_code,
    DEFAULT_BACKOFF_SETTINGS,
)

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
            ), use_prod=True
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

        datacite.persist_doi_metadata_in_gen3(doi)

    if __name__ == "__main__":
        main()
    ```
    """

    DEFAULT_DOI_ACCESS_INFORMATION = (
        "The data identified by this DOI is available through Gen3. Please refer "
        "to other documentation for more details on access. Alternatively, use the contact "
        "information provided on this page to request such information."
    )
    DEFAULT_DOI_CONTACT_INFORMATION = (
        "Unavailable. Please refer to other links or "
        "documentation on this site to determine an effective contact."
    )
    DOI_RESOLVER = "https://doi.org"
    PRODUCTION_URL = "https://api.datacite.org"
    TEST_URL = "https://api.test.datacite.org"

    def __init__(
        self,
        api=None,
        auth_provider=None,
        use_prod=False,
    ):
        """
        Initiatilize.

        Args:
            api (str, optional): Override API with provided URL. Will ignore
                `use_prod` if this is provided
            auth_provider (Gen3Auth, optional): A Gen3Auth class instance or compatible interface
            use_prod (bool, optional): Default False. Whether or not to use the
                production Datacite URL.
        """
        self._auth_provider = auth_provider
        if api:
            self.api = api
        elif use_prod:
            self.api = DataCite.PRODUCTION_URL
        else:
            self.api = DataCite.TEST_URL

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create_doi(self, doi):
        """
        Create DOI provided via DataCite's API

        Args:
            doi (gen3.doi.DigitalObjectIdentifier): DOI to create
        """
        payload = doi.as_dict()
        headers = {
            "Content-type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }

        endpoint = self.api.rstrip("/") + "/dois"
        logging.info(f"POST-ing to {endpoint}...")
        logging.debug(f"Data payload to {endpoint}: {payload}")
        response = requests.post(
            endpoint, json=payload, auth=self._auth_provider, headers=headers
        )
        logging.debug(f"Response: {response.text}")
        raise_for_status_and_print_error(response)

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def read_doi(self, identifier):
        """
        Get DOI provided by DataCite's API.

        Args:
            identifier (str): DOI identifier to read
        """
        headers = {
            "Content-type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }

        endpoint = self.api.rstrip("/") + f"/dois/{identifier}"
        logging.info(f"GET-ing to {endpoint}...")
        response = requests.get(endpoint, auth=self._auth_provider, headers=headers)
        logging.debug(f"Response: {response.text}")

        # 404's should not raise exceptions, instead returning nothing
        if is_status_code(response, "404"):
            return None

        raise_for_status_and_print_error(response)

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def update_doi(self, doi):
        """
        Update DOI provided via DataCite's API

        Args:
            doi (gen3.doi.DigitalObjectIdentifier): DOI to update
        """
        payload = doi.as_dict()
        identifier = doi.identifier
        headers = {
            "Content-type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }

        endpoint = self.api.rstrip("/") + f"/dois/{identifier}"
        logging.info(f"PUT-ing to {endpoint}...")
        logging.debug(f"Data payload to {endpoint}: {payload}")
        response = requests.put(
            endpoint, json=payload, auth=self._auth_provider, headers=headers
        )
        logging.debug(f"Response: {response.text}")
        raise_for_status_and_print_error(response)

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def delete_doi(self, identifier):
        """
        Delete DOI provided via DataCite's API

        Args:
            identifier (str): DOI identifier to delete
        """
        headers = {
            "Content-type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }

        endpoint = self.api.rstrip("/") + f"/dois/{identifier}"
        logging.info(f"DELETE-ing to {endpoint}...")
        response = requests.delete(endpoint, auth=self._auth_provider, headers=headers)
        logging.debug(f"Response: {response.text}")
        raise_for_status_and_print_error(response)

        return response

    def persist_doi_metadata_in_gen3(
        self,
        guid,
        doi,
        auth,
        additional_metadata=None,
        prefix="",
    ):
        """
        Persist the DOI Metadata in Gen3. The default behavior is
        to use the Gen3 Discovery feature set, where there exists an entry
        for the DOI already in the Discovery Metadata under the `guid` provided.

        The DOI Metadata will be added under the existing `guid`'s `gen3_discovery`
        metadata block so that it is available to the Gen3 Discovery Page.

        Args:
            guid (str): globally unique ID for the metadata record
            doi (DigitalObjectIdentifier): DOI object with required metadata
            auth (TYPE): Description
            additional_metadata (Dict, optional): Any additional metadata fields
                to add to the DOI metadata into the Gen3 Metadata API.
                This can include overrides to the default landing page info
                for the following fields:
                    is_available (bool): True/False
                    disclaimer (str): Text to display at the top of the page
                        intended use is for tombstone pages, where the data is
                        no longer available and we need to note that
                    citation (str): By default: `Creator (PublicationYear).
                        Title. Version. Publisher. ResourceType. Identifier`
                    access_information (str): Text on how to access this data
                    access_information_link (str): HTTP link to additional info
                        about access
                    version_information (str): Text on any version information
                    contact (str): Contact information required for landing page
            prefix (str): prefix for the metadata fields to prepend to all DOI
                fields from `doi` AND all fields in `additional_metadata`
        """
        metadata_service = Gen3Metadata(auth_provider=auth)
        identifier = doi.identifier

        metadata = doi.as_gen3_metadata(prefix=prefix)

        additional_metadata = additional_metadata or {}

        is_available = additional_metadata.get("is_available", True)
        is_available_text = "Yes" if is_available else "No"
        metadata[prefix + "is_available"] = is_available_text

        if additional_metadata.get("is_available"):
            del additional_metadata["is_available"]

        metadata[prefix + "disclaimer"] = additional_metadata.get("disclaimer", "")
        if additional_metadata.get("disclaimer"):
            del additional_metadata["disclaimer"]

        metadata[prefix + "citation"] = additional_metadata.get("citation")
        if not metadata[prefix + "citation"]:
            if not doi.optional_fields.get("version"):
                logging.warning(
                    f"DOI {doi.identifier} is missing `version` in doi.optional_fields. "
                    f"Defaulting to `1` for citation."
                )
            # `Creator (PublicationYear). Title. Version. Publisher. ResourceType. Identifier`
            metadata[prefix + "citation"] = (
                f"{'& '.join([creator.get('name') for creator in doi.creators])} "
                f"({doi.publication_year}). "
                f"{'& '.join([titles.get('title') for titles in doi.titles])} "
                f"{str(doi.optional_fields.get('version', '1'))}. "
                f"{doi.publisher}. {doi.doi_type}. {doi.identifier}"
            )
        if additional_metadata.get("citation"):
            del additional_metadata["citation"]

        metadata[prefix + "version_information"] = additional_metadata.get(
            "version_information"
        )
        if not metadata[prefix + "version_information"]:
            if not doi.optional_fields.get("version"):
                logging.warning(
                    f"DOI {doi.identifier} is missing `version` in doi.optional_fields. "
                    f"Defaulting to `1`."
                )
            metadata[
                prefix + "version_information"
            ] = f"This is version {doi.optional_fields.get('version', '1')} of this {doi.doi_type}."
        if additional_metadata.get("version_information"):
            del additional_metadata["version_information"]

        metadata[prefix + "access_information"] = additional_metadata.get(
            "access_information"
        )
        if not metadata[prefix + "access_information"]:
            logging.warning(
                "No `access_information` provided in "
                "`additional_metadata` in doi.persist_metadata_in_gen3() call. "
                f"Consider providing this. Default will be used: {DataCite.DEFAULT_DOI_ACCESS_INFORMATION}"
            )
            metadata[
                prefix + "access_information"
            ] = DataCite.DEFAULT_DOI_ACCESS_INFORMATION
        if additional_metadata.get("access_information"):
            del additional_metadata["access_information"]

        metadata[prefix + "contact"] = additional_metadata.get("contact")
        if not metadata[prefix + "contact"]:
            logging.warning(
                "No `contact` provided in "
                "`additional_metadata` in doi.persist_metadata_in_gen3() call. "
                f"Consider providing this. Default will be used: {DataCite.DEFAULT_DOI_CONTACT_INFORMATION}"
            )
            metadata[prefix + "contact"] = DataCite.DEFAULT_DOI_CONTACT_INFORMATION
        if additional_metadata.get("contact"):
            del additional_metadata["contact"]

        # ensure prefix gets added to any additional metadata
        additional_metadata_with_prefix = {
            prefix + key.lstrip(prefix): value
            for key, value in additional_metadata.items()
        }

        metadata.update(additional_metadata_with_prefix)
        logging.debug(f"Metadata for {guid}: {metadata}")

        try:
            existing_record = metadata_service.get(guid=guid)

            # combine new metadata with existing metadata b/c MDS only supports
            # shallow merge at the moment
            existing_record.setdefault("gen3_discovery", {}).update(metadata)
            metadata = existing_record

            metadata_service.update(guid=guid, metadata=metadata, aliases=[identifier])

            logging.info(f"Updated existing Gen3 metadata record for {guid}.")
            logging.debug(
                f"Updated existing Gen3 metadata record for {guid}: {existing_record}"
            )
        except requests.exceptions.HTTPError as exc:
            if is_status_code(exc, "404"):
                logging.info(
                    f"Existing Gen3 Discovery metadata record with GUID "
                    f"`{guid}` not found. Creating..."
                )
                metadata = get_discovery_metadata(metadata)
                logging.debug(
                    f"Creating missing GUID `{guid}` with metadata: {metadata}."
                )
                metadata_service.create(
                    guid=guid, metadata=metadata, aliases=[identifier]
                )
            else:
                # non 404s should actually escalate
                raise

        return metadata


class DigitalObjectIdentifier(object):
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
              "url": "https://schema.datacite.org/meta/kernel-4.4/index.html",
              "schemaVersion": "http://datacite.org/schema/kernel-4"
            }
          }
        }
    """

    # to update to a later version, the classes that handle representing various
    # objects will need to change. Also it's possible other parts of this implementation
    # will need to change (including general types, how they should be representined in JSON, etc)
    # As such, this will remain "hard coded"
    SCHEMA_VERSION = "http://datacite.org/schema/kernel-4.4"

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
        "descriptions",
        "dates",
        "relatedIdentifiers",
        "geoLocations",
        "language",
        "alternateIdentifiers",
        "identifiers",
        "sizes",
        "formats",
        "version",
        "rightsList",
        "fundingReferences",
    ]

    DEFAULT_DOI_TYPE = "Dataset"

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
        root_url=None,
        event=None,
        _type="dois",
        **kwargs,
    ):
        """
        To create a DOI you must specify identifier OR prefix.

        Mandatory fields for registration are defaulted the init.

        Supply any additional reccomended/optional as keyword args

        Arg definitions summarized from the official schema for v4.4:
        https://schema.datacite.org/meta/kernel-4.4/doc/DataCite-MetadataKernel_v4.4.pdf

        Args:
            prefix (str, optional): DOI Prefix
            identifier (str, optional): a unique string that identifies the resource (the DOI)
                NOTE: `identifier` is NOT case sensitive but the `url` is. Different
                      Datacite API's tend to return the DOI either with the mixed cases
                      presented, fully lower, or fully upper (which adds to the confusion).
                      As such, anything provided/received will be forced UPPER_CASE
                      automatically to reduce confusion.
            creators (List[Dict], optional): the main
                researcher(s) involved in producing the data,
                or the author(s) of the publication.
            titles (List[Dict], optional): a name or titles by which a resource is known
                Format [{"title": "some title"}, ...]
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
            root_url (str, optional): the root url for the landing page, must be used
                in combination with `identifier` to form a final landing page url
            event (str, optional): what event to issue against the DOI
                Event field logic per DataCite API:
                    publish - Triggers a state move from draft or registered to findable
                    register - Triggers a state move from draft to registered
                    hide - Triggers a state move from findable to registered
            _type (str, optional): The type of thing to interact with in Datacite
        """
        self.prefix = prefix
        self.identifier = identifier or ""
        self.creators = creators or []
        self.titles = titles or []
        self.publisher = publisher
        self.publication_year = publication_year
        self.doi_type = doi_type or DigitalObjectIdentifier.DEFAULT_DOI_TYPE
        self.url = url
        self.root_url = root_url
        self.event = event
        self._type = _type

        # See NOTE: in docstring under `identifier`
        self.identifier = self.identifier.upper()

        # any additional kwargs get interpreted as optional fields
        self.optional_fields = {}

        for key, value in kwargs.items():
            if key in DigitalObjectIdentifier.OPTIONAL_FIELDS:
                self.optional_fields[key] = value
            else:
                logging.warning(
                    f"Skipping '{key}={value}' because '{key}' "
                    f"is not a supported optional DOI metadata field."
                )
                logging.debug(
                    f"Supported optional DOI metadata fields: "
                    f"{DigitalObjectIdentifier.OPTIONAL_FIELDS}"
                )

        if not (self.identifier or self.prefix):
            raise DataCiteDOIValidationError(
                "You must either specify a) `identifier` which is a full DOI with prefix "
                "or b) `prefix` (which will let Datacite autogenerate the rest of "
                "the ID. You can't leave both blank."
            )

        if self.root_url and self.url:
            logging.warning(
                "You provided both `root_url` and `url`. "
                f"Preferring `url` ({self.url}) and ignoring `root_url` ({self.root_url})."
            )
            self.root_url = None

        if self.root_url and self.prefix and not self.identifier:
            raise DataCiteDOIValidationError(
                "To use `root_url`, you MUST specify `identifier` (which is a "
                "full DOI with prefix). You CANNOT rely on `prefix` (which will "
                "let Datacite autogenerate the rest of the ID)."
            )

        if not self.url:
            if not self.root_url:
                raise DataCiteDOIValidationError(
                    "You must either specify a) `url` which is a full landing page url  "
                    "or b) `root_url` (which will automatically get appended with "
                    "the `identifier` to form final url). You can't leave both blank."
                )

            self.url = self._get_url_from_root()

        if doi_type and doi_type not in DigitalObjectIdentifier.RESOURCE_GENERAL_TYPES:
            logging.error(f"{doi_type} is not an accepted resourceTypeGeneral")
            raise DataCiteDOIValidationError(
                f"{doi_type} is not an accepted resourceTypeGeneral"
            )

    def _get_url_from_root(self):
        return self.root_url.rstrip("/") + f"/{self.identifier}/"

    def as_gen3_metadata(self, prefix=""):
        """
        Return a flat dictionary for ingestion into Gen3's Metadata Service

        Returns:
            Dict: flat dictionary for ingestion into Gen3's Metadata Service
        """
        data = {}

        if self.identifier:
            data[prefix + "identifier"] = self.identifier
            data[prefix + "resolveable_link"] = (
                DataCite.DOI_RESOLVER.rstrip("/") + "/" + self.identifier
            )
        if self.creators:
            data[prefix + "creators"] = " & ".join(
                [item["name"] for item in self.creators]
            )
        if self.titles:
            data[prefix + "titles"] = ", ".join([item["title"] for item in self.titles])
        if self.publisher:
            data[prefix + "publisher"] = self.publisher
        if self.publication_year:
            data[prefix + "publication_year"] = self.publication_year
        if self.doi_type:
            data[prefix + "resource_type"] = self.doi_type
        if self.url:
            data[prefix + "url"] = self.url

        for key, value in self.optional_fields.items():
            if key == "contributors":
                data[prefix + key] = " & ".join([item["name"] for item in value])
            elif key == "descriptions":
                data[prefix + key] = "\n\n".join(
                    [
                        item["descriptionType"] + ": " + item["description"]
                        for item in value
                    ]
                )
            elif key == "alternateIdentifiers":
                # sometimes this is a list and sometimes is an object depending
                # on which Datacite API you hit (PUT/POST/DELETE). Handle both
                if type(value) == list:
                    value = ", ".join(
                        [
                            item.get("alternateIdentifier")
                            + f" ({item.get('alternateIdentifierType')})"
                            for item in value
                        ]
                    )
                else:
                    value = (
                        value.get("alternateIdentifier")
                        + f" ({value.get('alternateIdentifierType')})"
                    )

                data[prefix + key] = value
            elif key == "identifiers":
                # sometimes this is a list and sometimes is an object depending
                # on which Datacite API you hit (PUT/POST/DELETE). Handle both
                if type(value) == list:
                    value = ", ".join(
                        [
                            item.get("identifier") + f" ({item.get('identifierType')})"
                            for item in value
                        ]
                    )
                else:
                    value = (
                        value.get("identifier") + f" ({value.get('identifierType')})"
                    )

                data[prefix + key] = value
            elif key == "fundingReferences":
                # sometimes this is a list and sometimes is an object depending
                # on which Datacite API you hit (PUT/POST/DELETE). Handle both
                if type(value) == list:
                    value = ", ".join([funder.get("funderName") for funder in value])
                else:
                    value = value.get("funderName")

                data[prefix + key] = value
            else:
                data[prefix + key] = value

        return data

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
            data["data"]["attributes"]["creators"] = [item for item in self.creators]
        if self.titles:
            data["data"]["attributes"]["titles"] = self.titles
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

        data["data"]["attributes"]["schemaVersion"] = self.SCHEMA_VERSION

        for key, value in self.optional_fields.items():
            data["data"]["attributes"][key] = value

        # per DataCite API's spec, if you don't specify a full DOI you need
        # to specify the prefix at least
        if not self.identifier:
            del data["data"]["attributes"]["doi"]
            del data["data"]["id"]
            data["data"]["attributes"]["prefix"] = self.prefix

        return data

    @staticmethod
    def from_datacite_create_doi_response(response):
        """
        Return a DigitalObjectIdentifier instance from the Response from a
        Datacite Create DOI API call.
        """
        raw_attributes = response.json().get("data", {}).get("attributes", {})
        attributes = {}

        # we only want the mandatory and optional fields we know how to handle,
        # exclude all other fields returned by the API
        for key, value in raw_attributes.items():
            if key in DigitalObjectIdentifier.OPTIONAL_FIELDS:
                attributes[key] = value

        return DigitalObjectIdentifier(
            prefix=raw_attributes.get("prefix"),
            # Why .upper()? See NOTE: in docstring under `identifier`
            identifier=raw_attributes.get("doi").upper(),
            creators=raw_attributes.get("creators"),
            titles=raw_attributes.get("titles"),
            publisher=raw_attributes.get("publisher"),
            publication_year=raw_attributes.get("publicationYear"),
            url=raw_attributes.get("url"),
            **attributes,
        )


class DigitalObjectIdentifierCreator(object):
    """
    Class to help with representing a Creator object for the DataCite API
    per their schema.

    NOTE: If you use anything other than the schema this was built for (v4.4),
          this logic will need to be updated.

    From the DataCite Metadata Schema V 4.4:

    * 20.2 Creator 0-n
        The institution or person
        responsible for creating the
        related resource
        To supply multiple creators,
        repeat this property.

    * 20.2.1 creatorName 1
        The full name of the related
        item creator

        Examples: Charpy, Antoine;
        Jemison, Mae; Foo Data
        Center Note: The personal
        name, format should be:
        family, given. Non- roman
        names may be transliterated
        according to the ALA-LC
        schemas12.

    * 20.2.1.a nameType 0-1
        The type of name Controlled List Values:
        Organizational
        Personal (default)

    * 20.2.2 givenName 0-1
        The personal or first name of
        the creator
        Examples based on the 20.11.1 names: Antoine; Mae

    * 20.2.3 familyName 0-1
        The surname or last name of
        the creator

        Examples based on the 2.1
        names: Charpy; Jemison
    """

    NAME_TYPE_PERSON = "Personal"
    NAME_TYPE_ORGANIZATION = "Organizational"

    def __init__(
        self,
        name,
        name_type=NAME_TYPE_PERSON,
        given_name=None,
        family_name=None,
    ):
        self.name = name

        allowed_name_types = [
            DigitalObjectIdentifierCreator.NAME_TYPE_PERSON,
            DigitalObjectIdentifierCreator.NAME_TYPE_ORGANIZATION,
        ]
        if name_type not in allowed_name_types:
            logging.error(
                f"Provided name_type '{name_type}' is NOT in {allowed_name_types}. "
                f"Defaulting to '{DigitalObjectIdentifierCreator.NAME_TYPE_PERSON}'."
            )
            name_type = DigitalObjectIdentifierCreator.NAME_TYPE_PERSON

        self.name_type = name_type
        self.given_name = given_name
        self.family_name = family_name

    def as_dict(self):
        data = {
            "name": self.name,
            "nameType": self.name_type,
        }

        if self.given_name:
            data.update({"givenName": self.given_name})

        if self.family_name:
            data.update({"familyName": self.family_name})

        if (
            self.given_name
            and self.family_name
            and self.name_type == DigitalObjectIdentifierCreator.NAME_TYPE_PERSON
        ):
            data.update({"name": f"{self.family_name}, {self.given_name}"})

        return data


class DigitalObjectIdentifierFundingReference(object):
    """
    This could theoretically support more options, but at the point of writing,
    they weren't deemed necessary. Leaving this as an isolated class in case
    we want to expand in the future.

    From the DataCite Metadata Schema V 4.4:

    * 19 FundingReference 0-n
        Information about financial
        support (funding) for the
        resource being registered
        It is a best practice to supply
        funding information when
        financial support has been
        received.

    * 19.1 funderName 1
        Name of the funding provider Example: Gordon and Betty
        Moore Foundation

    * 19.2 funderIdentifier 0-1
        Uniquely identifies a funding
        entity, according to various
        types.
        Example:
        https://doi.org/10.13039/100
        000936

    * 19.2.a funderIdentifierType 0-1
        The type of the funderIdentifier Controlled List Values:
            GRID
            ISNI
            ROR
            Other
    """

    def __init__(
        self,
        name,
    ):
        self.name = name

    def as_dict(self):
        data = {
            "funderName": f"{self.name}",
        }

        return data


class DigitalObjectIdentifierTitle(object):
    """
    This could theoretically support more options, but at the point of writing,
    they weren't deemed necessary. Leaving this as an isolated class in case
    we want to expand in the future.

    From the DataCite Metadata Schema V 4.4:
    * 3 Title 1-n
        A name or title by which a
        resource is known. May be
        the title of a dataset or the
        name of a piece of software.
        Free text.

    * 3.a titleType 0-1
        The type of Title (other than
        the Main Title)
        Controlled List Values:
        AlternativeTitle
        Subtitle
        TranslatedTitle
        Other
    """

    def __init__(
        self,
        title,
    ):
        self.title = title

    def as_dict(self):
        data = {
            "title": f"{self.title}",
        }

        return data


class DigitalObjectIdentifierDescription(object):
    """
    From the DataCite Metadata Schema V 4.4:
    * 17 Description 0-n
        All additional information that
        does not fit in any of the other
        categories. May be used for
        technical information.
        Free text
        ***
        It is a best practice to supply a
        description.

    * 17.a descriptionType 1
        The type of the Description If Description is used,
        descriptionType is mandatory.

        Controlled List Values:
        Abstract
        Methods
        SeriesInformation
        TableOfContents
        TechnicalInfo
        Other

        Note: SeriesInformation as a
        container for series title,
        volume, issue, page number,
        and related fields, is now
        superseded by the new
        RelatedItem property
    """

    TYPE_ABSTRACT = "Abstract"
    TYPE_METHODS = "Methods"
    TYPE_SERIES_INFORMATION = "SeriesInformation"
    TYPE_TABLE_OF_CONTENTS = "TableOfContents"
    TYPE_TECHNICAL_INFO = "TechnicalInfo"
    TYPE_OTHER = "Other"

    def __init__(self, description, description_type=TYPE_ABSTRACT):
        self.description = description

        allowed_types = [
            DigitalObjectIdentifierDescription.TYPE_ABSTRACT,
            DigitalObjectIdentifierDescription.TYPE_METHODS,
            DigitalObjectIdentifierDescription.TYPE_SERIES_INFORMATION,
            DigitalObjectIdentifierDescription.TYPE_TABLE_OF_CONTENTS,
            DigitalObjectIdentifierDescription.TYPE_TECHNICAL_INFO,
            DigitalObjectIdentifierDescription.TYPE_OTHER,
        ]
        if description_type not in allowed_types:
            logging.error(
                f"Provided description_type '{description_type}' is NOT in {allowed_types}. "
                f"Defaulting to '{DigitalObjectIdentifierDescription.TYPE_ABSTRACT}'."
            )
            description_type = DigitalObjectIdentifierDescription.NAME_TYPE_PERSON

        self.description_type = description_type

    def as_dict(self):
        data = {
            "description": f"{self.description}",
            "descriptionType": f"{self.description_type}",
        }

        return data


class DigitalObjectIdenfitierAlternateID(object):
    """
    From the DataCite Metadata Schema V 4.4:

    11 alternateIdentifier 0-n An identifier other than the
      primary Identifier applied to the
      resource being registered. This
      may be any alphanumeric string
      which is unique within its
      domain of issue. May be used for
      local identifiers. The
      AlternateIdentifier should be an
      additional identifier for the same
      instance of the resource (i.e.,
      same location, same file).
      Free text
      ***
      Example:
      E-GEOD-34814

    11.a alternateIdentifierType
      1 The type of the
      AlternateIdentifier
      Free text
      ***
      If alternateIdentifier is used,
      alternateIdentifierType is
      mandatory. For the above
      example, the
      alternateIdentifierType would
      be "A local accession number"
    """

    def __init__(self, alternate_id, alternate_id_type):
        self.alternate_id = alternate_id
        self.alternate_id_type = alternate_id_type

    def as_dict(self):
        data = {
            "alternateIdentifier": f"{self.alternate_id}",
            "alternateIdentifierType": f"{self.alternate_id_type}",
        }

        return data
