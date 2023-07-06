"""
Classes and functions for interacting with Gen3's Discovery Metadata and configured
external sources to obtain DOI metadata and mint real DOIs using Datacite's API.
"""

import csv

from cdislogging import get_logger

from gen3.doi import DataCite, DigitalObjectIdentifier
from gen3.external.nih.dbgap_doi import dbgapDOI
from gen3.tools.metadata.discovery import (
    BASE_CSV_PARSER_SETTINGS,
    output_expanded_discovery_metadata,
)
from gen3.utils import (
    get_delimiter_from_extension,
    get_or_create_event_loop_for_thread,
    is_status_code,
)

logging = get_logger("__name__")


class GetMetadataInterface(object):
    """
    Abstract base class acting as interface for customized metadata retrievers.

    This is intended to be used to create an instance for use in the
    `mint_dois_for_discovery_datasets` function. See /docs for examples on
    Discovery DOI Metadata usage.

    To use, implement a new class and inherit from this and implement the single
    `get_doi_metadata` function. See `DbgapMetadataInterface` for an example.
    """

    def __init__(
        self,
        doi_publisher,
        current_discovery_alternate_id_to_guid,
        all_discovery_metadata,
        exclude_datasets=None,
    ):
        """
        `current_discovery_alternate_id_to_guid` should be populated already with the
        `metadata_field_for_alternate_id` value as the keys and the Metadata GUID
        as the values. In other words, it should map the proposed DOI to the
        existing metadata GUID.

        Args:
            doi_publisher (str): The DOI Publisher that should be added as the
                'Publisher' in the DOI Metadata.
            current_discovery_alternate_id_to_guid (Dict): Mapping of the proposed DOI to the
                existing metadata GUID.
            all_discovery_metadata (Dict): all the current discovery metadata
                as a dictionary with metadata GUID as the key.
            exclude_datasets (list[str], optional): List of datasets to ignore from Discovery Metadata
                (don't attempt to read DOI Metadata or mint DOIs for these). The strings in the
                list can be either the alternate IDs (from the specified `metadata_field_for_alternate_id`)
                or they can be the Metadata GUIDs (or a mix).
        """
        self.doi_publisher = doi_publisher
        self.current_discovery_alternate_id_to_guid = (
            current_discovery_alternate_id_to_guid
        )
        self.all_discovery_metadata = all_discovery_metadata
        self.exclude_datasets = exclude_datasets or []

        if self.exclude_datasets:
            logging.debug(f"Excluding datasets: {exclude_datasets}")

        is_valid = True
        if not self.doi_publisher:
            logging.error("`doi_publisher` was not provided and is required")
            is_valid = False

        if not self.current_discovery_alternate_id_to_guid:
            logging.error(
                "`current_discovery_alternate_id_to_guid` was not provided and is required"
            )
            is_valid = False

        if not self.all_discovery_metadata:
            logging.error("`all_discovery_metadata` was not provided and is required")
            is_valid = False

        if not is_valid:
            raise Exception(
                "Unable to initialize object without provided fields. "
                "Please see previous logged errors"
            )

        super(GetMetadataInterface, self).__init__()

    def get_doi_metadata(self):
        """
        Return DOI metadata for each of the DOIs listed in `current_discovery_alternate_id_to_guid`
        (optionally utilize the existing Discovery Metadata GUIDs and/or other
        Discovery Metadata in `all_discovery_metadata` to make the necessary API
        calls to get the DOI metadata).

        Returns:
                Dict[dict]: metadata for each of the provided IDs (which
                            are the keys in the returned dict)
        """
        raise NotImplementedError()


class DbgapMetadataInterface(GetMetadataInterface):
    def get_doi_metadata(self):
        """
        Return dbGaP metadata.
        """
        studies = [
            study
            for study, guid in self.current_discovery_alternate_id_to_guid.items()
            if study not in self.exclude_datasets and guid not in self.exclude_datasets
        ]

        # there's a separate DOI class for dbGaP as a wrapper around various
        # different APIs we need to call. Whereas if there are API sources that have
        # all available data, you could write a more generic SomeClass.as_gen3_metadata()
        #
        # NOTE: The final DOI does NOT include the required field `url` (because generating
        #       that requires knowledge of the final DOI identifier).
        #       So if you're using this, you need to ensure the DigitalObjectIdentifier
        #       object you create with this metadata is supplied with the right landing page URL (or
        #       you can ask it to generate one based on a root url provided).
        dbgap_doi = dbgapDOI(publisher=self.doi_publisher)

        return dbgap_doi.get_metadata_for_ids(ids=studies)


def mint_dois_for_discovery_datasets(
    gen3_auth,
    datacite_auth,
    get_doi_identifier_function,
    metadata_interface,
    doi_publisher,
    commons_discovery_page,
    doi_disclaimer,
    doi_access_information,
    doi_access_information_link,
    doi_contact,
    exclude_datasets=None,
    metadata_field_for_alternate_id="guid",
    use_doi_in_landing_page_url=False,
    doi_metadata_field_prefix="doi_",
    datacite_use_prod=False,
    datacite_api=None,
    publish_dois=False,
):
    """
    Get Discovery Page records, obtain DOI-related information, mint/update DOIs, and persist
    related DOI data into Gen3. Returns GUIDs with finalized metadata persisted into Gen3.

    This will:
        - Get all Discovery Page records based on the provided `gen3_auth`
        - Use `metadata_interface` to use class for a particular
          DOI metadata retrieval (such as retrieving DOI metadata for dbGaP
          studies)
        - Request an identifier for missing records by using the provided
          `get_doi_identifier_function`
        - Send request(s) to DataCite's API to mint/update DOI's
        - Persist DOI related metadata into Gen3's Discovery Metadata

        WARNING: The act of "minting" the DOI does not fully publish it.
                 You must explicitly supply `publish_dois=True`

        TODO: intelligent versioning. How do we know when a new VERSION of the
              DOI needs to be created with a link to the previous?
              Perhaps there's just a field to attach a related DOI?

    Args:
        gen3_auth (Gen3Auth): Gen3 auth or tuple with basic auth name and password
        datacite_auth (requests.auth.HTTPBasicAuth): Basic auth username and password
            provided by datacite to use against their API.

            WARNING: DO NOT HARD-CODE SECRETS OR PASSWORDS IN YOUR CODE. USE
                     ENVIRONMENT VARIABLES OR PULL FROM AN EXTERNAL SOURCE.
        get_doi_identifier_function (Function): A function to call to get a valid DOI identifier
            This DOES need to include the required prefix. For example,
            the function should return something like: "10.12345/This-is-custom-123" and randomize
            output on every call to get a new ID.
        metadata_interface (gen3.discovery_dois.GetMetadataInterface CLASS, NOT INSTANCE):
            GetMetadataInterface child class name. Must implement the interface.
            For example: `DbgapMetadataInterface`
        doi_publisher (str): Who/what to list as the DOI Publisher (required field)
        commons_discovery_page (str): URL for Discovery Page root. Usually something
            like: "https://example.com/discovery"
        doi_disclaimer (str): Disclaimer to add to all records (this only ends
            up in the Gen3 Metadata such that it can be visualized in the Discovery Page)
        doi_access_information (str): Text description of how to access the underlying
            data referenced by the DOI. This is a required part of the Landing Page.
        doi_access_information_link (str): Link out to more info about data access.
            This isn't required, but combining this with the `doi_access_information`
            provides an option for text and a link out.
        doi_contact (str): Required DOI metadata, who to contact with questions
            about the DOI. This could be an email, link to contact form, etc.
        exclude_datasets (list[str], optional): List of datasets to ignore from Discovery Metadata
            (don't attempt to read DOI Metadata or mint DOIs for these). The strings in the
            list can be either the alternate IDs (from the specified `metadata_field_for_alternate_id`)
            or they can be the Metadata GUIDs (or a mix).
        metadata_field_for_alternate_id (str): Field in current Discovery Metadata
            where the alternate ID is that you want to use
            (this could be a dbGaP ID or anything else that is GLOBALLY UNIQUE)
            If you want to use the "guid" itself, you shouldn't need to change this.
        use_doi_in_landing_page_url (bool, optional): Whether or not to use
            the actual DOI in the landing page URL (derived from `commons_discovery_page`).
            If this is False, the existing GUID is used instead.
            If you want to make the URL contain the real DOI and
            create a way to alias Discovery so `/discovery/GUID` and `/discovery/DOI`
            go to the same place, that's cool too. But that feature wasn't
            available at the time this code was written, so keep this False until
            then.
        doi_metadata_field_prefix (str, optional): What to prepend to Gen3 metadata
            fields related to DOIs
        datacite_use_prod (bool, optional): Default `False`. Whether or not to use
            the production Datacite API.
        datacite_api (str, optional): Provide to override the Datacite API to
            send DOI mint requests to. Providing this ignores `datacite_use_prod`
        publish_dois (bool, optional): Whether or not to fully publish the DOIs
            created.

            WARNING: Setting this as True is NOT REVERSABLE once this function
                     completes. This makes the DOIs PERMENANTLY SEARCHABLE.
    """
    exclude_datasets = exclude_datasets or []

    (
        current_discovery_alternate_id_to_guid,
        all_discovery_metadata,
    ) = get_alternate_id_to_guid_mapping(
        auth=gen3_auth,
        metadata_field_for_alternate_id=metadata_field_for_alternate_id,
    )

    all_doi_data = metadata_interface(
        doi_publisher=doi_publisher,
        current_discovery_alternate_id_to_guid=current_discovery_alternate_id_to_guid,
        all_discovery_metadata=all_discovery_metadata,
        exclude_datasets=exclude_datasets,
    ).get_doi_metadata()

    datacite = DataCite(
        api=datacite_api,
        use_prod=datacite_use_prod,
        auth_provider=datacite_auth,
    )

    doi_identifiers = {}

    for alternate_id, doi_metadata in all_doi_data.items():
        if (
            alternate_id in exclude_datasets
            or current_discovery_alternate_id_to_guid[alternate_id] in exclude_datasets
        ):
            logging.info(
                f"Metadata was found, but we're excluding {metadata_field_for_alternate_id} "
                f"`{alternate_id}` (Metadata GUID: "
                f"`{current_discovery_alternate_id_to_guid[alternate_id]}`). "
                f"No DOI metadata will be retrieved and no DOI will be minted."
            )
            continue

        # check if the Discovery metadata thinks an existing DOI has been minted
        # (e.g. see if there's a DOI listed in the metadata)
        existing_identifier = all_discovery_metadata.get(
            current_discovery_alternate_id_to_guid[alternate_id], {}
        ).get(doi_metadata_field_prefix + "identifier")

        if existing_identifier:
            existing_doi_response = datacite.read_doi(existing_identifier)

        # either there's no identifier in existing metadata OR there is but there's
        # not an existing DOI minted, so we should get new DOI metadata and mint
        if not existing_identifier or (
            existing_identifier and not is_status_code(existing_doi_response, 200)
        ):
            identifier = existing_identifier or get_doi_identifier_function()
            _raise_exception_on_collision(datacite, identifier)

            # note that doi_identifiers is updated within this function
            _create_or_update_doi_and_persist(
                update=False,
                datacite=datacite,
                current_discovery_alternate_id_to_guid=current_discovery_alternate_id_to_guid,
                alternate_id=alternate_id,
                use_doi_in_landing_page_url=use_doi_in_landing_page_url,
                identifier=identifier,
                commons_discovery_page=commons_discovery_page,
                doi_metadata=doi_metadata,
                publish_dois=publish_dois,
                gen3_auth=gen3_auth,
                doi_disclaimer=doi_disclaimer,
                doi_access_information=doi_access_information,
                doi_access_information_link=doi_access_information_link,
                doi_contact=doi_contact,
                doi_metadata_field_prefix=doi_metadata_field_prefix,
                doi_identifiers=doi_identifiers,
            )
        else:
            _create_or_update_doi_and_persist(
                update=True,
                datacite=datacite,
                current_discovery_alternate_id_to_guid=current_discovery_alternate_id_to_guid,
                alternate_id=alternate_id,
                use_doi_in_landing_page_url=use_doi_in_landing_page_url,
                identifier=existing_identifier,
                commons_discovery_page=commons_discovery_page,
                doi_metadata=doi_metadata,
                publish_dois=publish_dois,
                gen3_auth=gen3_auth,
                doi_disclaimer=doi_disclaimer,
                doi_access_information=doi_access_information,
                doi_access_information_link=doi_access_information_link,
                doi_contact=doi_contact,
                doi_metadata_field_prefix=doi_metadata_field_prefix,
                doi_identifiers=doi_identifiers,
            )

    return doi_identifiers


def _create_or_update_doi_and_persist(
    update,
    datacite,
    current_discovery_alternate_id_to_guid,
    alternate_id,
    use_doi_in_landing_page_url,
    identifier,
    commons_discovery_page,
    doi_metadata,
    publish_dois,
    gen3_auth,
    doi_disclaimer,
    doi_access_information,
    doi_access_information_link,
    doi_contact,
    doi_metadata_field_prefix,
    doi_identifiers,
):
    # writes metadata to a record
    guid = current_discovery_alternate_id_to_guid[alternate_id]

    # the DOI already exists for existing metadata, update as necessary
    if use_doi_in_landing_page_url:
        doi = DigitalObjectIdentifier(
            identifier=identifier,
            root_url=commons_discovery_page,
            **doi_metadata,
        )
    else:
        url = commons_discovery_page.rstrip("/") + f"/{guid}/"
        doi = DigitalObjectIdentifier(
            identifier=identifier,
            url=url,
            **doi_metadata,
        )

    if publish_dois:
        logging.info(f"Publishing DOI `{identifier}`...")
        doi.event = "publish"

    # takes either a DOI object, or an ID and will query the MDS
    if update:
        response = datacite.update_doi(doi)
    else:
        response = datacite.create_doi(doi)

    doi = DigitalObjectIdentifier.from_datacite_create_doi_response(response)

    # TODO: update `is_available` in `additional_metadata`?

    metadata = datacite.persist_doi_metadata_in_gen3(
        guid=guid,
        doi=doi,
        auth=gen3_auth,
        additional_metadata={
            "disclaimer": doi_disclaimer,
            "access_information": doi_access_information,
            "access_information_link": doi_access_information_link,
            "contact": doi_contact,
        },
        prefix=doi_metadata_field_prefix,
    )

    doi_identifiers[identifier] = metadata


def _raise_exception_on_collision(datacite, identifier):
    existing_doi_for_new_id = datacite.read_doi(identifier)
    if existing_doi_for_new_id:
        raise Exception(
            f"Conflicting DOI identifier generated. {identifier} already exists in DataCite."
        )


def get_alternate_id_to_guid_mapping(metadata_field_for_alternate_id, auth):
    """
    Return mapping from the alternate ID in current Discovery Metadata
    to Metadata GUID. This function uses the provided `metadata_field_for_alternate_id`
    to find the actual value in the Discovery Metadata).

    Args:
        metadata_field_for_alternate_id (str): Field in current Discovery Metadata
            where the alternate ID is (this could be a dbGaP ID or anything else
            that is GLOBALLY UNIQUE)
        auth (Gen3Auth): Gen3 auth or tuple with basic auth name and password

    Returns:
        (alternate_id_to_guid Dict, all_discovery_metadata Dict): mapping from alternate_id to
            guid & all the discovery metadata
    """
    loop = get_or_create_event_loop_for_thread()
    output_filename = loop.run_until_complete(
        output_expanded_discovery_metadata(auth, endpoint=auth.endpoint)
    )

    alternate_id_to_guid = {}
    all_discovery_metadata = {}
    with open(output_filename) as metadata_file:
        csv_parser_setting = {
            **BASE_CSV_PARSER_SETTINGS,
            "delimiter": get_delimiter_from_extension(output_filename),
        }
        metadata_reader = csv.DictReader(metadata_file, **{**csv_parser_setting})
        for row in metadata_reader:
            if row.get(metadata_field_for_alternate_id):
                alternate_id_to_guid[row.get(metadata_field_for_alternate_id)] = row[
                    "guid"
                ]
                all_discovery_metadata[row["guid"]] = row
            else:
                logging.warning(
                    f"Could not find field {metadata_field_for_alternate_id} on row: {row}. Skipping..."
                )

    return alternate_id_to_guid, all_discovery_metadata
