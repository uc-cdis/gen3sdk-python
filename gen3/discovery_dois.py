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


def mint_dois_for_dbgap_discovery_datasets(
    gen3_auth,
    datacite_auth,
    dbgap_phsid_field,
    get_doi_identifier_function,
    publisher,
    commons_discovery_page,
    doi_disclaimer,
    doi_access_information,
    doi_access_information_link,
    doi_contact,
    use_doi_in_landing_page_url=False,
    doi_metadata_field_prefix="doi_",
    datacite_api=DataCite.TEST_URL,
    publish_dois=False,
):
    """
    Get Discovery Page records, obtain DOI-related information, mint/update DOIs, and persist
    related DOI data into Gen3. Returns GUIDs with finalized metadata persisted into Gen3.

    This will:
        - Get all Discovery Page records based on the provided `gen3_auth`
        - Query dbGaP APIs based on the phsid obtained from the
          field specified by `dbgap_phsid_field`
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
        dbgap_phsid_field (str): Field in current Discovery Metadata where phsid is
        get_doi_identifier_function (Function): A function to call to get a valid DOI identifier
            This DOES need to include the required prefix. For example,
            the function should return something like: "10.12345/This-is-custom-123" and randomize
            output on every call to get a new ID.
        publisher (str): Who/what to list as the DOI Publisher (required field)
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
        doi_metadata_field_prefix (str, optional): What to prepend to Gen3 metadata
            fields related to DOIs
        datacite_api (str, optional): The Datacite API to send DOI mint requests to.
            Defaults to their Test API. You can provide `None` to default to production, or
            provide whatever Datacite API you have.
        publish_dois (bool, optional): Whether or not to fully publish the DOIs
            created.

            WARNING: Setting this as True is NOT REVERSABLE once this function
                     completes. This makes the DOIs PERMENANTLY SEARCHABLE.

    Returns:
        Dict: mapping from DOI identifier to final Gen3 Metadata
    """
    (
        current_discovery_doi_id_to_guid,
        all_discovery_metadata,
    ) = get_phsid_to_guid_mapping(auth=gen3_auth, dbgap_phsid_field=dbgap_phsid_field)

    studies = current_discovery_doi_id_to_guid.keys()

    # there's a separate DOI class for dbGaP as a wrapper around various
    # different APIs we need to call. Whereas if there are API sources that have
    # all available data, you could write a more generic SomeClass.as_gen3_metadata()
    #
    # NOTE: The final DOI does NOT include the required field `url` (because generating
    #       that requires knowledge of the final DOI identifier).
    #       So if you're using this, you need to ensure the DigitalObjectIdentifier
    #       object you create with this metadata is supplied with the right landing page URL (or
    #       you can ask it to generate one based on a root url provided).
    dbgap_doi = dbgapDOI(publisher=publisher)

    all_doi_data = dbgap_doi.get_metadata_for_ids(ids=studies)

    datacite = DataCite(
        api=datacite_api,
        auth_provider=datacite_auth,
    )

    doi_identifiers = {}

    for doi_id, doi_metadata in all_doi_data.items():
        existing_identifier = all_discovery_metadata.get(
            doi_metadata_field_prefix + "identifier"
        )

        if existing_identifier:
            existing_doi_response = datacite.read_doi(existing_identifier)

        # either there's no identifier in existing metadata OR there is but there's
        # not an existing DOI minted, so we should get new DOI metadata and mint
        if not existing_identifier or (
            existing_identifier and not is_status_code(existing_doi_response, 200)
        ):
            identifier = existing_identifier or get_doi_identifier_function()
            existing_doi_for_new_id = datacite.read_doi(identifier)
            if existing_doi_for_new_id:
                raise Exception(
                    f"Conflicting DOI identifier generated. {identifier} already exists in DataCite."
                )

            # writes metadata to a record
            guid = current_discovery_doi_id_to_guid[doi_id]

            if use_doi_in_landing_page_url:
                doi = DigitalObjectIdentifier(
                    identifier=identifier,
                    root_url=commons_discovery_page,
                    **doi_metadata,
                )
            else:
                url = commons_discovery_page.rstrip("/") + f"/{guid}"
                doi = DigitalObjectIdentifier(
                    identifier=identifier,
                    url=url,
                    **doi_metadata,
                )

            if publish_dois:
                logging.info(f"Publishing DOI `{identifier}`...")
                doi.event = "publish"

            # takes either a DOI object, or an ID and will query the MDS
            response = datacite.create_doi(doi)
            doi = DigitalObjectIdentifier.from_datacite_create_doi_response(response)

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
        else:
            # writes metadata to a record
            guid = current_discovery_doi_id_to_guid[doi_id]

            # the DOI already exists for existing metadata, update as necessary
            if use_doi_in_landing_page_url:
                doi = DigitalObjectIdentifier(
                    identifier=identifier,
                    root_url=commons_discovery_page,
                    **doi_metadata,
                )
            else:
                url = commons_discovery_page.rstrip("/") + f"/{guid}"
                doi = DigitalObjectIdentifier(
                    identifier=identifier,
                    url=url,
                    **doi_metadata,
                )

            if publish_dois:
                logging.info(f"Publishing DOI `{identifier}`...")
                doi.event = "publish"

            response = datacite.update_doi(doi)
            doi = DigitalObjectIdentifier.from_datacite_create_doi_response(response)

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

    return doi_identifiers


def get_phsid_to_guid_mapping(dbgap_phsid_field, auth):
    """
    Return mapping from phsid in current Discovery Metadata to Metadata GUID
    (uses the provided field to find the phsid).

    Args:
        dbgap_phsid_field (str): Field in current Discovery Metadata where phsid is
        auth (Gen3Auth): Gen3 auth or tuple with basic auth name and password

    Returns:
        (phsid_to_guid Dict, all_discovery_metadata Dict): mapping from phsid to
            guid & all the discovery metadata
    """
    loop = get_or_create_event_loop_for_thread()
    output_filename = loop.run_until_complete(
        output_expanded_discovery_metadata(auth, endpoint=auth.endpoint)
    )

    phsid_to_guid = {}
    all_discovery_metadata = {}
    with open(output_filename) as metadata_file:
        csv_parser_setting = {
            **BASE_CSV_PARSER_SETTINGS,
            "delimiter": get_delimiter_from_extension(output_filename),
        }
        metadata_reader = csv.DictReader(metadata_file, **{**csv_parser_setting})
        for row in metadata_reader:
            if row.get(dbgap_phsid_field):
                phsid_to_guid[row.get(dbgap_phsid_field)] = row["guid"]
                all_discovery_metadata[row["guid"]] = row
            else:
                logging.warning(
                    f"Could not find field {dbgap_phsid_field} on row: {row}. Skipping..."
                )

    return phsid_to_guid, all_discovery_metadata
