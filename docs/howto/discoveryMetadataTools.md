## Gen3 Discovery Page Metadata Tools

**Table of Contents**

- [Overview](#overview)
- [Export Discovery Metadata into File](#export-discovery-metadata-from-file)
- [Publish Discovery Metadata from File](#publish-discovery-metadata-from-file)
- [DOIs in Gen3](#dois-in-gen3-discovery-metadata-and-page-for-visualizing-public-doi-metadata)
- [dbGaP FHIR Metadata in Gen3 Discovery](#combine-dbgap-fhir-metadata-with-current-discovery-metadata)
- [Publish Discovery Metadata Objects from File](#publish-discovery-metadata-objects-from-file)

### Overview

The Gen3 Discovery Page allows the visualization of metadata. There are a collection
of SDK/CLI functionality that assists with the managing of such metadata in Gen3.

`gen3 discovery --help` will provide the most up to date information about CLI
functionality.

Like other CLI functions, the CLI code mostly just wraps an SDK function call.

So you can choose to use the CLI or write your own Python script and use the SDK
functions yourself. Generally this provides the most flexibility, at less
of a convenience.

### Export Discovery Metadata into File
Gen3's SDK can be used to export discovery metadata from a certain Gen3 environment into a file by using the `output_expanded_discovery_metadata()` function. By default this function will query for metadata with `guid_type=discovery_metadata` for the dump, and export the metadata into a TSV file. User can also specify a different `guid_type` values for this operation, and/or choose to export the metadata into a JSON file. When using TSV format, some certain fields from metadata will be flattened or "jsonified" so that each metadata record can be fitted into one row.

Example of usage:
```python
from gen3.tools.metadata.discovery import (
    output_expanded_discovery_metadata,
)
from gen3.utils import get_or_create_event_loop_for_thread
from gen3.auth import Gen3Auth

if __name__ == "__main__":
    auth = Gen3Auth()
    loop = get_or_create_event_loop_for_thread()
    loop.run_until_complete(
        output_expanded_discovery_metadata(
            auth, endpoint="GEN3_ENV_HOSTNAME", output_format="json"
        )
    )
```

### Publish Discovery Metadata from File
Gen3's SDK can also be used to publish discovery metadata onto a target Gen3 environment from a file by using the `publish_discovery_metadata()` function. Ideally the metadata file should be originated from a metadata dump obtained by using the `output_expanded_discovery_metadata()` function.

Example of usage:
```python
from gen3.tools.metadata.discovery import (
    publish_discovery_metadata,
)
from gen3.utils import get_or_create_event_loop_for_thread
from gen3.auth import Gen3Auth

if __name__ == "__main__":
    auth = Gen3Auth()
    loop = get_or_create_event_loop_for_thread()
    loop.run_until_complete(
        publish_discovery_metadata(
            auth, "./metadata.tsv", endpoint=HOSTNAME, guid_field="_hdp_uid"
        )
    )
```

### DOIs in Gen3: Discovery Metadata and Page for Visualizing Public DOI Metadata

Gen3's SDK supports minting DOIs from DataCite, storing DOI metadata in a Gen3 instance,
and visualizing the DOI metadata in our Discovery Page to serve as a DOI "Landing Page".

> **DOI?** A digital object identifier (DOI) is a persistent identifier or handle used to identify objects uniquely, standardized by the International Organization for Standardization (ISO). DOIs are in wide use mainly to identify academic, professional, and government information, such as journal articles, research reports, data sets, and official publications. However, they also have been used to identify other types of information resources, such as commercial videos.

The general overview for how Gen3 supports DOIs is as follows:

* Gen3 SDK/CLI used to gather Metadata from External Public Metadata Sources
* Gen3 SDK/CLI used to do any conversions to DOI Metadata
* Gen3 SDK/CLI communicates with DataCite API to mint DOI
    * NOTE: the gathering of metadata, conversion to DOI fields, and final minting may or may not be a part of a regular data ingestion. It’s possible that this is used ad-hocly, as needed
* Gen3 SDK/CLI persists metadata in Gen3
* Persisted metadata in Gen3 exposed via Discovery Page
* Discovery Page is used as the required DOI Landing Page

> **What is DataCite?** In order to create a DOI, one must use a DOI registration service. In the US there are two: CrossRef and DataCite. We are focusing on DataCite, because that is what we were provided access to.

#### Example Script: Manually Creating a Single DOI and Persisting the Metadata in Gen3

Prerequisites:

* Environment variable `DATACITE_USERNAME` set as a valid DataCite username for interacting with their API
* Environment variable `DATACITE_PASSWORD` set as a valid DataCite password for interacting with their API

This shows a full example of:

* Setting up the necessary classes for interacting with Gen3 & Datacite
* Getting the DOI metadata (ideally from some external source like a file or another API, but here we've hard-coded it)
* Creating/Minting the DOI in DataCite
* Persisting the DOI metadata into a Gen3 Discovery record in the metadata service

```python
import os
from requests.auth import HTTPBasicAuth

from cdislogging import get_logger

from gen3.doi import (
  DataCite,
  DigitalObjectIdentifier,
  DigitalObjectIdentifierCreator,
  DigitalObjectIdentifierTitle,
)
from gen3.auth import Gen3Auth

logging = get_logger("__name__", log_level="info")

# This prefix should be provided by DataCite
PREFIX = "10.12345"
PUBLISHER = "Example"
COMMONS_DISCOVERY_PAGE = "https://example.com/discovery"

DOI_DISCLAIMER = ""
DOI_ACCESS_INFORMATION = "You can find information about how to access this resource in the link below."
DOI_ACCESS_INFORMATION_LINK = "https://example.com/more/info"
DOI_CONTACT = "https://example.com/contact/"


def test_manual_single_doi(publish_dois=False):
  # Setup
  gen3_auth = Gen3Auth()
  datacite = DataCite(
    use_prod=False,
    auth_provider=HTTPBasicAuth(
      os.environ.get("DATACITE_USERNAME"),
      os.environ.get("DATACITE_PASSWORD"),
    ),
  )

  gen3_metadata_guid = "Example-Study-01"

  # Get DOI metadata (ideally from some external source)
  identifier = "10.82483/BDC-268Z-O151"
  creators = [
    DigitalObjectIdentifierCreator(
      name="Bar, Foo",
      name_type=DigitalObjectIdentifierCreator.NAME_TYPE_PERSON,
    ).as_dict()
  ]
  titles = [DigitalObjectIdentifierTitle("Some Example Study in Gen3").as_dict()]
  publisher = "Example Gen3 Sponsor"
  publication_year = 2023
  doi_type = "Dataset"
  version = 1

  doi_metadata = {
    "identifier": identifier,
    "creators": creators,
    "titles": titles,
    "publisher": publisher,
    "publication_year": publication_year,
    "doi_type": doi_type,
    "version": version,
  }

  # Create/Mint the DOI in DataCite
  # The default url generated is "root_url" + identifier
  # If your Discovery metadata records don't use the DOI as the GUID,
  # you may need to supply the URL yourself like below
  url = COMMONS_DISCOVERY_PAGE.rstrip("/") + f"/{gen3_metadata_guid}"
  doi = DigitalObjectIdentifier(url=url, **doi_metadata)

  if publish_dois:
    logging.info(f"Publishing DOI `{identifier}`...")
    doi.event = "publish"

  # works for only new DOIs
  # You can use this for updates: `datacite.update_doi(doi)`
  response = datacite.create_doi(doi)
  doi = DigitalObjectIdentifier.from_datacite_create_doi_response(response)

  # Persist necessary DOI Metadata in Gen3 Discovery to support the landing page
  metadata = datacite.persist_doi_metadata_in_gen3(
    guid=gen3_metadata_guid,
    doi=doi,
    auth=gen3_auth,
    additional_metadata={
      "disclaimer": DOI_DISCLAIMER,
      "access_information": DOI_ACCESS_INFORMATION,
      "access_information_link": DOI_ACCESS_INFORMATION_LINK,
      "contact": DOI_CONTACT,
    },
    prefix="doi_",
  )
  logging.debug(f"Gen3 Metadata for GUID `{gen3_metadata_guid}`: {metadata}")


def main():
  test_manual_single_doi()


if __name__ == "__main__":
  main()

```

#### Example Partial Discovery Page Configuration for DOIs

This is portion of the Gen3 Data Portal configuration that pertains to
the Discovery Page. The code provided shows an example of how to configure
the visualization of the DOI metadata.

In order to be compliant with Landing Pages, the URL you provide during minting
needs to automatically display all this information. So if you have other tabs
of non-DOI information, they cannot be the first focused tab upon resolving the
DOI url.

```json
"discoveryConfig": {
    // ...
    "features": {
      // ...
      "search": {
        "searchBar": {
          "enabled": true,
          "searchableTextFields": [
            "doi_titles",
            "doi_version_information",
            "doi_citation",
            "doi_creators",
            "doi_publisher",
            "doi_identifier",
            "doi_alternateIdentifiers",
            "doi_contributors",
            "doi_descriptions",
            "doi_publication_year",
            "doi_resolvable_link",
            "doi_fundingReferences",
            "doi_relatedIdentifiers"
          ]
        },
    // ...
    "detailView": {
      // ...
      "tabs": [
        {
          "tabName": "DOI",
          "groups": [
              {
                "header": "Dataset Information",
                "fields": [
                  {
                    "type": "block",
                    "label": "",
                    "sourceField": "doi_disclaimer",
                    "default": ""
                  },
                  {
                    "type": "text",
                    "label": "Title:",
                    "sourceField": "doi_titles",
                    "default": "Not specified"
                  },
                  {
                    "type": "link",
                    "label": "DOI:",
                    "sourceField": "doi_resolvable_link",
                    "default": "None"
                  },
                  {
                    "type": "text",
                    "label": "Data available:",
                    "sourceField": "doi_is_available",
                    "default": "None"
                  },
                  {
                    "type": "text",
                    "label": "Creators:",
                    "sourceField": "doi_creators",
                    "default": "Not specified"
                  },
                  {
                    "type": "text",
                    "label": "Citation:",
                    "sourceField": "doi_citation",
                    "default": "Not specified"
                  },
                  {
                    "type": "link",
                    "label": "Contact:",
                    "sourceField": "doi_contact",
                    "default": "Not specified"
                  }
                ]
              },
              {
                "header": "How to Access the Data",
                "fields": [
                  {
                    "type": "block",
                    "label": "How to access the data:",
                    "sourceField": "doi_access_information",
                    "default": "Not specified"
                  },
                  {
                    "type": "link",
                    "label": "Data and access information:",
                    "sourceField": "doi_access_information_link",
                    "default": "Not specified"
                  }
                ]
              },
              {
                "header": "Additional Information",
                "fields": [
                  {
                    "type": "text",
                    "label": "Publisher:",
                    "sourceField": "doi_publisher",
                    "default": "Not specified"
                  },
                  {
                    "type": "text",
                    "label": "Funded by:",
                    "sourceField": "doi_fundingReferences",
                    "default": "Not specified"
                  },
                  {
                    "type": "text",
                    "label": "Publication Year:",
                    "sourceField": "doi_publication_year",
                    "default": "Not specified"
                  },
                  {
                    "type": "text",
                    "label": "Resource Type:",
                    "sourceField": "doi_resource_type",
                    "default": "Not specified"
                  },
                  {
                    "type": "text",
                    "label": "Version:",
                    "sourceField": "doi_version_information",
                    "default": "Not specified"
                  },
                  {
                    "type": "text",
                    "label": "Contributors:",
                    "sourceField": "doi_contributors",
                    "default": "Not specified"
                  },
                  {
                    "type": "text",
                    "label": "Related Identifiers:",
                    "sourceField": "doi_relatedIdentifiers",
                    "default": "Not specified"
                  }
                ]
              },
              {
                "header": "Description",
                "fields": [
                  {
                    "type": "block",
                    "label": "Description:",
                    "sourceField": "doi_descriptions",
                    "default": "Not specified"
                  }
                ]
              }
          ]
        },
        // ...
```

#### Automate DOI creation for Datasets

Automates the pulling of current datasets from Discovery, getting identifiers,
scraping various APIs for DOI related metadata, and then going through
the DOI creation loop to mint the DOI in Datacite and persist the metadata back in
Gen3.

See below for a full example using the dbGaP `DbgapMetadataInterface`.

More interfaces may exist in the future for doing this by querying non-dbGaP
sources.

```python
import os
from requests.auth import HTTPBasicAuth

from cdislogging import get_logger

from gen3.auth import Gen3Auth
from gen3.discovery_dois import mint_dois_for_discovery_datasets, DbgapMetadataInterface
from gen3.utils import get_random_alphanumeric

logging = get_logger("__name__", log_level="info")

PREFIX = "10.12345"
PUBLISHER = "Example"
COMMONS_DISCOVERY_PAGE = "https://example.com/discovery"

DOI_DISCLAIMER = ""
DOI_ACCESS_INFORMATION = "You can find information about how to access this resource in the link below."
DOI_ACCESS_INFORMATION_LINK = "https://example.com/more/info"
DOI_CONTACT = "https://example.com/contact/"

def mint_discovery_dois():
    auth = Gen3Auth()

    # this alternate ID is some globally unique ID other than the GUID that
    # will be needed to get DOI metadata (like the phsid for dbGaP)
    metadata_field_for_alternate_id = "dbgap_accession"

    # you can choose to exclude certain Discovery Metadata datasets based on
    # their GUID or alternate ID (this means they won't get additional DOI metadata
    # or have DOIs minted, they'll be skipped)
    exclude_datasets=["MetadataGUID_to_exclude", "AlternateID_to_exclude", "..."]

    # When this is True, you CANNOT REVERT THIS ACTION. A published DOI
    # cannot be deleted. It is recommended to test with "Draft" state DOIs first
    # (which is the default when publish_dois is not True).
    publish_dois = False

    mint_dois_for_discovery_datasets(
        gen3_auth=auth,
        datacite_auth=HTTPBasicAuth(
            os.environ.get("DATACITE_USERNAME"),
            os.environ.get("DATACITE_PASSWORD"),
        ),
        metadata_field_for_alternate_id=metadata_field_for_alternate_id,
        get_doi_identifier_function=get_doi_identifier,
        metadata_interface=DbgapMetadataInterface,
        doi_publisher=PUBLISHER,
        commons_discovery_page=COMMONS_DISCOVERY_PAGE,
        doi_disclaimer=DOI_DISCLAIMER,
        doi_access_information=DOI_ACCESS_INFORMATION,
        doi_access_information_link=DOI_ACCESS_INFORMATION_LINK,
        doi_contact=DOI_CONTACT,
        publish_dois=publish_dois,
        datacite_use_prod=False,
        exclude_datasets=["MetadataGUID_to_exclude", "AlternateID_to_exclude", "..."]
    )


def get_doi_identifier():
    return (
        PREFIX + "/EXAMPLE-" + get_random_alphanumeric(4) + "-" + get_random_alphanumeric(4)
    )


def main():
    mint_discovery_dois()


if __name__ == "__main__":
    main()

```

### Combine dbGaP FHIR Metadata with Current Discovery Metadata

For CLI, see `gen3 discovery combine --help`.

This will describe how to use the SDK functions directly. If you use the CLI,
it will automatically read current Discovery metadata and then combine with the
file you provide (after applying a prefix to all the columns, if you specify that).

> Note: This supports CSV and TSV formats for the metadata file

Let's assume:

- You don't have the current Discovery metadata in a file locally
- You want to merge new metadata (parsed from dbGaP's FHIR server) with the existing Discovery metadata
- You want to prefix all the new columns with `DBGAP_FHIR_`

Here's how you would do that without using the CLI:

```python
from gen3.auth import Gen3Auth
from gen3.tools.metadata.discovery import (
    output_expanded_discovery_metadata,
    combine_discovery_metadata,
)
from gen3.external.nih.dbgap_fhir import dbgapFHIR
from gen3.utils import get_or_create_event_loop_for_thread


def main():
    """
    Read current Discovery metadata, then combine with dbgapFHIR metadata.
    """
    # Get current Discovery metadata
    loop = get_or_create_event_loop_for_thread()
    auth = Gen3Auth(refresh_file="credentials.json")
    current_discovery_metadata_file = loop.run_until_complete(
        output_expanded_discovery_metadata(auth, endpoint=auth.endpoint)
    )

    # Get dbGaP FHIR Metadata
    studies = [
        "phs000007.v31",
        "phs000166.v2",
        "phs000179.v6",
    ]
    dbgapfhir = dbgapFHIR()
    simplified_data = dbgapfhir.get_metadata_for_ids(phsids=studies)
    dbgapFHIR.write_data_to_file(simplified_data, "fhir_metadata_file.tsv")

    # Combine new FHIR Metadata with existing Discovery Metadata
    metadata_filename = "fhir_metadata_file.tsv"
    discovery_column_to_map_on = "guid"
    metadata_column_to_map = "Id"
    output_filename = "combined_discovery_metadata.tsv"
    metadata_prefix = "DBGAP_FHIR_"

    output_file = combine_discovery_metadata(
        current_discovery_metadata_file,
        metadata_filename,
        discovery_column_to_map_on,
        metadata_column_to_map,
        output_filename,
        metadata_prefix=metadata_prefix,
    )

    # You now have a file with the combined information that you can publish
    # NOTE: Combining does NOT publish automatically into Gen3. You should
    #       QA the output (make sure the result is correct), and then publish.


if __name__ == "__main__":
    main()
```

### Publish Discovery Metadata Objects from File
Gen3's SDK can be used to ingest data objects related to datasets in Gen3 environment from a file by using the `publish_discovery_object_metadata()` function. To obtain a file of existing metadata objects, use the `output_discovery_objects()` function. By default new objects published from a file are appended to a dataset in a Gen3 environment. If object guids from a file already exist for a dataset in the Gen3 environment, objects are updated. If the `overwrite` option is `True`, all current metadata objects related to a dataset are instead replaced. You can also use this functionality from the CLI. See `gen3 discovery objects --help`

Example of usage:
```python
"""
Example script showing reading Discovery Objects Metadata and then
publishing it back, just to demonstrate the functions.

Before running this, ensure your ~/.gen3/credentials.json contains
an API key for a Gen3 instance to interact with and/or adjust the
Gen3Auth logic to provide auth in another way
"""
from cdislogging import get_logger

from gen3.tools.metadata.discovery_objects import (
    publish_discovery_object_metadata,
    output_discovery_objects,
)
from gen3.utils import get_or_create_event_loop_for_thread
from gen3.auth import Gen3Auth

logging = get_logger("__name__")

if __name__ == "__main__":
    auth = Gen3Auth()
    loop = get_or_create_event_loop_for_thread()
    logging.info(f"Reading discovery objects metadata from: {auth.endpoint}...")
    output_filename = loop.run_until_complete(
        output_discovery_objects(
            auth,
            output_format="tsv",
        )
    )
    logging.info(f"Output discovery objects metadata: {output_filename}")

    # Here you can modify the file by hand or in code and then publish to update
    # Alternatively, you can skip the read above and just provide a file with
    # the object metadata you want to publish

    logging.info(
        f"publishing discovery object metadata to: {auth.endpoint} from file: {output_filename}"
    )
    loop.run_until_complete(
        publish_discovery_object_metadata(
            auth,
            output_filename,
            overwrite=False,
        )
    )
```