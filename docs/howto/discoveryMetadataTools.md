## Gen3 Discovery Page Metadata Tools

**Table of Contents**

- [Overview](#overview)
- [DOIs in Gen3](#dois-in-gen3-discovery-metadata-and-page-for-visualizing-public-doi-metadata)
- [dbGaP FHIR Metadata in Gen3 Discovery](#combine-dbgap-fhir-metadata-with-current-discovery-metadata)

### Overview

The Gen3 Discovery Page allows the visualization of metadata. There are a collection
of SDK/CLI functionality that assists with the managing of such metadata in Gen3.

`gen3 discovery --help` will provide the most up to date information about CLI
functionality.

Like other CLI functions, the CLI code mostly just wraps an SDK function call.

So you can choose to use the CLI or write your own Python script and use the SDK
functions yourself. Generally this provides the most flexibility, at less
of a convenience.

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
    api=DataCite.TEST_URL,
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
  doi = DigitalObjectIdentifier(root_url=COMMONS_DISCOVERY_PAGE, **doi_metadata)

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
                    "sourceField": "disclaimer",
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
                    "sourceField": "doi_resolveable_link",
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

#### Work in Progress. Script to automate dbGaP scraping for updating datasets and minting DOIs

- TODO: Push DOI from submitted to registered

See below for a full example of DOI metadata gathering, minting, and persisting
into Gen3.

```python
import os
from requests.auth import HTTPBasicAuth

from cdislogging import get_logger

from gen3.auth import Gen3Auth
from gen3.discovery_dois import mint_dois_for_dbgap_discovery_datasets
from gen3.utils import get_random_alphanumeric

logging = get_logger("__name__", log_level="info")

PREFIX = "10.12345"
PUBLISHER = "Example"
COMMONS_DISCOVERY_PAGE = "https://example.com/discovery"

DOI_DISCLAIMER = ""
DOI_ACCESS_INFORMATION = "You can find information about how to access this resource in the link below."
DOI_ACCESS_INFORMATION_LINK = "https://example.com/more/info"
DOI_CONTACT = "https://example.com/contact/"


def get_doi_identifier():
    return (
        PREFIX + "/EXAMPLE-" + get_random_alphanumeric(4) + "-" + get_random_alphanumeric(4)
    )


def main():
    auth = Gen3Auth()
    dbgap_phsid_field = "dbgap_accession"

    mint_dois_for_dbgap_discovery_datasets(
        gen3_auth=auth,
        datacite_auth=HTTPBasicAuth(
            os.environ.get("DATACITE_USERNAME"),
            os.environ.get("DATACITE_PASSWORD"),
        ),
        dbgap_phsid_field=dbgap_phsid_field,
        get_doi_identifier_function=get_doi_identifier,
        publisher=PUBLISHER,
        commons_discovery_page=COMMONS_DISCOVERY_PAGE,
        doi_disclaimer=DOI_DISCLAIMER,
        doi_access_information=DOI_ACCESS_INFORMATION,
        doi_access_information_link=DOI_ACCESS_INFORMATION_LINK,
        doi_contact=DOI_CONTACT,
    )

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