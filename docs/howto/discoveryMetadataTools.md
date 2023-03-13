## Gen3 Discovery Page Metadata Tools

TOC
- [Overview](#overview)
- [Combine Metadata with Current Discovery Metadata](#combine-metadata-with-current-discovery-metadata)

### Overview

The Gen3 Discovery Page allows the visualization of metadata. There are a collection
of SDK/CLI functionality that assists with the managing of such metadata in Gen3.

`gen3 discovery --help` will provide the most up to date information about CLI
functionality.

Like other CLI functions, the CLI code mostly just wraps an SDK function call.

So you can choose to use the CLI or write your own Python script and use the SDK
functions yourself. Generally this provides the most flexibility, at less
of a convenience.

### Combine Metadata with Current Discovery Metadata

For CLI, see `gen3 discovery combine --help`.

This will describe how to use the SDK functions directly. If you use the CLI,
it will automatically read current Discovery metadata and then combine with the
file you provide (after applying a prefix to all the columns, if you specify that).

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
from gen3.nih import dbgapFHIR
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