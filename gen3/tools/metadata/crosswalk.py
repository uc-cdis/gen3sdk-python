import asyncio
import csv
import datetime
import json
import os
import tempfile
from urllib.parse import urlparse

import requests.exceptions
from cdislogging import get_logger

from gen3.index import Gen3Index
from gen3.metadata import Gen3Metadata
from gen3.tools import metadata
from gen3.utils import deep_dict_update

MAX_GUIDS_PER_REQUEST = 2000
MAX_CONCURRENT_REQUESTS = 5
BASE_CSV_PARSER_SETTINGS = {
    "delimiter": ",",
    "quotechar": "",
    "quoting": csv.QUOTE_NONE,
    "escapechar": "\\",
}

GUID_TYPE = "subject"
CROSSWALK_NAMESPACE = "crosswalk"

logging = get_logger("__name__", log_level="debug")


async def read_crosswalk_metadata(
    auth, output_filename="crosswalk_metadata.csv", limit=500
):
    """
    fetch crosswalk metadata from a commons and output to crosswalk-metadata.tsv

    Data in the commons is in this format:

        "GUID": {
          "crosswalk": {
            "subject": {
              "{{commons_url}}": {
                "{{field_name}}": {
                  "value": "",
                  "type": "",
                  "description": ""
                }
                // ... more field entries here
              },
              // ... more commons entries here
              "mapping_methodologies": [
                ""
              ]
            }
          }
        }

    Output format of crosswalk data in csv:

    commons_url|identifier_type|identifier_name,commons_url|identifier_type|identifier_name
    A01-00888, phs002363.v1_RC-1358
    ...

    Output format of additional info in csv (including descriptions):
        {{commons_url}}, {{identifier_name}}, {{description}}

    See docs/howto/crosswalk.md for more detailed information.
    """
    mds = Gen3Metadata(auth_provider=auth)

    count = 0
    with tempfile.TemporaryDirectory() as metadata_cache_dir:
        crosswalk_columns = set()

        # dictionary of { "commons url | identifier name": description }
        crosswalk_info = {}

        for offset in range(0, limit, MAX_GUIDS_PER_REQUEST):
            partial_metadata = mds.query(
                f"_guid_type={GUID_TYPE}",
                return_full_metadata=True,
                limit=min(limit, MAX_GUIDS_PER_REQUEST),
                offset=offset,
            )

            if not len(partial_metadata):
                break

            # dump crosswalk metadata into temporary files so we don't have to
            # hold everything in memory for output. Do keep track of the
            # columns and field descriptions for output (that should be small)
            for guid, guid_metadata in partial_metadata.items():
                with open(
                    f"{metadata_cache_dir}/{guid.replace('/', '_')}", "w+"
                ) as cached_guid_file:
                    guid_crosswalk_metadata = guid_metadata[CROSSWALK_NAMESPACE]

                    for commons_url, commons_crosswalk in guid_crosswalk_metadata.get(
                        GUID_TYPE
                    ).items():
                        # guid_crosswalk_metadata.get(GUID_TYPE) is something like this:
                        #
                        # {
                        #   "{{commons_url}}": {
                        #     "{{field_name}}": {
                        #       "value": "",
                        #       "type": "",
                        #       "description": ""
                        #     }
                        #     // ... more field entries here
                        #   },
                        #   // ... more commons entries here
                        #   "mapping_methodologies": [
                        #     ""
                        #   ]
                        # }

                        # don't interpret mapping info as a column in the crosswalk file
                        if commons_url == "mapping_methodologies":
                            continue

                        for (
                            identifier_name,
                            indentifer_info,
                        ) in commons_crosswalk.items():
                            # identifier_name, indentifer_info is something like:
                            #
                            #     "{{field_name}}", {
                            #       "value": "",
                            #       "type": "",
                            #       "description": ""
                            #     }
                            column_name = "|".join(
                                [
                                    commons_url,
                                    indentifer_info.get("type"),
                                    identifier_name,
                                ]
                            )
                            crosswalk_columns.add(column_name)

                            crosswalk_info[
                                commons_url + "|" + identifier_name
                            ] = indentifer_info.get("description")

                    json.dump(guid_crosswalk_metadata, cached_guid_file)

        crosswalk_columns = sorted(list(crosswalk_columns))

        logging.debug(f"got columns: {crosswalk_columns}")
        logging.debug(f"got crosswalk_info: {crosswalk_info}")

        output_info_filename = "".join(output_filename.split(".")[:-1]) + "_info.csv"
        logging.debug(f"writing crosswalk to: {output_filename}...")
        with open(
            output_filename,
            "w+",
        ) as output_file:
            writer = csv.DictWriter(
                output_file,
                **{**BASE_CSV_PARSER_SETTINGS, "fieldnames": crosswalk_columns},
            )
            writer.writeheader()

            # read from the temporary cached crosswalk metadata to get the values
            for guid in sorted(os.listdir(metadata_cache_dir)):
                with open(f"{metadata_cache_dir}/{guid}") as f:
                    fetched_metadata = json.load(f)

                    output_metadata = {}
                    for column in crosswalk_columns:
                        (
                            commons_url,
                            identifier_type,
                            identifier_name,
                        ) = _get_crosswalk_columns_parts(column)
                        output_metadata[column] = (
                            fetched_metadata.get(GUID_TYPE)
                            .get(commons_url, {})
                            .get(identifier_name, {})
                            .get("value", "")
                        )

                    # output_metadata looks something like this:
                    # {
                    #   "commons_url_a|identifier_type_a|identifier_name_a": "A01-00888",
                    #   "commons_url_b|identifier_type_b|identifier_name_b": "phs002363.v1_RC-1358",
                    # }
                    writer.writerow(output_metadata)

        logging.info(f"done writing crosswalk to: {output_filename}")
        logging.debug(f"writing crosswalk info to: {output_info_filename}...")

        with open(
            output_info_filename,
            "w+",
        ) as output_info_file:
            # Output format of additional info in csv (including descriptions):
            #     {{commons_url}}, {{identifier_name}}, {{description}}
            info_columns = ["commons_url", "identifier_name", "description"]
            writer = csv.DictWriter(
                output_info_file,
                **{**BASE_CSV_PARSER_SETTINGS, "fieldnames": info_columns},
            )
            writer.writeheader()

            for commons_url_and_identifier_name, description in crosswalk_info.items():
                output_metadata = {}
                commons_url, identifier_name = commons_url_and_identifier_name.split(
                    "|"
                )
                output_metadata["commons_url"] = commons_url
                output_metadata["identifier_name"] = identifier_name
                output_metadata["description"] = description or ""

                writer.writerow(output_metadata)

        logging.info(f"done writing crosswalk info to: {output_info_filename}")


async def publish_crosswalk_metadata(
    auth,
    file,
    info_file=None,
    guid_type=GUID_TYPE,
    mapping_methodologies=None,
):
    """
    Publish crosswalk metadata from a tsv file
    """
    mapping_methodologies = mapping_methodologies or []

    mds = Gen3Metadata(auth_provider=auth)
    index = Gen3Index(auth_provider=auth)

    delimiter = "," if file.endswith(".csv") else "\t"

    # dictionary of { "commons url | identifier name": description }
    crosswalk_info = {}
    if info_file:
        with open(info_file, "rt", encoding="utf-8-sig") as csvfile:
            metadata_info_reader = csv.DictReader(csvfile, delimiter=delimiter)

            if (
                metadata_info_reader.fieldnames
                and len(metadata_info_reader.fieldnames) != 3
            ) or [
                "commons_url",
                "identifier_name",
                "description",
            ] != metadata_info_reader.fieldnames:
                error_msg = (
                    "Invalid Crosswalk Metadata Info file format. Please ensure 3 "
                    f"columns for info with headers: commons_url,identifier_name,description). "
                    f"Found: {metadata_info_reader.fieldnames}"
                )
                logging.error(error_msg)
                raise Exception(error_msg)

            for metadata_line in metadata_info_reader:
                # unique key of 2 first columns, value is the description (last column)
                crosswalk_info[
                    metadata_line["commons_url"].strip()
                    + "|"
                    + metadata_line["identifier_name"].strip()
                ] = metadata_line["description"].strip()

        logging.debug(f"crosswalk_info: {crosswalk_info}")

    with open(file, "rt", encoding="utf-8-sig") as csvfile:
        metadata_reader = csv.DictReader(csvfile, delimiter=delimiter)

        if len(metadata_reader.fieldnames) < 2:
            error_msg = (
                "Invalid Crosswalk Metadata file format. Please ensure > 2 "
                f"columns for mapping. Found: {len(metadata_reader.fieldnames)}"
            )
            logging.error(error_msg)
            raise Exception(error_msg)

        try:
            crosswalk_columns_parts = {
                columns.strip(): _get_crosswalk_columns_parts(columns)
                for columns in metadata_reader.fieldnames
            }
        except Exception as exc:
            error_msg = (
                "Invalid Crosswalk Metadata file format. Please ensure all "
                "columns are pipe-delimited with "
                "'commons_url | identifier_type | identifier_name'. "
                f"Found: {metadata_reader.fieldnames}"
            )
            logging.error(error_msg)
            raise

        logging.debug(f"crosswalk_columns_parts: {crosswalk_columns_parts}")

        pending_requests = []

        logging.debug(f"Attempting to get valid GUIDs...")
        guids_available_for_use = index.get_valid_guids(count=1000)
        logging.debug(f"Got {len(guids_available_for_use)} valid GUIDs for use.")

        for metadata_line in metadata_reader:
            raw_crosswalk_metadata = {
                key.strip(): value.strip() for key, value in metadata_line.items()
            }

            logging.debug(
                f"raw_crosswalk_metadata: {raw_crosswalk_metadata}, "
                f"from line: {metadata_line}"
            )

            metadata = {}
            metadata_aliases = []
            for column, value in raw_crosswalk_metadata.items():
                commons_url, identifier_type, identifier_name = crosswalk_columns_parts[
                    column
                ]
                metadata_aliases.append(value)

                to_update = {
                    "value": value,
                    "type": identifier_type,
                }
                description = crosswalk_info.get(
                    commons_url + "|" + identifier_name, ""
                )
                # only override the potentially existing description if a new one is
                # provided by the new crosswalk
                if description:
                    to_update.update({"description": description})

                metadata.setdefault(commons_url, {}).setdefault(
                    identifier_name, {}
                ).update(to_update)

            logging.debug(f"new crosswalk metadata: {metadata}")

            # MDS does not support a deep merge, so we need to merge any existing crosswalk
            # data with this new data here before updating

            # try to find any existing GUID from the values provided
            mds_record = None
            for alias in metadata_aliases:
                try:
                    mds_record = await mds.async_get(alias)
                except Exception as exc:
                    pass

                if mds_record:
                    break

            if mds_record:
                logging.debug(f"existing metadata record: {mds_record}")
                mds_record_guid_type = mds_record.get("_guid_type")
                mds_record_crosswalk_for_guid = mds_record.get(
                    CROSSWALK_NAMESPACE, {}
                ).get(guid_type, {})
                mds_record_mapping_methodologies = set(
                    mds_record.get(CROSSWALK_NAMESPACE, {})
                    .get(GUID_TYPE, {})
                    .get("mapping_methodologies", set())
                )
                for item in mapping_methodologies:
                    mds_record_mapping_methodologies.add(item)
            else:
                logging.debug(f"no existing metadata record found.")
                mds_record_guid_type = GUID_TYPE
                mds_record_crosswalk_for_guid = {}
                mds_record_mapping_methodologies = set(mapping_methodologies)

            # combine with new metadata using a deep update (e.g. merging nested dicts)
            deep_dict_update(mds_record_crosswalk_for_guid, metadata)
            mds_record_crosswalk_for_guid.update(
                {
                    "mapping_methodologies": sorted(
                        list(mds_record_mapping_methodologies)
                    )
                }
            )

            # add standard structure around the crosswalk metadata
            final_metadata = {
                "_guid_type": mds_record_guid_type,
                CROSSWALK_NAMESPACE: {
                    guid_type: mds_record_crosswalk_for_guid,
                },
            }

            # refresh list if needed, then get a guid
            if not guids_available_for_use:
                guids_available_for_use = index.get_valid_guids(count=1000)
            guid = guids_available_for_use.pop(-1)

            logging.info(f"crosswalk metadata for {guid}: {final_metadata}")

            # call update with merge to ensure this doesn't wipe out any
            # non-crosswalk namespaced blocks of metadata
            if mds_record:
                pending_requests += [
                    mds.async_update(
                        guid, final_metadata, aliases=metadata_aliases, merge=True
                    )
                ]
            else:
                pending_requests += [
                    mds.async_create(guid, final_metadata, aliases=metadata_aliases)
                ]

            if len(pending_requests) == MAX_CONCURRENT_REQUESTS:
                await asyncio.gather(*pending_requests)
                pending_requests = []

        await asyncio.gather(*pending_requests)


def try_delete_crosswalk_guid(auth, guid):
    """
    Deletes all crosswalk metadata under [guid] if it exists

    # TODO needs tests and CLI command
    """
    mds = Gen3Metadata(auth_provider=auth)
    try:
        metadata = mds.get(guid)
        if metadata["_guid_type"] == "crosswalk_metadata":
            mds.delete(guid)
        else:
            logging.warning(f"{guid} is not crosswalk metadata. Skipping.")
    except requests.exceptions.HTTPError as e:
        logging.warning(e)


def _get_crosswalk_columns_parts(column):
    commons_url, identifier_type, identifier_name = column.strip().split("|")
    return (
        commons_url.strip(),
        identifier_type.strip(),
        identifier_name.strip(),
    )
