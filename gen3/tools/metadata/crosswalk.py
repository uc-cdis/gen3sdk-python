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
from gen3.utils import deep_dict_update, raise_for_status_and_print_error

MAX_GUIDS_PER_REQUEST = 2000
MAX_CONCURRENT_REQUESTS = 5
BASE_CSV_PARSER_SETTINGS = {
    "delimiter": "\t",
    "quotechar": "",
    "quoting": csv.QUOTE_NONE,
    "escapechar": "\\",
}

GUID_TYPE = "subject"
CROSSWALK_NAMESPACE = "crosswalk"

logging = get_logger("__name__", log_level="debug")


async def output_expanded_crosswalk_metadata(
    auth, endpoint=None, limit=500, use_agg_mds=False
):
    """
    fetch crosswalk metadata from a commons and output to {commons}-crosswalk-metadata.tsv
    """
    if endpoint:
        mds = Gen3Metadata(
            auth_provider=auth,
            endpoint=endpoint,
            service_location="mds/aggregate" if use_agg_mds else "mds",
        )
    else:
        mds = Gen3Metadata(
            auth_provider=auth,
            service_location="mds/aggregate" if use_agg_mds else "mds",
        )

    count = 0
    with tempfile.TemporaryDirectory() as metadata_cache_dir:
        all_fields = set()
        num_tags = 0

        for offset in range(0, limit, MAX_GUIDS_PER_REQUEST):
            partial_metadata = mds.query(
                f"_guid_type={GUID_TYPE}",
                return_full_metadata=True,
                limit=min(limit, MAX_GUIDS_PER_REQUEST),
                offset=offset,
                use_agg_mds=use_agg_mds,
            )

            # if agg MDS we will flatten the results as they are in "common" : dict format
            # However this can result in duplicates as the aggregate mds is namespaced to
            # handle this, therefore prefix the commons in front of the guid
            if use_agg_mds:
                partial_metadata = {
                    f"{c}__{i}": d
                    for c, y in partial_metadata.items()
                    for x in y
                    for i, d in x.items()
                }

            if len(partial_metadata):
                for guid, guid_metadata in partial_metadata.items():
                    with open(
                        f"{metadata_cache_dir}/{guid.replace('/', '_')}", "w+"
                    ) as cached_guid_file:
                        guid_crosswalk_metadata = guid_metadata[CROSSWALK_NAMESPACE]
                        json.dump(guid_crosswalk_metadata, cached_guid_file)
                        all_fields |= set(guid_crosswalk_metadata.keys())
                        num_tags = max(
                            num_tags, len(guid_crosswalk_metadata.get("tags", []))
                        )
            else:
                break

        output_columns = (
            ["guid"]
            # "tags" is flattened to _tag_0 through _tag_n
            + sorted(list(all_fields - set(["tags"])))
            + [f"_tag_{n}" for n in range(num_tags)]
        )
        base_schema = {column: "" for column in output_columns}

        output_filename = _metadata_file_from_auth(auth)
        with open(
            output_filename,
            "w+",
        ) as output_file:
            writer = csv.DictWriter(
                output_file,
                **{**BASE_CSV_PARSER_SETTINGS, "fieldnames": output_columns},
            )
            writer.writeheader()

            for guid in sorted(os.listdir(metadata_cache_dir)):
                with open(f"{metadata_cache_dir}/{guid}") as f:
                    fetched_metadata = json.load(f)
                    flattened_tags = {
                        f"_tag_{tag_num}": f"{tag['category']}: {tag['name']}"
                        for tag_num, tag in enumerate(fetched_metadata.pop("tags", []))
                    }

                    true_guid = guid
                    if use_agg_mds:
                        true_guid = guid.split("__")[1]
                    output_metadata = _sanitize_tsv_row(
                        {
                            **base_schema,
                            **fetched_metadata,
                            **flattened_tags,
                            "guid": true_guid,
                        }
                    )
                    writer.writerow(output_metadata)

        return output_filename


async def publish_crosswalk_metadata(
    auth,
    file,
    info_file=None,
    guid_type=GUID_TYPE,
    mapping_methodology="",
):
    """
    Publish crosswalk metadata from a tsv file
    """
    mds = Gen3Metadata(auth_provider=auth)
    index = Gen3Index(auth_provider=auth)

    delimiter = "," if file.endswith(".csv") else "\t"

    # dictionary of { "commons url + identifier name": description }
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
                description = crosswalk_info.get(commons_url + identifier_name, "")
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
                mds_record_mapping_methodologies.add(mapping_methodology)
            else:
                logging.debug(f"no existing metadata record found.")
                mds_record_guid_type = GUID_TYPE
                mds_record_crosswalk_for_guid = {}
                mds_record_mapping_methodologies = set([mapping_methodology])

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
            # non-crosswalk namespaced blocks of
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


def _sanitize_tsv_row(tsv_row):
    sanitized = {}
    for k, v in tsv_row.items():
        if type(v) in [list, dict]:
            sanitized[k] = json.dumps(v)
        elif type(v) == str:
            sanitized[k] = v.replace("\n", "\\n")
    return sanitized


def _get_crosswalk_columns_parts(column):
    commons_url, identifier_type, identifier_name = column.strip().split("|")
    return (
        commons_url.strip(),
        identifier_type.strip(),
        identifier_name.strip(),
    )


def _metadata_file_from_auth(auth):
    return (
        "-".join(urlparse(auth.endpoint).netloc.split(".")) + "-crosswalk_metadata.tsv"
    )
