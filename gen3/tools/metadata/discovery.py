import csv
import json
from cdislogging import get_logger
import tempfile
import asyncio
import os
from urllib.parse import urlparse

import requests.exceptions

from gen3.metadata import Gen3Metadata

MAX_GUIDS_PER_REQUEST = 2000
MAX_CONCURRENT_REQUESTS = 5
BASE_CSV_PARSER_SETTINGS = {
    "delimiter": "\t",
    "quotechar": "",
    "quoting": csv.QUOTE_NONE,
    "escapechar": "\\",
}

logging = get_logger("__name__")


async def output_expanded_discovery_metadata(
    auth, endpoint=None, limit=500, use_agg_mds=False
):
    """
    fetch discovery metadata from a commons and output to {commons}-discovery-metadata.tsv
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
                "_guid_type=discovery_metadata",
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
                        guid_discovery_metadata = guid_metadata["gen3_discovery"]
                        json.dump(guid_discovery_metadata, cached_guid_file)
                        all_fields |= set(guid_discovery_metadata.keys())
                        num_tags = max(
                            num_tags, len(guid_discovery_metadata.get("tags", []))
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


async def publish_discovery_metadata(
    auth,
    metadata_filename,
    endpoint=None,
    omit_empty_values=False,
    guid_type="discovery_metadata",
    guid_field=None,
    is_unregistered_metadata=False,
    reset_unregistered_metadata=False,
    update_registered_metadata=True,
):
    """
    Publish discovery metadata from a tsv file
    """
    if endpoint:
        mds = Gen3Metadata(auth_provider=auth, endpoint=endpoint)
    else:
        mds = Gen3Metadata(auth_provider=auth)

    if not metadata_filename:
        metadata_filename = _metadata_file_from_auth(auth)

    delimiter = "," if metadata_filename.endswith(".csv") else "\t"

    with open(metadata_filename) as metadata_file:
        csv_parser_setting = {**BASE_CSV_PARSER_SETTINGS, "delimiter": delimiter}
        if is_unregistered_metadata:
            csv_parser_setting["quoting"] = csv.QUOTE_MINIMAL
            csv_parser_setting["quotechar"] = '"'
        metadata_reader = csv.DictReader(metadata_file, **{**csv_parser_setting})
        tag_columns = [
            column for column in metadata_reader.fieldnames if "_tag_" in column
        ]
        pending_requests = []

        registered_metadata_guids = []
        registered_metadata = {}
        if is_unregistered_metadata:
            if not update_registered_metadata:
                registered_metadata_guids = mds.query(
                    f"_guid_type={guid_type}", limit=2000, offset=0
                )
            else:
                registered_metadata = mds.query(
                    f"_guid_type={guid_type}",
                    return_full_metadata=True,
                    limit=2000,
                    offset=0,
                )
                registered_metadata_guids = registered_metadata.keys()

        for metadata_line in metadata_reader:
            discovery_metadata = {
                key: _try_parse(value) for key, value in metadata_line.items()
            }

            if guid_field is None:
                guid = discovery_metadata.pop("guid")
            else:
                guid = discovery_metadata[guid_field]

            # when publishing unregistered metadata, skip those who are already registered if both reset_unregistered_metadata and update_registered_metadata are set to false
            if (
                is_unregistered_metadata
                and str(guid) in registered_metadata_guids
                and not reset_unregistered_metadata
                and not update_registered_metadata
            ):
                continue

            if len(tag_columns):
                # all columns _tag_0 -> _tag_n are pushed to a "tags" column
                coalesced_tags = [
                    {"name": tag_name.strip(), "category": tag_category.strip()}
                    for tag_category, tag_name in [
                        tag.split(":")
                        for tag in map(discovery_metadata.pop, tag_columns)
                        if tag != ""
                    ]
                ]
                discovery_metadata["tags"] = coalesced_tags

            if omit_empty_values:
                discovery_metadata = {
                    key: value
                    for key, value in discovery_metadata.items()
                    if value not in ["", [], {}]
                }

            new_guid_type = guid_type
            if is_unregistered_metadata:
                if reset_unregistered_metadata or (
                    str(guid) not in registered_metadata_guids
                ):
                    # only set GUID type to "unregistered_discovery_metadata" for unregistered metadata, or reset_unregistered_metadata is set
                    new_guid_type = f"unregistered_{guid_type}"
                elif str(guid) in registered_metadata_guids:
                    if update_registered_metadata:
                        existing_registered_metadata = {}
                        try:
                            existing_registered_metadata = registered_metadata.get(
                                str(guid)
                            ).get("gen3_discovery")
                        except AttributeError:
                            pass
                        discovery_metadata = {
                            **existing_registered_metadata,
                            **discovery_metadata,
                        }
                    else:
                        logging.warning(f"{guid} is not already registered. Skipping.")
                        continue

            metadata = {
                "_guid_type": new_guid_type,
                "gen3_discovery": discovery_metadata,
            }

            pending_requests += [mds.async_create(guid, metadata, overwrite=True)]
            if len(pending_requests) == MAX_CONCURRENT_REQUESTS:
                await asyncio.gather(*pending_requests)
                pending_requests = []

        await asyncio.gather(*pending_requests)


def try_delete_discovery_guid(auth, guid):
    """
    Deletes all discovery metadata under [guid] if it exists
    """
    mds = Gen3Metadata(auth_provider=auth)
    try:
        metadata = mds.get(guid)
        if metadata["_guid_type"] == "discovery_metadata":
            mds.delete(guid)
        else:
            logging.warning(f"{guid} is not discovery metadata. Skipping.")
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


def _try_parse(data):
    if data:
        data = data.replace("\\n", "\n")
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return data
    return ""


def _metadata_file_from_auth(auth):
    return (
        "-".join(urlparse(auth.endpoint).netloc.split(".")) + "-discovery_metadata.tsv"
    )
