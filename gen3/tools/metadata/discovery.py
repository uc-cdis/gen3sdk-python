import csv
import json
import datetime
import logging
import tempfile
import asyncio
import os
from urllib.parse import urlparse

import requests.exceptions

from gen3.metadata import Gen3Metadata
from gen3.tools import metadata
from gen3.utils import raise_for_status


MAX_GUIDS_PER_REQUEST = 2000
MAX_CONCURRENT_REQUESTS = 5


async def output_expanded_discovery_metadata(auth, endpoint=None, limit=500):
    """
    fetch discovery metadata from a commons and output to {commons}-discovery-metadata.tsv
    """
    if endpoint:
        mds = Gen3Metadata(auth_provider=auth, endpoint=endpoint)
    else:
        mds = Gen3Metadata(auth_provider=auth)

    with tempfile.TemporaryDirectory() as metadata_cache_dir:
        all_fields = set()
        num_tags = 0

        for offset in range(0, limit, MAX_GUIDS_PER_REQUEST):
            partial_metadata = mds.query(
                "_guid_type=discovery_metadata",
                return_full_metadata=True,
                limit=min(limit, MAX_GUIDS_PER_REQUEST),
                offset=offset,
            )

            if len(partial_metadata):
                for guid, guid_metadata in partial_metadata.items():
                    with open(f"{metadata_cache_dir}/{guid}", "w+") as cached_guid_file:
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
        with open(output_filename, "w+") as output_file:
            writer = csv.DictWriter(
                output_file,
                fieldnames=output_columns,
                delimiter="\t",
                quotechar="",
                quoting=csv.QUOTE_NONE,
                escapechar="\\",
            )
            writer.writeheader()

            for guid in sorted(os.listdir(metadata_cache_dir)):
                with open(f"{metadata_cache_dir}/{guid}") as f:
                    fetched_metadata = json.load(f)
                    flattened_tags = {
                        f"_tag_{tag_num}": f"{tag['category']}: {tag['name']}"
                        for tag_num, tag in enumerate(fetched_metadata.pop("tags", []))
                    }

                    output_metadata = {
                        k: json.dumps(v) if type(v) in [list, dict] else v
                        for k, v in {
                            **base_schema,
                            **fetched_metadata,
                            **flattened_tags,
                            "guid": guid,
                        }.items()
                    }
                    writer.writerow(output_metadata)

        return output_filename


async def publish_discovery_metadata(
    auth, metadata_filename, endpoint=None, omit_empty_values=False
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
        metadata_reader = csv.DictReader(
            metadata_file,
            delimiter=delimiter,
            quotechar="",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
        )
        tag_columns = [
            column for column in metadata_reader.fieldnames if "_tag_" in column
        ]
        pending_requests = []

        for metadata_line in metadata_reader:
            discovery_metadata = {
                key: _try_parse(value) for key, value in metadata_line.items()
            }

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

            guid = discovery_metadata.pop("guid")

            if omit_empty_values:
                discovery_metadata = {
                    key: value
                    for key, value in discovery_metadata.items()
                    if value not in ["", [], {}]
                }

            metadata = {
                "_guid_type": "discovery_metadata",
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


def _try_parse(data):
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return data
    return ""


def _metadata_file_from_auth(auth):
    return (
        "-".join(urlparse(auth.endpoint).netloc.split(".")) + "-discovery_metadata.tsv"
    )
