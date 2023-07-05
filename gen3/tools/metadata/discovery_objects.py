import csv
import json
import tempfile
import asyncio
import os
import requests.exceptions
from cdislogging import get_logger
from urllib.parse import urlparse

from gen3.metadata import Gen3Metadata

from gen3.tools.metadata.discovery import (
    MAX_GUIDS_PER_REQUEST,
    MAX_CONCURRENT_REQUESTS,
    BASE_CSV_PARSER_SETTINGS,
    read_mds_into_cache,
    get_discovery_metadata,
    sanitize_tsv_row,
)

REQUIRED_OBJECT_FIELDS = {"dataset_guid", "guid", "display_name"}

logging = get_logger("__name__")


async def output_discovery_objects(
    auth,
    dataset_guids=None,
    endpoint=None,
    limit=500,
    use_agg_mds=False,
    guid_type="discovery_metadata",
    only_object_guids=False,
    template=False,
    output_format="tsv",
    output_filename_suffix="",
):
    """
    fetch discovery objects data from a commons and output to {commons}-discovery_objects.tsv or {commons}-discovery_objects.json

    Args:
        auth (Gen3Auth): a Gen3Auth object
        dataset_guids (list of str): a list of datasets to read objects from
        endpoint (str): HOSTNAME of a Gen3 environment, defaults to None
        limit (int): max number of records in one operation, defaults to 500
        use_agg_mds (bool): whether to use AggMDS during export, defaults to False
        guid_type (str): intended GUID type for query, defaults to discovery_metadata
        only_objects_guids (bool): whether to output guids to command line such that they can be piped to another command
        template (bool): whether to output a file with just required headers, to be filled out and then published
        output_format (str): format of output file (can only be either tsv or json), defaults to tsv
        output_filename_suffix (str): additional suffix for the output file name, defaults to ""
    """
    if template:
        output_filename = _create_discovery_objects_filename(auth, "TEMPLATE", ".tsv")
        object_fields = ["guid", "dataset_guid", "display_name"]
        with open(output_filename, "w+", encoding="utf-8") as output_file:
            writer = csv.DictWriter(
                output_file,
                **{**BASE_CSV_PARSER_SETTINGS, "fieldnames": object_fields},
            )
            writer.writeheader()
        return output_filename

    if output_format != "tsv" and output_format != "json":
        logging.error(
            f"Unsupported output file format {output_format}! Only tsv or json is allowed"
        )
        raise ValueError(f"Unsupported output file format {output_format}")

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

    with tempfile.TemporaryDirectory() as metadata_cache_dir:
        partial_metadata, all_fields, num_tags = read_mds_into_cache(
            limit,
            MAX_GUIDS_PER_REQUEST,
            mds,
            guid_type,
            use_agg_mds,
            metadata_cache_dir,
        )

        # output to command line
        if only_object_guids:
            return_objects = []
            for guid in sorted(os.listdir(metadata_cache_dir)):
                if (not dataset_guids) or (guid in dataset_guids):
                    with open(f"{metadata_cache_dir}/{guid}", encoding="utf-8") as f:
                        fetched_metadata = json.load(f)
                        return_objects += (
                            fetched_metadata["objects"]
                            if "objects" in fetched_metadata.keys()
                            else []
                        )
            return return_objects

        # output as TSV
        elif output_format == "tsv":
            output_filename = _create_discovery_objects_filename(
                auth, output_filename_suffix, ".tsv"
            )
            object_fields = REQUIRED_OBJECT_FIELDS.copy()
            all_objects = []

            for guid in sorted(os.listdir(metadata_cache_dir)):
                if (not dataset_guids) or (guid in dataset_guids):
                    with open(f"{metadata_cache_dir}/{guid}", encoding="utf-8") as f:
                        fetched_metadata = json.load(f)
                        curr_objects = (
                            fetched_metadata["objects"]
                            if ("objects" in fetched_metadata.keys())
                            else []
                        )
                        for obj in curr_objects:
                            object_fields |= set(obj.keys())
                            obj["dataset_guid"] = guid
                        all_objects += curr_objects
            object_fields.remove("guid")
            object_fields = sorted(object_fields)
            object_fields.insert(0, "guid")

            with open(output_filename, "w+", encoding="utf-8") as output_file:
                writer = csv.DictWriter(
                    output_file,
                    **{**BASE_CSV_PARSER_SETTINGS, "fieldnames": object_fields},
                )
                writer.writeheader()
                for obj in all_objects:
                    output_metadata = sanitize_tsv_row(obj)
                    writer.writerow(output_metadata)

        else:
            # output as JSON
            output_filename = _create_discovery_objects_filename(
                auth, output_filename_suffix, ".json"
            )
            output_metadata = []
            dataset_guids = dataset_guids if dataset_guids else partial_metadata.keys()
            for guid in dataset_guids:
                true_guid = guid
                if use_agg_mds:
                    true_guid = guid.split("__")[1]
                metadata = partial_metadata[guid]["gen3_discovery"]["objects"]
                for obj in metadata:
                    output_metadata.append({"dataset_guid": true_guid, **obj})

            with open(output_filename, "w+", encoding="utf-8") as output_file:
                output_file.write(json.dumps(output_metadata, indent=4))

        return output_filename


async def publish_discovery_object_metadata(
    auth,
    metadata_filename,
    endpoint=None,
    guid_type="discovery_metadata",
    overwrite=False,
):
    """
    Publish discovery objects from a TSV file

    Args:
        auth (Gen3Auth): a Gen3Auth object
        metadata_filename (str): the file path of the local objects file to be published, must be in TSV format
        endpoint (str): HOSTNAME of a Gen3 environment, defaults to None
        guid_type (str): intended GUID type for publishing, defaults to discovery_metadata
        overwrite (bool): whether to allow replacing objects to a dataset_guid instead of appending
    """
    if endpoint:
        mds = Gen3Metadata(auth_provider=auth, endpoint=endpoint)
    else:
        mds = Gen3Metadata(auth_provider=auth)

    if not metadata_filename.endswith(".tsv"):
        logging.error(
            f"Unsupported file type supplied {metadata_filename}! Only TSV are allowed."
        )
        raise ValueError(f"Unsupported file type supplied {metadata_filename}")

    delimiter = "\t"
    with open(metadata_filename, encoding="utf-8") as metadata_file:
        csv_parser_setting = {**BASE_CSV_PARSER_SETTINGS, "delimiter": delimiter}
        metadata_reader = csv.DictReader(metadata_file, **{**csv_parser_setting})
        pending_requests = []
        dataset_dict = {}

        for obj_line in metadata_reader:
            # if required fields are missing, display error
            required_fields = list(REQUIRED_OBJECT_FIELDS)
            missing_fields = []
            for field in required_fields:
                if not obj_line[field]:
                    missing_fields.append(field)
            if missing_fields:
                logging.error(f"Missing required field(s) {missing_fields}.")
                raise ValueError(f"Required field(s) missing: {missing_fields}")

            dataset_guid = obj_line.pop("dataset_guid")
            if dataset_guid not in dataset_dict:
                dataset_dict[dataset_guid] = {"objects": []}
            dataset_dict[dataset_guid]["objects"].append(obj_line)

        for dataset_guid in dataset_dict.keys():
            # if dataset_guid already exists, update (noting the use of --overwrite), if it doesn’t already exist, create it
            try:
                curr_dataset_metadata = mds.get(dataset_guid)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    curr_dataset_metadata = get_discovery_metadata(
                        provided_metadata={}, guid_type=guid_type
                    )
                else:
                    raise

            # allow replacing instead of appending
            if overwrite or not (
                "objects" in curr_dataset_metadata["gen3_discovery"].keys()
            ):
                curr_dataset_metadata["gen3_discovery"]["objects"] = dataset_dict[
                    dataset_guid
                ]["objects"]
            else:
                curr_dataset_metadata["gen3_discovery"]["objects"] += dataset_dict[
                    dataset_guid
                ]["objects"]

            pending_requests += [
                mds.async_create(dataset_guid, curr_dataset_metadata, overwrite=True)
            ]
            if len(pending_requests) == MAX_CONCURRENT_REQUESTS:
                await asyncio.gather(*pending_requests)
                pending_requests = []
        await asyncio.gather(*pending_requests)


def try_delete_discovery_objects(auth, delete_args):
    """
    Delete discovery objects from a TSV file of objects, or all objects from one or more datasets

    Args:
        auth (Gen3Auth): a Gen3Auth object
        delete_args (list of str): a TSV file of objects, or a list of datasets
    """
    mds = Gen3Metadata(auth_provider=auth)
    for arg in delete_args:
        # delete objects from tsv file
        if arg[-4:].lower() == ".tsv":
            dataset_dict = {}
            # read object guids to delete into a dict, batch deletes by dataset guid for efficiency
            with open(arg, encoding="utf-8") as tsv:
                tsv_reader = csv.DictReader(tsv, delimiter="\t")
                for row in tsv_reader:
                    dataset_guid = row["dataset_guid"]
                    guid = row["guid"]
                    dataset_dict.setdefault(dataset_guid, set()).add(guid)
            for dataset_guid, objects_to_delete in dataset_dict.items():
                try:
                    metadata = mds.get(dataset_guid)
                    if "objects" in metadata["gen3_discovery"].keys():
                        curr_objects = metadata["gen3_discovery"]["objects"]
                        curr_objects = [
                            obj
                            for obj in curr_objects
                            if (obj["guid"] not in objects_to_delete)
                        ]
                        metadata["gen3_discovery"]["objects"] = curr_objects
                        mds.create(dataset_guid, metadata, overwrite=True)
                except requests.exceptions.HTTPError as e:
                    logging.warning(e)
        else:
            # delete all objects from dataset_guid
            try:
                metadata = mds.get(arg)
                if metadata["_guid_type"] == "discovery_metadata":
                    if "objects" in metadata["gen3_discovery"].keys():
                        metadata["gen3_discovery"]["objects"] = []
                        mds.create(arg, metadata, overwrite=True)
                else:
                    logging.warning(f"{guid} is not discovery metadata. Skipping.")
            except requests.exceptions.HTTPError as e:
                logging.warning(e)


def _create_discovery_objects_filename(auth, suffix="", file_extension=".tsv"):
    return (
        "-".join(urlparse(auth.endpoint).netloc.split("."))
        + "-discovery_objects"
        + (f"-{suffix}" if suffix else "")
        + file_extension
    )
