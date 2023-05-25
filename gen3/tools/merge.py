import os
import csv
from collections import OrderedDict
from pathlib import Path
import sys
import copy
import time
import base64
from datetime import datetime
from cdislogging import get_logger

from gen3.utils import get_delimiter_from_extension

logging = get_logger("__name__")


def _get_guids_for_manifest_row(row, data_from_indexing_manifest, config, **kwargs):
    """
    Given a row from the metadata manifest, return the guids that match it based on
    whatever key was used.

    Example:
        row = {"submitted_sample_id": "123", "foo": "bar", "fizz": "buzz"}
        data_from_indexing_manifest = {
            "123": [{"guid": "aefdf8f2-9e96-4601-a8b9-c3f661b27bc8"}],
            "456": [{"guid": "56e908b2-12df-434e-be9b-023edf66814b"}]
        }

    Returns:
        List: guids
    """
    # the name of column to return that exists as a value in the data_from_indexing_manifest
    guid_column_name = config.get("guid_column_name", "guid")

    # the key from the row to use for the exact match against the keys in
    # data_from_indexing_manifest
    row_key = config.get("row_column_name")
    key_id_from_row = row.get(row_key, "").strip()

    return [
        row.get(guid_column_name)
        for row in data_from_indexing_manifest.get(key_id_from_row, [])
        if row.get(guid_column_name)
    ]


def get_guids_for_manifest_row_partial_match(
    row, data_from_indexing_manifest, config, **kwargs
):
    """
    Given a row from the manifest, return the guid to use for the metadata object by
    partially matching against the keys.

    WARNING: This iterates over the entire data_from_indexing_manifest dict EVERY TIME
             IT'S CALLED. So this is O(n2).

    ANOTHER WARNING: This does not support GUIDs matching multiple rows
                     of metadata, it only supports metadata matching multiple
                     GUIDs.

    Example:
        row = {"submitted_sample_id": "123", "foo": "bar", "fizz": "buzz"}
        data_from_indexing_manifest = {
            "123": {"guid": "aefdf8f2-9e96-4601-a8b9-c3f661b27bc8"},
            "456": {"guid": "56e908b2-12df-434e-be9b-023edf66814b"}
        }

    Returns:
        List: guids
    """
    # the name of column to return that exists as a value in the data_from_indexing_manifest
    guid_column_name = config.get("guid_column_name")

    # the key from the row to use for the partial match against the keys in
    # data_from_indexing_manifest
    row_key = config.get("row_column_name")
    key_from_row = row.get(row_key, "").strip()

    matching_guids = []
    matching_keys = []
    logging.info(
        f"{len(data_from_indexing_manifest)} unmatched records remaining in indexing manifest file."
    )
    for key, matching_rows in data_from_indexing_manifest.items():
        if key_from_row in key and matching_rows:
            matching_keys.append(key)
            matching_guids.extend(
                [
                    row.get(guid_column_name)
                    for row in matching_rows
                    if row.get(guid_column_name)
                ]
            )

    # no need to search already matched records
    for key in matching_keys:
        del data_from_indexing_manifest[key]

    return matching_guids


def _get_data_from_indexing_manifest(
    manifest_file,
    config,
    delimiter="\t",
    include_all_indexing_cols_in_output=True,
    **kwargs,
):
    """
    Create an OrderedDictionary mapping some key to a list of matching records with some
    field to use as a GUID.

    Args:
        manifest_file (string)
        delimiter (string): delimiter used to separate entries in the file. for a tsv,
            this is \t

    Returns:
        column_to_matching_rows (dict): maps a key to a list and appends data from rows
            with matching columns
    """
    key_column_name = config.get("indexing_manifest_column_name")

    column_to_matching_rows = {}
    with open(manifest_file, "rt", encoding="utf-8-sig") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=delimiter)

        value_column_names = [item for item in [config.get("guid_column_name")] if item]

        if include_all_indexing_cols_in_output:
            value_column_names = value_column_names + csvReader.fieldnames

        for row in csvReader:
            key = str(row[key_column_name]).strip()
            column_to_matching_rows.setdefault(key, []).append(
                {item: row.get(item) for item in value_column_names}
            )

    logging.debug(
        f"sample data from indexing manifest file: {str(column_to_matching_rows)[:250]}"
    )
    return column_to_matching_rows


manifest_row_parsers = {
    "guids_for_manifest_row": _get_guids_for_manifest_row,
    "get_data_from_indexing_manifest": _get_data_from_indexing_manifest,
}
manifests_mapping_config = {
    "guid_column_name": "guid",
    "row_column_name": "submitted_sample_id",
    "indexing_manifest_column_name": "sample_id",
}


def merge_guids_into_metadata(
    indexing_manifest,
    metadata_manifest,
    indexing_manifest_file_delimiter=None,
    metadata_manifest_file_delimiter=None,
    manifest_row_parsers=manifest_row_parsers,
    manifests_mapping_config=manifests_mapping_config,
    output_filename="merged-metadata-manifest.tsv",
    include_all_indexing_cols_in_output=True,
):
    start_time = time.perf_counter()
    logging.debug(f"start time: {start_time}")

    # if delimiter not specified, try to get based on file ext
    if not indexing_manifest_file_delimiter:
        indexing_manifest_file_delimiter = get_delimiter_from_extension(
            indexing_manifest
        )
    if not metadata_manifest_file_delimiter:
        metadata_manifest_file_delimiter = get_delimiter_from_extension(
            metadata_manifest
        )

    logging.debug(f"Getting data from {indexing_manifest} and loading into dict.")
    data_from_indexing_manifest = manifest_row_parsers[
        "get_data_from_indexing_manifest"
    ](
        indexing_manifest,
        config=manifests_mapping_config,
        delimiter=indexing_manifest_file_delimiter,
        include_all_indexing_cols_in_output=include_all_indexing_cols_in_output,
    )

    logging.debug(
        f"Iterating over {metadata_manifest} and finding matches using dict created "
        f"from {indexing_manifest}."
    )
    with open(metadata_manifest, "rt", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file, delimiter=metadata_manifest_file_delimiter)
        headers = ["guid"]
        headers.extend(reader.fieldnames)

        if include_all_indexing_cols_in_output:
            with open(indexing_manifest, "rt", encoding="utf-8-sig") as csvfile:
                indexing_reader = csv.DictReader(
                    csvfile, delimiter=indexing_manifest_file_delimiter
                )
                headers.extend(indexing_reader.fieldnames)

        # remove any duplicates but preserve order
        unique_headers = []
        for header in headers:
            if header not in unique_headers:
                unique_headers.append(header)
        headers = unique_headers

        logging.debug(f"writing headers to {output_filename}: {headers}")
        write_header_to_file(
            filename=output_filename, fieldnames=headers, delimiter="\t"
        )

        logging.debug(f"beginning iteration over rows in {metadata_manifest}")
        for row in reader:
            guids = manifest_row_parsers["guids_for_manifest_row"](
                row, data_from_indexing_manifest, config=manifests_mapping_config
            )

            if not guids:
                # warning but write to output anyway
                row.update({"guid": ""})

                if include_all_indexing_cols_in_output:
                    row_key = manifests_mapping_config.get("row_column_name")
                    key_id_from_row = row.get(row_key, "").strip()
                    rows_to_add = data_from_indexing_manifest.get(key_id_from_row, {})

                    for new_row in rows_to_add:
                        for key, value in row.items():
                            # only replace if there's content
                            if value:
                                new_row.update({key: value})
                        append_row_to_file(
                            filename=output_filename,
                            row=new_row,
                            fieldnames=headers,
                            delimiter="\t",
                        )
                else:
                    append_row_to_file(
                        filename=output_filename,
                        row=row,
                        fieldnames=headers,
                        delimiter="\t",
                    )
            else:
                logging.debug(f"found guids {guids} matching row: {row}")

            for guid in guids:
                row.update({"guid": guid})

                if include_all_indexing_cols_in_output:
                    row_key = manifests_mapping_config.get("row_column_name")
                    key_id_from_row = row.get(row_key, "").strip()
                    for new_row in data_from_indexing_manifest.get(key_id_from_row, {}):
                        if new_row.get("guid") == guid:
                            for key, value in row.items():
                                # only replace if there's content
                                if value:
                                    new_row.update({key: value})
                            append_row_to_file(
                                filename=output_filename,
                                row=new_row,
                                fieldnames=headers,
                                delimiter="\t",
                            )
                else:
                    append_row_to_file(
                        filename=output_filename,
                        row=row,
                        fieldnames=headers,
                        delimiter="\t",
                    )

    end_time = time.perf_counter()
    logging.debug(f"end time: {end_time}")
    logging.debug(f"run time: {end_time-start_time}")

    logging.debug(f"output file:\n{os.path.abspath(output_filename)}")


def write_header_to_file(filename, fieldnames, delimiter="\t"):
    """
    Writes to a file in TSV format.

    Returns:
        None
    """
    with open(filename, mode="w+", encoding="utf-8-sig") as outfile:
        writer = csv.DictWriter(
            outfile, delimiter=delimiter, fieldnames=fieldnames, extrasaction="ignore"
        )
        writer.writeheader()


def append_row_to_file(filename, row, fieldnames, delimiter="\t"):
    """
    Appends to a file in TSV format.

    Returns:
        None
    """
    with open(filename, mode="a", encoding="utf-8-sig") as outfile:
        writer = csv.DictWriter(
            outfile, delimiter=delimiter, fieldnames=fieldnames, extrasaction="ignore"
        )
        writer.writerow(row)
