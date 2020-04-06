import os
import csv
from collections import OrderedDict
from pathlib import Path
import sys
import copy
import time
import base64
from datetime import datetime
import logging


def _get_guids_for_row(row, data_from_smaller_file, config, **kwargs):
    """
    Given a row from the manifest, return the guids that match it.

    Example:
        row = {"submitted_sample_id": "123", "foo": "bar", "fizz": "buzz"}
        data_from_smaller_file = {
            "123": {"guid": "aefdf8f2-9e96-4601-a8b9-c3f661b27bc8"},
            "456": {"guid": "56e908b2-12df-434e-be9b-023edf66814b"}
        }

    Returns:
        List: guids
    """
    # the name of column to return that exists as a value in the data_from_smaller_file
    guid_column_name = config.get("guid_column_name", "guid")

    # the key from the row to use for the exact match against the keys in
    # data_from_smaller_file
    row_key = config.get("row_column_name")
    key_id_from_row = row.get(row_key, "").strip()

    return [
        row.get(guid_column_name)
        for row in data_from_smaller_file.get(key_id_from_row, [])
        if row.get(guid_column_name)
    ]


def get_guids_for_row_partial_match(row, data_from_smaller_file, config, **kwargs):
    """
    Given a row from the manifest, return the guid to use for the metadata object by
    partially matching against the keys.

    WARNING: This iterates over the entire data_from_smaller_file dict EVERY TIME
             IT'S CALLED. So this is O(n2).

    Example:
        row = {"submitted_sample_id": "123", "foo": "bar", "fizz": "buzz"}
        data_from_smaller_file = {
            "123": {"guid": "aefdf8f2-9e96-4601-a8b9-c3f661b27bc8"},
            "456": {"guid": "56e908b2-12df-434e-be9b-023edf66814b"}
        }

    Returns:
        List: guids
    """
    # the name of column to return that exists as a value in the data_from_smaller_file
    guid_column_name = config.get("guid_column_name")

    # the key from the row to use for the exact match against the keys in
    # data_from_smaller_file
    row_key = config.get("row_column_name")
    key_from_row = row.get(row_key).strip()

    matching_guids = []
    matching_keys = []
    logging.info(
        f"{len(data_from_smaller_file)} unmatched records remaining in smaller file."
    )
    for key, matching_rows in data_from_smaller_file.items():
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
        del data_from_smaller_file[key]

    return matching_guids


def _get_data_from_smaller_file(manifest_file, config, delimiter="\t", **kwargs):
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
    key_column_name = config.get("smaller_file_column_name")
    value_column_names = [config.get("guid_column_name")]

    column_to_matching_rows = {}
    with open(manifest_file, "rt") as csvfile:
        csvReader = csv.DictReader(csvfile, delimiter=delimiter)
        for row in csvReader:
            key = str(row[key_column_name]).strip()
            column_to_matching_rows.setdefault(key, []).append(
                {item: row.get(item) for item in value_column_names}
            )

    logging.debug(
        f"sample data from smaller file: {str(column_to_matching_rows)[:250]}"
    )
    return column_to_matching_rows


manifest_row_parsers = {
    "guids_for_row": _get_guids_for_row,
    "get_data_from_smaller_file": _get_data_from_smaller_file,
}
manifests_mapping_config = {
    "guid_column_name": "guid",
    "row_column_name": "submitted_sample_id",
    "smaller_file_column_name": "sample_id",
}


def merge_guids_into_metadata(
    indexing_manifest,
    metadata_manifest,
    is_indexing_file_smaller=True,
    indexing_manifest_file_delimiter=None,
    metadata_manifest_file_delimiter=None,
    manifest_row_parsers=manifest_row_parsers,
    manifests_mapping_config=manifests_mapping_config,
    output_filename="merged-metadata-manifest.tsv",
):
    start_time = time.perf_counter()
    logging.info(f"start time: {start_time}")

    # if delimter not specified, try to get based on file ext
    if not indexing_manifest_file_delimiter:
        indexing_manifest_file_delimiter = _get_delimiter_from_extension(
            indexing_manifest
        )
    if not metadata_manifest_file_delimiter:
        metadata_manifest_file_delimiter = _get_delimiter_from_extension(
            metadata_manifest
        )

    # determine filenames and delimiters based on flag
    expected_smallest_file = (
        indexing_manifest if is_indexing_file_smaller else metadata_manifest
    )
    expected_smallest_file_delimiter = (
        indexing_manifest_file_delimiter
        if is_indexing_file_smaller
        else metadata_manifest_file_delimiter
    )
    other_file = metadata_manifest if is_indexing_file_smaller else indexing_manifest
    other_file_delimiter = (
        metadata_manifest_file_delimiter
        if is_indexing_file_smaller
        else indexing_manifest_file_delimiter
    )

    _warn_if_input_is_not_memory_efficient(
        indexing_manifest,
        metadata_manifest,
        is_indexing_file_smaller,
        expected_smallest_file,
    )

    logging.debug(f"Getting data from {expected_smallest_file} and loading into dict.")
    data_from_smaller_file = manifest_row_parsers["get_data_from_smaller_file"](
        expected_smallest_file,
        config=manifests_mapping_config,
        delimiter=expected_smallest_file_delimiter,
    )

    logging.debug(
        f"Iterating over {other_file} and finding guid using dict created "
        f"from {expected_smallest_file}."
    )
    with open(other_file, "rt") as file:
        reader = csv.DictReader(file, delimiter=other_file_delimiter)
        headers = ["guid"]
        headers.extend(reader.fieldnames)

        logging.debug(f"writing headers to {output_filename}: {headers}")
        write_header_to_file(
            filename=output_filename, fieldnames=headers, delimiter="\t"
        )

        logging.debug(f"beginning iteration over rows in {other_file}")
        for row in reader:
            guids = manifest_row_parsers["guids_for_row"](
                row, data_from_smaller_file, config=manifests_mapping_config
            )

            if not guids:
                # warning but write to output anyway
                # TODO should we not write to output?
                logging.warning(f"could not find matching guid for row: {row}")
            else:
                logging.debug(f"found guids {guids} matching row: {row}")

            for guid in guids:
                row.update({"guid": guid})
                append_row_to_file(
                    filename=output_filename,
                    row=row,
                    fieldnames=headers,
                    delimiter="\t",
                )

    end_time = time.perf_counter()
    logging.info(f"end time: {end_time}")
    logging.info(f"run time: {end_time-start_time}")


def _warn_if_input_is_not_memory_efficient(
    indexing_manifest,
    metadata_manifest,
    is_indexing_file_smaller,
    expected_smallest_file,
):
    """
    Warn user if their input isn't the most memory efficient configuration.

    Args:
        expected_smallest_file (str): filename of file expected to be the smallest
    """
    # determine the smallest file, we'll load this one into a python dict (in memory)
    # and iterate over the other one line by line to save memory
    indexing_file_size = Path(indexing_manifest).stat().st_size
    metadata_file_size = Path(metadata_manifest).stat().st_size

    smallest_file = (
        metadata_manifest
        if min(indexing_file_size, metadata_file_size) == indexing_file_size
        else indexing_manifest
    )

    if smallest_file != expected_smallest_file:
        logging.warning(
            f"Warning: memory may be used inefficiently.\n"
            f"The script expected {expected_smallest_file} to be a smaller file.\n"
            f"{indexing_manifest} size: {indexing_file_size}.\n"
            f"{metadata_manifest} size: {indexing_file_size}.\n"
            f"is_indexing_file_smaller set to {is_indexing_file_smaller}.\n"
            f"Consider setting that to the opposite. Script\n"
            f"will be loading {expected_smallest_file} fully into memory."
        )


def _get_delimiter_from_extension(filename):
    file_ext = os.path.splitext(filename)
    if file_ext[-1].lower() == ".tsv":
        file_delimiter = "\t"
    else:
        # default, assume CSV
        file_delimiter = ","
    return file_delimiter


def write_header_to_file(filename, fieldnames, delimiter="\t"):
    """
    Writes to a file in TSV format.

    Returns:
        None
    """
    with open(filename, mode="w+") as outfile:
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
    with open(filename, mode="a") as outfile:
        writer = csv.DictWriter(
            outfile, delimiter=delimiter, fieldnames=fieldnames, extrasaction="ignore"
        )
        writer.writerow(row)
