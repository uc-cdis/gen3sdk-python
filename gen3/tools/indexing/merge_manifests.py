import os
import sys
import glob

import logging
import csv

from collections import OrderedDict


def merge_bucket_manifests(
    directory=".",
    manifest_extension="tsv",
    delimiter="\t",
    merge_column="md5",
    output_manifest="merged-bucket-manifest.tsv",
):
    """
    Merge all of the input manifests in the provided directory into a single
    output manifest. Files contained in the input manifests are merged on the
    basis of a common hash (i.e. merge_column). The url and authz values for
    matching input files are concatenated with spaces in the merged output file
    record.

    Args:
        directory(str): path of the directory containing the input manifests
        manifest_extension(str): the extension for the input manifests
        delimiter(str): the delimiter that should be used for reading the input
        and writing the output manifests
        merge_column(str): the common hash used to merge files. it is unique
        for every file in the output manifest
        output_manifest(str): the file to write the output manifest to

    Returns:
        None
    """
    all_rows = {}
    logging.info(f"Iterating over manifests in {directory} directory")
    for input_manifest in glob.glob(os.path.join(directory, f"*.{manifest_extension}")):
        with open(input_manifest) as f:
            logging.info(
                f"Reading, parsing, and merging files from {input_manifest} manifest"
            )
            total_row_count = sum(1 for row in f)
            f.seek(0)

            reader = csv.reader(f, delimiter=delimiter)
            headers = OrderedDict([(h, i) for i, h in enumerate(next(reader))])
            for i, row in enumerate(reader):
                _merge_row(all_rows, headers, row, merge_column)
                if i % 1000 == 0 or i + 2 == total_row_count:
                    _update_progress(i + 2, total_row_count)
            logging.info("")

    with open(output_manifest, "w") as csvfile:
        logging.info(f"Writing merged manifest to {output_manifest}")
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(headers.keys())

        total_row_count = len(all_rows)
        for i, hashh in enumerate(all_rows):
            writer.writerow(all_rows[hashh])
            if i % 1000 == 0 or i + 2 == total_row_count:
                _update_progress(i + 2, total_row_count)
        logging.info("")


def _merge_row(all_rows, headers, row_to_merge, merge_column):
    """
    Update all_rows with row_to_merge.

    Args:
        all_rows(dict): maps a merge_column value to a single merged row
        headers(OrderedDict): maps manifest headers to index of each header
        (e.g for "url, size, md5, authz", headers would be { "url": 0, "size":
        1, "md5": 2, "authz": 3 })
        row_to_merge(list(str)): the row to update all_rows with
        merge_column(str): the header of the column that is used to merge rows
        (e.g. "md5")

    Returns:
        None

    """
    hashh = row_to_merge[headers[merge_column]]
    if hashh in all_rows:
        size = row_to_merge[headers["size"]]
        if size != all_rows[hashh][headers["size"]]:
            raise csv.Error(
                f"Could not merge file with {merge_column} equal to {hashh} while reading {input_manifest} because of size mismatch."
            )

        url = row_to_merge[headers["url"]]
        if url not in all_rows[hashh][headers["url"]]:
            all_rows[hashh][headers["url"]] += f" {url}"

        authz = row_to_merge[headers["authz"]]
        if authz not in all_rows[hashh][headers["authz"]]:
            all_rows[hashh][headers["authz"]] += f" {authz}"
    else:
        all_rows[hashh] = row_to_merge


def _update_progress(rows_read, total_row_count):
    """
    Print progress bar and percentage to STDOUT for how much of a file has been
    read from or written to.

    Args:
        rows_read(int): the number of rows read so far in file
        total_row_count(int): the total number of rows in file

    Returns:
        None

    """
    progress = int(rows_read / total_row_count * 100)
    pound_signs = progress // 10
    print(f"\r[{'#'*(pound_signs)}{' '*(10-pound_signs)}] {progress}%", end="")
