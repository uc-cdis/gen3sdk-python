import os
import logging
import csv

from collections import OrderedDict


def merge_bucket_manifests(
    directory=".",
    merge_column="md5",
    output_manifest_file_delimiter=None,
    output_manifest="merged-bucket-manifest.tsv",
):
    """
    Merge all of the input manifests in the provided directory into a single
    output manifest. Files contained in the input manifests are merged on the
    basis of a common hash (i.e. merge_column). The url and authz values for
    matching input files are concatenated with spaces in the merged output file
    record.

    Args:
        directory(str): path of the directory containing the input manifests.
        all of the manifests contained in directory are assumed to be in a
        delimiter-separated values (DSV) format, and that there are no other
        non-DSV files in directory.
        merge_column(str): the common hash used to merge files. it is unique
        for every file in the output manifest
        output_manifest_file_delimiter(str): the delimiter used for writing the
        output manifest. if not provided, the delimiter will be determined
        based on the file extension of output_manifest
        output_manifest(str): the file to write the output manifest to

    Returns:
        None
    """
    all_rows = {}
    logging.info(f"Iterating over manifests in {directory} directory")
    for input_manifest in os.listdir(directory):
        with open(os.path.join(directory, input_manifest)) as csv_file:
            dialect = csv.Sniffer().sniff(csv_file.readline())
            csv_file.seek(0)

            logging.info(
                f"Reading, parsing, and merging files from {input_manifest} manifest"
            )
            csv_reader = csv.reader(csv_file, dialect)
            headers = OrderedDict([(h, i) for i, h in enumerate(next(csv_reader))])
            for row in csv_reader:
                _merge_row(all_rows, headers, row, merge_column)

    if output_manifest_file_delimiter is None:
        output_manifest_file_ext = os.path.splitext(output_manifest)
        if output_manifest_file_ext[-1].lower() == ".tsv":
            output_manifest_file_delimiter = "\t"
        else:
            output_manifest_file_delimiter = ","

    with open(output_manifest, "w") as csv_file:
        logging.info(f"Writing merged manifest to {output_manifest}")
        csv_writer = csv.writer(csv_file, delimiter=output_manifest_file_delimiter)
        csv_writer.writerow(headers.keys())

        for hash_code in all_rows:
            csv_writer.writerow(all_rows[hash_code])


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
    hash_code = row_to_merge[headers[merge_column]]
    if hash_code in all_rows:
        size = row_to_merge[headers["size"]]
        if size != all_rows[hash_code][headers["size"]]:
            raise csv.Error(
                "Found two objects with the same hash but different sizes,"
                f" could not merge. Details: object {row_to_merge} could not be"
                f" merged with object {all_rows[hash_code]} because {size} !="
                f" {all_rows[hash_code][headers['size']]}."
            )

        url = row_to_merge[headers["url"]]
        if url not in all_rows[hash_code][headers["url"]]:
            all_rows[hash_code][headers["url"]] += f" {url}"

        authz = row_to_merge[headers["authz"]]
        if authz not in all_rows[hash_code][headers["authz"]]:
            all_rows[hash_code][headers["authz"]] += f" {authz}"
    else:
        all_rows[hash_code] = row_to_merge
