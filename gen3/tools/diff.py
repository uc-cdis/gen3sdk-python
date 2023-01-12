"""
Takes the differential between the matching columns of two manifests

TODO: Able to handle situations where only specific columns should be compared
"""

import os
import csv
import sys
import traceback

from cdislogging import get_logger

logging = get_logger("__name__")


def manifest_diff(
    directory=".",
    files=None,
    key_column="id",
    allow_additional_columns=False,
    output_manifest_file_delimiter=None,
    output_manifest="diff-manifest.tsv",
    **kwargs,
):
    """
    Args:
        directory(str):
            Path of the directory containing the input manifests. All of the manifests contained in directory are assumed to be in a delimiter-separated values (DSV) format, and that there are no other non-DSV files in directory.
        files(list[str]):
            List of paths containing the input manifests. All of the manifests contained in directory are assumed to be in a delimiter-separated values (DSV) format, and that there are no other non-DSV files in directory
        key_column(str):
            column of unique identifier in manifest
        manifest_content(dict(dict)):
            Dict of manifest content being compared
        allow_additional_columns(boolean):
            Bool allowing manifests to have different columns
        output_manifest_file_delimiter(str):
            The delimiter used for writing the output manifest. If not provided, the delimiter will be determined
            based on the file extension of output_manifest
        output_manifest(str):
            The file to write the output manifest to

    Returns:
        None
    """

    files = files or []
    if not files:
        logging.info(f"Iterating over manifests in {directory} directory")
        for file in sorted(os.listdir(directory)):
            files.append(os.path.join(directory, file))

    files.sort()

    content = _precheck_manifests(
        allow_additional_columns=allow_additional_columns,
        files=files,
        key_column=key_column,
    )

    diff_content = _compare_manifest_columns(
        allow_additional_columns=allow_additional_columns,
        manifest_content=content,
    )

    _write_csv(
        output_manifest_file_delimiter=output_manifest_file_delimiter,
        output_manifest=output_manifest,
        diff_content=diff_content,
    )


def _precheck_manifests(
    allow_additional_columns,
    key_column,
    files=[],
    **kwargs,
):
    """
    Precheck of all manifests for:
    - two files given
    - files share same extension
    - if additional columns not allowed, check for all headers are matching between two files

    Args:
        allow_additional_columns(bool)
        files(list[str])
        key_column(str)

    Returns:
        if pass all checks, dict(list, list): CSV content and headers
        else, bool False

        {
            csvdict:{}
                "id1": [{"header1": "", "header2": "", ...}],
                "id2": [{}],
                ...
            },
            headers: ["header1", "header2", ...]
        }
    """

    logging.info(f"Prechecking manifest files: {files}")

    if not len(files) == 2:
        raise Exception("Must take difference of two files, check dir for hidden files")

    tsv_files = [file_name for file_name in files if ".tsv" in file_name.lower()]
    csv_files = [file_name for file_name in files if ".csv" in file_name.lower()]
    if len(tsv_files) == len(files):
        file_delimiter = "\t"
    elif len(csv_files) == len(files):
        file_delimiter = ","
    else:
        raise ValueError("Not all files have the same extension type")

    headers = []
    manifest_content = []
    for manifest in files:
        with open(manifest, "r", encoding="utf-8-sig") as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=file_delimiter)

            field_names = csv_reader.fieldnames
            logging.debug(f"Field names from {manifest}: {field_names}")
            headers.append(field_names)
            if len(headers) == 2:
                if not allow_additional_columns:
                    if not headers[0] == headers[1]:
                        raise Exception(
                            f"Headers are not the same among manifests. {set(headers[1]) ^ set(headers[0])}"
                        )

            content = {}
            for row in csv_reader:
                for column in row:
                    if column == "acl":
                        row[column] = str(sorted(eval(row[column])))
                content[row[key_column]] = row

            manifest_content.append(content)

    return {
        "csvdict": manifest_content,
        "headers": [x for x in headers[0] if x in headers[1]],
    }


def _compare_manifest_columns(
    allow_additional_columns,
    manifest_content={},
    **kwargs,
):
    """
    Args:
        manifest_content(dict(list, list)): Dict of CSV list and headers set
        allow_additional_columns(bool)

    Returns:
        Dict containing dict of diff list and headers list
    """

    headers = manifest_content["headers"]
    # TODO ability to only have certain headers compared
    dict1 = manifest_content["csvdict"][0]
    dict2 = manifest_content["csvdict"][1]

    diff_content = [dict2[i] for i in dict2 if i not in dict1 or dict2[i] != dict1[i]]

    return {"headers": headers, "csvdict": diff_content}


def _write_csv(output_manifest_file_delimiter, output_manifest, diff_content={}):
    """
    write to csv file

    Args:
        output_manifest_file_delimiter(str): file name
        output_manifest: file name with extension
        diff_content(dict(list)): dict of file info

    Returns:
        None.
        Writes a manifest file of the diff between manifests
    """

    if output_manifest_file_delimiter is None:
        output_manifest_file_ext = os.path.splitext(output_manifest)
        if output_manifest_file_ext[-1].lower() == ".tsv":
            output_manifest_file_delimiter = "\t"
        else:
            output_manifest_file_delimiter = ","

    headers = diff_content["headers"]

    logging.info(f"Writing diff manifest to {output_manifest}")
    with open(output_manifest, "w") as outputfile:
        output_writer = csv.DictWriter(
            outputfile,
            delimiter=output_manifest_file_delimiter,
            fieldnames=headers,
        )
        output_writer.writeheader()

        for record in diff_content["csvdict"]:
            output_writer.writerow(record)

        logging.info(f"Finished writing merged manifest to {output_manifest}")
