import os
import logging
import csv
import copy

from collections import OrderedDict
from gen3.tools.indexing.index_manifest import get_and_verify_fileinfos_from_manifest
from gen3.tools.indexing.manifest_columns import (
    GUID_STANDARD_KEY,
    SIZE_STANDARD_KEY,
    MD5_STANDARD_KEY,
    ACL_STANDARD_KEY,
    URLS_STANDARD_KEY,
    AUTHZ_STANDARD_KEY,
)


def merge_bucket_manifests(
    directory=".",
    files=None,
    output_manifest_file_delimiter=None,
    output_manifest="merged-bucket-manifest.tsv",
    continue_after_error=False,
    allow_mult_guids_per_hash=False,
    **kwargs,
):
    """
    Merge all of the input manifests in the provided directory into a single
    output manifest. Files contained in the input manifests are merged on the
    basis of a common hash. The url and authz values for
    matching input files are concatenated with spaces in the merged output file
    record.

    Args:
        directory(str): path of the directory containing the input manifests.
            all of the manifests contained in directory are assumed to be in a
            delimiter-separated values (DSV) format, and that there are no other
            non-DSV files in directory.
        files(list[str]): list of paths containing the input manifests.
            all of the manifests contained in directory are assumed to be in a
            delimiter-separated values (DSV) format, and that there are no other
            non-DSV files in directory.
        output_manifest_file_delimiter(str): the delimiter used for writing the
            output manifest. if not provided, the delimiter will be determined
            based on the file extension of output_manifest
        output_manifest(str): the file to write the output manifest to
        continue_after_error(bool): whether or not to continue merging even after a "critical"
            error like 2 different GUIDs with same md5
        allow_mult_guids_per_hash(bool): allows multiple records with the same
            md5 to exist in the final manifest if they have diff guids.
            Use this with EXTREME caution as the purpose
            of this code is to combine such entries, however, in cases where you have
            existing GUIDs with the same md5 but still want to merge manifests
            together, this can be used.

    Returns:
        None
    """
    files = files or []
    if not files:
        logging.info(f"Iterating over manifests in {directory} directory")
        for file in sorted(os.listdir(directory)):
            files.append(os.path.join(directory, file))

    logging.info(f"Merging files: {files}")

    headers = set()
    all_rows = {}
    for manifest in files:
        records_from_file, _ = get_and_verify_fileinfos_from_manifest(
            manifest, include_additional_columns=True
        )
        for record in records_from_file:
            record_to_write = copy.deepcopy(record)
            if record[MD5_STANDARD_KEY] in all_rows:
                previous_guid_exists = False
                # if the record already exists, let's start with existing data and
                # update as needed
                record_to_write = copy.deepcopy(all_rows[record[MD5_STANDARD_KEY]][-1])

                if GUID_STANDARD_KEY in record:
                    guid = record[GUID_STANDARD_KEY]
                    if (
                        guid
                        and record_to_write.get(GUID_STANDARD_KEY)
                        and guid != record_to_write.get(GUID_STANDARD_KEY)
                    ):
                        error_msg = (
                            "Found two objects with the same hash but different guids,"
                            f" could not merge. Details: object {record} could not be"
                            f" merged with object {record_to_write} because {guid} !="
                            f" {record_to_write.get(GUID_STANDARD_KEY)}."
                        )
                        logging.error(error_msg)

                        if not continue_after_error and not allow_mult_guids_per_hash:
                            raise csv.Error(error_msg)

                        previous_guid_exists = True

                    if guid:
                        record_to_write[GUID_STANDARD_KEY] = guid

                if SIZE_STANDARD_KEY in record:
                    size = record[SIZE_STANDARD_KEY]

                    if size != record_to_write[SIZE_STANDARD_KEY]:
                        error_msg = (
                            "Found two objects with the same hash but different sizes,"
                            f" could not merge. Details: object {record} could not be"
                            f" merged with object {record_to_write} because {size} !="
                            f" {record_to_write[SIZE_STANDARD_KEY]}."
                        )
                        logging.error(error_msg)

                        if not continue_after_error:
                            raise csv.Error(error_msg)

                # if there's a prev guid and we're allowing duplicates, we don't want
                # to copy the existing url/authz/acl, so clear them out
                if previous_guid_exists and allow_mult_guids_per_hash:
                    record_to_write = copy.deepcopy(record)

                if AUTHZ_STANDARD_KEY not in record_to_write:
                    record_to_write[AUTHZ_STANDARD_KEY] = ""
                if AUTHZ_STANDARD_KEY in record:
                    authz = record[AUTHZ_STANDARD_KEY]
                    record_to_write[AUTHZ_STANDARD_KEY] = " ".join(
                        list(
                            set(
                                record_to_write[AUTHZ_STANDARD_KEY].split(" ")
                                + authz.split(" ")
                            )
                        )
                    ).strip(" ")

                if ACL_STANDARD_KEY not in record_to_write:
                    record_to_write[ACL_STANDARD_KEY] = ""
                if ACL_STANDARD_KEY in record:
                    acl = record[ACL_STANDARD_KEY]
                    record_to_write[ACL_STANDARD_KEY] = " ".join(
                        list(
                            set(
                                record_to_write[ACL_STANDARD_KEY].split(" ")
                                + acl.split(" ")
                            )
                        )
                    ).strip(" ")

                # default value if not available
                if URLS_STANDARD_KEY not in record_to_write:
                    record_to_write[URLS_STANDARD_KEY] = ""
                # if value provided, add it to existing values
                if URLS_STANDARD_KEY in record:
                    urls = record[URLS_STANDARD_KEY]
                    record_to_write[URLS_STANDARD_KEY] = " ".join(
                        list(
                            set(
                                record_to_write[URLS_STANDARD_KEY].split(" ")
                                + urls.split(" ")
                            )
                        )
                    ).strip(" ")

                # for any column not in the standard set, either update the existing
                # record with new data, or initialize field to data provided
                for column_name in [
                    key
                    for key in record.keys()
                    if key
                    not in (
                        GUID_STANDARD_KEY,
                        SIZE_STANDARD_KEY,
                        MD5_STANDARD_KEY,
                        ACL_STANDARD_KEY,
                        URLS_STANDARD_KEY,
                        AUTHZ_STANDARD_KEY,
                    )
                ]:
                    if column_name in record_to_write:
                        record_to_write[column_name] = " ".join(
                            list(
                                set(
                                    record_to_write[column_name].split(" ")
                                    + record[column_name].split(" ")
                                )
                            )
                        ).strip(" ")
                    else:
                        record_to_write[column_name] = record[column_name]

                # if there's NOT a previous guid matching this record and we're NOT allowing
                # duplicates, remove existing record so that we can replace with newly updated one
                if not (previous_guid_exists and allow_mult_guids_per_hash):
                    all_rows[record_to_write[MD5_STANDARD_KEY]] = []

            for key in record_to_write.keys():
                headers.add(key)

            all_rows.setdefault(record_to_write[MD5_STANDARD_KEY], []).append(
                record_to_write
            )

    if output_manifest_file_delimiter is None:
        output_manifest_file_ext = os.path.splitext(output_manifest)
        if output_manifest_file_ext[-1].lower() == ".tsv":
            output_manifest_file_delimiter = "\t"
        else:
            output_manifest_file_delimiter = ","

    # order headers with alphabetical for standard columns, followed by alphabetical for
    # non-standard columns
    stardard_headers = sorted(
        [
            GUID_STANDARD_KEY,
            SIZE_STANDARD_KEY,
            MD5_STANDARD_KEY,
            ACL_STANDARD_KEY,
            URLS_STANDARD_KEY,
            AUTHZ_STANDARD_KEY,
        ]
    )
    non_standard_headers = sorted(
        [header for header in headers if header not in stardard_headers]
    )

    headers = stardard_headers + non_standard_headers

    with open(output_manifest, "w") as outfile:
        logging.info(f"Writing merged manifest to {output_manifest}")
        logging.info(f"Headers {headers}")
        output_writer = csv.DictWriter(
            outfile,
            delimiter=output_manifest_file_delimiter,
            fieldnames=headers,
            extrasaction="ignore",
        )
        output_writer.writeheader()

        for hash_code, records in all_rows.items():
            for record in records:
                output_writer.writerow(record)

        logging.info(f"Finished writing merged manifest to {output_manifest}")
