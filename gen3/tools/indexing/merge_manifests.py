"""
Merging indexing manifests with arbitrary columns

Example:

    guid    md5    size    urls    authz    more_data
    dg/123    f7cb...    42    http://cats.com    /foo    moredata
+
    guid    md5    size    urls    authz    extra_data
    dg/123    f7cb...    42    s3://bucket/cats    /baz    stuff
=
    acl    authz    guid    md5   size    urls    extra_data    more_data
    /baz /foo    dg/123    f7cb...    42    http://cats.com s3://bucket/cats    stuff    moredata

Is able to handle situations where multiple different guids for the same hash is
allowed. For example, if the following is valid:

guid    md5    size
dg/124  f7cbeb4f7fcc139d95cb9cc1cf0696ec    42
dg/123  f7cbeb4f7fcc139d95cb9cc1cf0696ec    42

By default, this will NOT allow multiple GUIDs per hash and will try to merge all
into one.

"""
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
    columns_with_arrays=None,
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
        columns_with_arrays(list[str]): list of column names where their values should
            be treated like arrays (so that when merging we know to combine)

    Returns:
        None
    """
    columns_with_arrays = columns_with_arrays or []
    columns_with_arrays.extend(
        [URLS_STANDARD_KEY, ACL_STANDARD_KEY, AUTHZ_STANDARD_KEY]
    )

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
            # simple case where this is the first time we've seen this hash
            if record[MD5_STANDARD_KEY] not in all_rows:
                for key in record_to_write.keys():
                    headers.add(key)
                all_rows[record_to_write[MD5_STANDARD_KEY]] = [record_to_write]
            else:
                # import pdb; pdb.set_trace()
                # if the hash already exists, we need to try and update existing
                # entries with any new data (and ensure we don't add duplicates)
                updated_records = {}
                for existing_record in copy.deepcopy(
                    all_rows[record[MD5_STANDARD_KEY]]
                ):
                    if SIZE_STANDARD_KEY in existing_record:
                        size = existing_record[SIZE_STANDARD_KEY]

                        if size != record[SIZE_STANDARD_KEY]:
                            error_msg = (
                                "Found two objects with the same hash but different sizes,"
                                f" could not merge. Details: object {existing_record} could not be"
                                f" merged with object {record} because {size} !="
                                f" {record[SIZE_STANDARD_KEY]}."
                            )
                            logging.error(error_msg)

                            if not continue_after_error:
                                raise csv.Error(error_msg)

                            # in the case we don't raise an error above, we need to continue
                            continue

                    # at this point, the record has the same hash and size as a previous guid
                    # so either we're allowing an entry like that, or not
                    guid = existing_record.get(GUID_STANDARD_KEY)
                    new_guid = record.get(GUID_STANDARD_KEY)

                    if GUID_STANDARD_KEY in existing_record:
                        if guid and new_guid and guid != new_guid:
                            warning_msg = (
                                "Found two objects with the same hash but different guids,"
                                f" could not merge. Details: object {existing_record} could not be"
                                f" merged with object {record} because {guid} !="
                                f" {new_guid}."
                            )

                            if not allow_mult_guids_per_hash:
                                logging.error(warning_msg)
                                raise csv.Error(error_msg)

                            info_msg = (
                                f"Allowing multiple GUIDs per hash. {new_guid} has same "
                                f"hash as {guid}.\n    Details: {record} is a different "
                                f"record with same hash as existing guid: {guid}."
                            )
                            logging.info(info_msg)

                    if guid == new_guid:
                        logging.debug(
                            f"merging any new data from {record} with existing record: {existing_record}"
                        )

                        record_to_write = _get_updated_record(
                            record,
                            existing_record,
                            continue_after_error=continue_after_error,
                            columns_with_arrays=columns_with_arrays,
                        )

                        updated_records.setdefault(
                            record_to_write.get(GUID_STANDARD_KEY), {}
                        ).update(record_to_write)
                    else:
                        updated_records.setdefault(
                            record_to_write.get(GUID_STANDARD_KEY), {}
                        ).update(record_to_write)
                        updated_records.setdefault(
                            existing_record.get(GUID_STANDARD_KEY), {}
                        ).update(existing_record)

                    for key in record_to_write.keys():
                        headers.add(key)

                all_rows[record[MD5_STANDARD_KEY]] = [
                    record for _, record in updated_records.items()
                ]

    if output_manifest_file_delimiter is None:
        output_manifest_file_ext = os.path.splitext(output_manifest)
        if output_manifest_file_ext[-1].lower() == ".tsv":
            output_manifest_file_delimiter = "\t"
        else:
            output_manifest_file_delimiter = ","

    # order headers with alphabetical for standard columns, followed by alphabetical for
    # non-standard columns
    stardard_headers = [
        GUID_STANDARD_KEY,
        SIZE_STANDARD_KEY,
        MD5_STANDARD_KEY,
        ACL_STANDARD_KEY,
        AUTHZ_STANDARD_KEY,
        URLS_STANDARD_KEY,
    ]
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


def _get_updated_record(
    new_record,
    existing_record,
    continue_after_error,
    columns_with_arrays,
):
    record_to_write = copy.deepcopy(existing_record)

    # for any column not in the standard set, either update the existing
    # record with new data, or leave column as data provided
    for column_name in [
        key
        for key in new_record.keys()
        if key
        not in (
            GUID_STANDARD_KEY,
            SIZE_STANDARD_KEY,
            MD5_STANDARD_KEY,
        )
    ]:
        # first handle space-delimited columns
        if column_name in columns_with_arrays:
            if column_name in existing_record:
                # column that has a space-delimited array of values
                record_to_write[column_name] = " ".join(
                    sorted(
                        list(
                            set(
                                new_record[column_name].split(" ")
                                + existing_record[column_name].split(" ")
                            )
                        )
                    )
                ).strip(" ")
            else:
                record_to_write[column_name] = " ".join(
                    sorted(list(set(new_record[column_name].split(" "))))
                ).strip(" ")
        # handle non-space-delimited columns
        else:
            if not existing_record.get(column_name) or (
                existing_record.get(column_name) == new_record[column_name]
            ):
                record_to_write[column_name] = new_record[column_name]
            elif not new_record[column_name]:
                record_to_write[column_name] = existing_record.get(column_name, "")
            else:
                # old and new have value, unsure how to merge
                error_msg = (
                    f"NOT merging column {column_name} for "
                    f"existing {existing_record} and new "
                    f"{new_record} because unsure how to merge the values.\nERROR: IGNORING NEW VALUE if "
                    f"forced to continue without error."
                )
                logging.error(error_msg)

                if not continue_after_error:
                    raise csv.Error(error_msg)

                # if we're here, that means we are just going to ignore new data
                # and add a row with the existing data

    return record_to_write
