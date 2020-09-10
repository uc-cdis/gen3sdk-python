import os
import logging
import csv
import copy

from collections import OrderedDict
from gen3.tools.indexing.index_manifest import (
    get_and_verify_fileinfos_from_tsv_manifest,
    GUID_STANDARD_KEY,
    FILENAME_STANDARD_KEY,
    SIZE_STANDARD_KEY,
    MD5_STANDARD_KEY,
    ACL_STANDARD_KEY,
    URLS_STANDARD_KEY,
    AUTHZ_STANDARD_KEY,
)


def merge_bucket_manifests(
    directory=".",
    files=None,
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
    files = files or []
    if not files:
        logging.info(f"Iterating over manifests in {directory} directory")
        for file in sorted(os.listdir(directory)):
            files.append(os.path.join(directory, file))

    logging.info(f"Merging files: {files}")

    headers = set()
    all_rows = {}
    for manifest in files:
        records_from_file, _ = get_and_verify_fileinfos_from_tsv_manifest(manifest)
        for record in records_from_file:
            record_to_write = copy.deepcopy(record)
            if record[MD5_STANDARD_KEY] in all_rows:
                record_to_write = copy.deepcopy(all_rows[record[MD5_STANDARD_KEY]])

                if SIZE_STANDARD_KEY in record:
                    size = record[SIZE_STANDARD_KEY]

                    if size != record_to_write[SIZE_STANDARD_KEY]:
                        raise csv.Error(
                            "Found two objects with the same hash but different sizes,"
                            f" could not merge. Details: object {record} could not be"
                            f" merged with object {record_to_write} because {size} !="
                            f" {record_to_write[SIZE_STANDARD_KEY]}."
                        )

                # default value if not available
                if URLS_STANDARD_KEY not in record_to_write:
                    record_to_write[URLS_STANDARD_KEY] = ""
                # if value provided, add it to existing values
                if URLS_STANDARD_KEY in record:
                    url = record[URLS_STANDARD_KEY]
                    if url not in record_to_write[URLS_STANDARD_KEY]:
                        record_to_write[URLS_STANDARD_KEY] += f" {url}"
                        # if this is the first one, strip off the space
                        record_to_write[URLS_STANDARD_KEY] = record_to_write[
                            URLS_STANDARD_KEY
                        ].strip()

                if AUTHZ_STANDARD_KEY not in record_to_write:
                    record_to_write[AUTHZ_STANDARD_KEY] = ""
                if AUTHZ_STANDARD_KEY in record:
                    authz = record[AUTHZ_STANDARD_KEY]
                    if authz not in record_to_write[AUTHZ_STANDARD_KEY]:
                        record_to_write[AUTHZ_STANDARD_KEY] += f" {authz}"
                        record_to_write[AUTHZ_STANDARD_KEY] = record_to_write[
                            AUTHZ_STANDARD_KEY
                        ].strip()

                if ACL_STANDARD_KEY not in record_to_write:
                    record_to_write[ACL_STANDARD_KEY] = ""
                if ACL_STANDARD_KEY in record:
                    acl = record[ACL_STANDARD_KEY]
                    if acl not in record_to_write[ACL_STANDARD_KEY]:
                        record_to_write[ACL_STANDARD_KEY] += f" {acl}"
                        record_to_write[ACL_STANDARD_KEY] = record_to_write[
                            ACL_STANDARD_KEY
                        ].strip()

                if GUID_STANDARD_KEY in record:
                    guid = record[GUID_STANDARD_KEY]
                    if (
                        guid
                        and record_to_write.get(GUID_STANDARD_KEY)
                        and guid != record_to_write.get(GUID_STANDARD_KEY)
                    ):
                        raise csv.Error(
                            "Found two objects with the same hash but different guids,"
                            f" could not merge. Details: object {record} could not be"
                            f" merged with object {record_to_write} because {guid} !="
                            f" {record_to_write.get(GUID_STANDARD_KEY)}."
                        )

                    if guid:
                        record_to_write[GUID_STANDARD_KEY] = guid

            for key in record_to_write.keys():
                headers.add(key)

            all_rows.update({record_to_write[MD5_STANDARD_KEY]: record_to_write})

    if output_manifest_file_delimiter is None:
        output_manifest_file_ext = os.path.splitext(output_manifest)
        if output_manifest_file_ext[-1].lower() == ".tsv":
            output_manifest_file_delimiter = "\t"
        else:
            output_manifest_file_delimiter = ","

    with open(output_manifest, "w") as outfile:
        logging.info(f"Writing merged manifest to {output_manifest}")
        logging.info(f"Headers {list(headers)}")
        output_writer = csv.DictWriter(
            outfile, delimiter="\t", fieldnames=list(headers), extrasaction="ignore"
        )
        output_writer.writeheader()

        for hash_code, record in all_rows.items():
            output_writer.writerow(record)

        logging.info(f"Finished writing merged manifest to {output_manifest}")
