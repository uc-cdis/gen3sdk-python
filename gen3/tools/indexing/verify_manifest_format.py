#  XXX reorder
import csv

import logging
from collections import OrderedDict, defaultdict

from gen3.tools.indexing.manifest_columns import (
    Columns,
    MD5Validator,
    URLValidator,
    SizeValidator,
    AuthzValidator,
)


#  XXX handle incorrect number of values in rows
#  XXX log warning for what columns will not be validated
#  XXX log missing validators
#  XXX run through first 100 lines of manifest
def is_valid_manifest_format(
    manifest_path,
    column_names_to_enums=None,
    allowed_protocols=["s3", "gs"],
    require_non_base64_md5=True,
    #  XXX implement
    error_on_empty_url=False
    #  required_columns=[Columns.MD5, Columns.SIZE],
):

    manifest_is_valid = True
    enums_to_validators = _init_enums_to_validators(
        allowed_protocols, require_non_base64_md5
    )
    with open(manifest_path) as dsv_file:
        dsv_reader = _get_dsv_reader(dsv_file)
        manifest_column_names = dsv_reader.fieldnames
        manifest_column_names_to_validators = _get_manifest_column_names_to_validators(
            manifest_column_names, enums_to_validators, column_names_to_enums
        )
        manifest_is_valid = (
            _validate_rows(dsv_reader, manifest_column_names_to_validators)
            and manifest_is_valid
        )

    return manifest_is_valid


def _init_enums_to_validators(allowed_protocols, require_non_base64_md5):

    return {
        Columns.MD5: MD5Validator(require_non_base64_md5=require_non_base64_md5),
        Columns.URL: URLValidator(allowed_protocols=allowed_protocols),
        Columns.SIZE: SizeValidator(),
        Columns.AUTHZ: AuthzValidator(),
    }


def _get_dsv_reader(dsv_file):
    dialect = csv.Sniffer().sniff(dsv_file.readline())
    dsv_file.seek(0)
    return csv.DictReader(dsv_file, dialect=dialect)


def _get_manifest_column_names_to_validators(
    manifest_column_names, enums_to_validators, column_names_to_enums=None,
):
    column_names_to_validators = {}
    if column_names_to_enums is None:
        for validator in enums_to_validators.values():
            for allowed_column_name in validator.allowed_column_names():
                column_names_to_validators[allowed_column_name] = validator
        return column_names_to_validators

    for column_name in manifest_column_names:
        if column_name in column_names_to_enums:
            column_names_to_validators[column_name] = enums_to_validators[
                column_names_to_enums[column_name]
            ]

    return column_names_to_validators


def _validate_rows(dsv_reader, manifest_column_names_to_validators):

    rows_are_valid = True
    #  XXX rename row
    for line_number, row in enumerate(dsv_reader, 2):
        for column_name, value in row.items():
            if column_name in manifest_column_names_to_validators:
                validator = manifest_column_names_to_validators[column_name]
                try:
                    validator.validate(value)
                except ValueError as e:
                    rows_are_valid = False
                    logging.error(
                        f"line {line_number}, validation failed for {column_name} column: {e}"
                    )

    return rows_are_valid
