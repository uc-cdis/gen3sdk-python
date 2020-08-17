import logging
import warnings
import csv

from gen3.tools.indexing.manifest_columns import (
    Columns,
    MD5Validator,
    URLValidator,
    SizeValidator,
    AuthzValidator,
    EmptyWarning,
    MultiValueError,
)


def is_valid_manifest_format(
    manifest_path,
    column_names_to_enums=None,
    allowed_protocols=["s3", "gs"],
    allow_base64_encoded_md5=False,
    error_on_empty_url=False,
    line_limit=None,
):
    logging.info(f'validating "{manifest_path}" manifest')
    warnings.filterwarnings("error")

    enums_to_validators = _init_enums_to_validators(
        allowed_protocols, allow_base64_encoded_md5, error_on_empty_url
    )
    with open(manifest_path) as dsv_file:
        dsv_reader = _get_dsv_reader(dsv_file)
        manifest_column_names = dsv_reader.fieldnames
        manifest_column_names_to_validators = _get_manifest_column_names_to_validators(
            manifest_column_names, enums_to_validators, column_names_to_enums
        )
        _log_manifest_column_names_to_validators(manifest_column_names_to_validators)
        manifest_is_valid = _validate_manifest_column_names(
            manifest_column_names_to_validators, enums_to_validators, error_on_empty_url
        )
        if line_limit is None or line_limit > 1:
            manifest_is_valid = (
                _validate_rows(
                    dsv_reader, manifest_column_names_to_validators, line_limit
                )
                and manifest_is_valid
            )

    _log_summary(manifest_is_valid, manifest_path, line_limit)
    return manifest_is_valid


def _init_enums_to_validators(
    allowed_protocols, allow_base64_encoded_md5, error_on_empty_url
):

    return {
        Columns.MD5: MD5Validator(allow_base64_encoding=allow_base64_encoded_md5),
        Columns.URL: URLValidator(
            allowed_protocols=allowed_protocols, error_on_empty=error_on_empty_url
        ),
        Columns.SIZE: SizeValidator(),
        Columns.AUTHZ: AuthzValidator(),
    }


def _get_dsv_reader(dsv_file):
    dialect = csv.Sniffer().sniff(dsv_file.readline())
    dsv_file.seek(0)
    return csv.DictReader(dsv_file, dialect=dialect, quoting=csv.QUOTE_NONE)


def _get_manifest_column_names_to_validators(
    manifest_column_names, enums_to_validators, column_names_to_enums=None,
):
    manifest_column_names_to_validators = {}
    if column_names_to_enums is None:
        for validator in enums_to_validators.values():
            for allowed_column_name in validator.ALLOWED_COLUMN_NAMES:
                if allowed_column_name in manifest_column_names:
                    manifest_column_names_to_validators[allowed_column_name] = validator
        return manifest_column_names_to_validators

    for column_name in manifest_column_names:
        if column_name in column_names_to_enums:
            manifest_column_names_to_validators[column_name] = enums_to_validators[
                column_names_to_enums[column_name]
            ]

    return manifest_column_names_to_validators


def _validate_manifest_column_names(
    manifest_column_names_to_validators, enums_to_validators, error_on_empty_url
):
    def check_column(column_enum):
        validator = enums_to_validators[column_enum]
        if validator not in manifest_column_names_to_validators.values():
            message = f'line 1, could not find a column name corresponding to required "{column_enum}". Default accepted column names for "{column_enum}" are {validator.ALLOWED_COLUMN_NAMES}'
            if column_enum == Columns.URL and not error_on_empty_url:
                logging.warning(message)
            else:
                logging.error(message)
                return False

        return True

    return all([check_column(c) for c in [Columns.MD5, Columns.SIZE, Columns.URL]])


def _log_manifest_column_names_to_validators(manifest_column_names_to_validators):
    if not manifest_column_names_to_validators:
        logging.error("line 1, no manifest columns were mapped to validators")
    for manifest_column_name, validator in manifest_column_names_to_validators.items():
        logging.info(
            f'mapped manifest column "{manifest_column_name}" to "{validator.__class__.__name__}" class instance'
        )


def _validate_rows(dsv_reader, manifest_column_names_to_validators, line_limit=None):

    rows_are_valid = True
    for line_number, row in enumerate(dsv_reader, 2):
        row_items = row.items()
        if len(row_items) != len(dsv_reader.fieldnames):
            logging.warning(
                f"line {line_number}, number of fields ({len(row_items)}) in row is unequal to number of column names in manifest ({len(dsv_reader.fieldnames)})"
            )

        for column_name, value in row_items:
            if column_name in manifest_column_names_to_validators:
                validator = manifest_column_names_to_validators[column_name]
                try:
                    validator.validate(value)
                except EmptyWarning:
                    logging.warning(
                        f'line {line_number}, "{column_name}" field is empty'
                    )
                except MultiValueError as e:
                    rows_are_valid = False
                    logging.error(f'line {line_number}, "{column_name}" values {e}')
                except ValueError as e:
                    rows_are_valid = False
                    logging.error(f'line {line_number}, "{column_name}" value {e}')

        if line_number == line_limit:
            break

    return rows_are_valid


def _log_summary(manifest_is_valid, manifest_path, lines_validated):
    summary = f'finished validating "{manifest_path}" manifest'
    if type(lines_validated) == int:
        summary = f'finished validating first {lines_validated} lines in "{manifest_path}" manifest'
        if lines_validated == 1:
            summary = f'finished validating first line in "{manifest_path}" manifest'
    if manifest_is_valid:
        summary += ", no errors were found"
    logging.info(summary)
