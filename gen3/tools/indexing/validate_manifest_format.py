"""
Module to implement is_valid_manifest_format
"""
import warnings
import csv

from gen3.tools.utils import (
    Columns,
    MD5Validator,
    URLValidator,
    SizeValidator,
    AuthzValidator,
    EmptyWarning,
    MultiValueError,
)

from cdislogging import get_logger

logging = get_logger("__name__")


def is_valid_manifest_format(
    manifest_path,
    column_names_to_enums=None,
    allowed_protocols=["s3", "gs"],
    allow_base64_encoded_md5=False,
    error_on_empty_url=False,
    line_limit=None,
):
    """
    Validates the contents of a manifest of file objects and logs all errors
    found. Each logged error message includes a description along with the line
    number and column in which the error occurred.

    is_valid_manifest_format can validate md5, size, url and authz values by
    making use of the MD5Validator, SizeValidator, URLValidator, and
    AuthzValidator classes, respectively. See documentation in these Validator
    subclasses for more details on how specific values are validated.

    is_valid_manifest_format attempts to automatically map manifest column
    names to Validator subclasses based on the ALLOWED_COLUMN_NAMES tuple
    attribute defined in each Validator subclass. column_names_to_enums
    argument should alternatively be provided when the manifest contains custom
    column names.

    The input manifest may contain extra columns that are not intended to be
    validated, and columns can appear in any order.

    Args:
        manifest_path(str):
            path to the manifest to be validated. the manifest is assumed to be
            in a dsv (delimiter-separated values) format (e.g. csv, tsv, etc.).
            the delimiter used is automatically detected and used to parse the
            manifest
        column_names_to_enums(dict, optional):
            maps custom manifest column names to Columns enums, which
            determines which Validator subclass will be used for which manifest
            column. note that when column_names_to_enums is provided by caller,
            this means that no automatic column name matching occurs. for this
            reason, in the example below, even though it is in SizeValidator's
            ALLOWED_COLUMN_NAMES, "file_size" must still be explicitly mapped
            to Columns.SIZE. this behavior is to account for the case in which
            a user does not want validation performed on a column whose name
            would otherwise automatically be mapped to a Validator subclass
            (e.g. maybe validation on a column with the name "size" is not
            desired)

            example:
            {
                "custom md5 name": Columns.MD5,
                "file_size": Columns.SIZE,
                "custom url name": Columns.URL,
                "custom authz name": Columns.AUTHZ
            }
        allowed_protocols(list(str), optional):
            allowed protocols for url validation. note that if
            allowed_protocols is provided by caller, url values will only be
            validated using the provided protocols (e.g. if
            allowed_protocols=["http", "https"], an error would be raised when
            validating "s3://valid_bucket/valid_key" url)
        allow_base64_encoded_md5(bool, optional):
            whether or not Base64 encoded md5 values are allowed. if False,
            only hexadecimal encoded 128-bit md5 values are considered valid,
            and Base64 encoded values will be logged as errors. if True, both
            hexadecimal and Base64 encoded 128-bit md5 values are considered
            valid
        error_on_empty_url(bool, optional):
            whether to treat completely empty url values as errors

            for the following example manifest, if error_on_empty_url is False,
            a warning would be logged for the completely empty url value on
            line 2. if error_on_empty_url is True, an error would be generated
            instead
            ```
            md5,url,size
            1596f493ba9ec53023fca640fb69bd3b,,42
            ```

            note that regardless of error_on_empty_url, errors will be
            generated no matter what for arrays or quotes from which urls could
            not be extracted. for example, for the following manifest, errors
            would be generated for the url value on lines 2, 3, 4, and 5
            ```
            md5,url,size
            1596f493ba9ec53023fca640fb69bd30,"",42
            1596f493ba9ec53023fca640fb69bd31,'',43
            1596f493ba9ec53023fca640fb69bd32,[],44
            1596f493ba9ec53023fca640fb69bd33,["", ""],45
            ```
        line_limit(int, optional):
            number of lines in manifest to validate including the header. if
            not provided, every line is validated

    Returns:
        bool: True if no errors were found in manifest. False otherwise
    """
    logging.info(f'validating "{manifest_path}" manifest')
    warnings.filterwarnings("error")

    enums_to_validators = _init_enums_to_validators(
        allowed_protocols, allow_base64_encoded_md5, error_on_empty_url
    )
    with open(manifest_path, "r", encoding="utf-8-sig") as dsv_file:
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
    """
    Initialize Validator subclass instances (i.e. validators) with provided
    arguments and map Columns enums to instantiated validators

    Args:
        allowed_protocols(list(str)): allowed protocols for URLValidator
        allow_base64_encoding(bool): whether to allow base64 encoding for
            MD5Validator
        error_on_empty_url(bool): whether to error on an empty url for
            URLValidator

    Returns:
        dict: maps Columns enums to corresponding instantiated validators
    """
    return {
        Columns.MD5: MD5Validator(allow_base64_encoding=allow_base64_encoded_md5),
        Columns.URL: URLValidator(
            allowed_protocols=allowed_protocols, error_on_empty=error_on_empty_url
        ),
        Columns.SIZE: SizeValidator(),
        Columns.AUTHZ: AuthzValidator(),
    }


def _get_dsv_reader(dsv_file):
    """
    Detect the delimiter used in opened dsv (delimiter-separated values) file
    and return csv.DictReader object initialized to iterate over file contents

    Args:
        dsv_file(file object): opened dsv file object

    Returns:
        csv.DictReader: reader object initialized to iterate over file contents
    """
    dialect = csv.Sniffer().sniff(dsv_file.readline())
    dsv_file.seek(0)
    return csv.DictReader(dsv_file, dialect=dialect, quoting=csv.QUOTE_NONE)


def _get_manifest_column_names_to_validators(
    manifest_column_names,
    enums_to_validators,
    column_names_to_enums=None,
):
    """
    Maps manifest column names to Validator subclass instances. If
    column_names_to_enums is None, a given manifest column name is mapped to a
    validator if the manifest column name exactly matches one of the
    validator's ALLOWED_COLUMN_NAMES

    Args:
        manifest_column_names(list(str)): list of the manifest's column names
        enums_to_validators(dict): maps Columns enums to Validator subclass
            instances. example:
            {
                Columns.MD5: <MD5Validator object>,
                Columns.SIZE: <SizeValidator object>,
                Columns.URL: <URLValidator object>,
                Columns.AUTHZ: <AuthzValidator object>
            }
        column_names_to_enums(dict): custom mapping between manifest column names
            and Columns enums that will be used to determine corresponding
            validators. note that no automatic matching occurs between manifest
            column names and the validators' ALLOWED_COLUMN_NAMES when
            column_names_to_enums is supplied. for this reason, in the example
            below, even though it is in SizeValidator's ALLOWED_COLUMN_NAMES,
            "size" still must be added as a key
            example:
            {
                "custom md5 name": Columns.MD5,
                "size": Columns.SIZE,
                "custom urls name": Columns.URL,
                "custom authz name": Columns.AUTHZ
            }

    Returns:
        dict: manifest column names mapped to Validator subclass instances.
            example:
            {
                "custom md5 name": <MD5Validator object>,
                "size": <SizeValidator object>,
                "custom urls name": <URLValidator object>,
                "custom authz name": <AuthzValidator object>
            }
    """
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
    """
    Validates that manifest has all required columns. Column names
    corresponding to Columns.MD5 and Columns.SIZE enums are required, and
    error_on_empty_url determines if Columns.URL is required

    Args:
        manifest_column_names_to_validators(dict): maps manifest column names
            to Validator subclass instances. example:
            {
                "md5": <MD5Validator object>,
                "size": <SizeValidator object>,
                "urls": <URLValidator object>,
                "authz": <AuthzValidator object>
            }
        enums_to_validators(dict): maps Columns enums to Validator subclass
            instances. example:
            {
                Columns.MD5: <MD5Validator object>,
                Columns.SIZE: <SizeValidator object>,
                Columns.URL: <URLValidator object>,
                Columns.AUTHZ: <AuthzValidator object>
            }
        error_on_empty_url(bool): if True, indicates that url column name is
            required in manifest

    Returns:
        bool: True if manifest has all required columns, False otherwise
    """

    def check_column(column_enum):
        """
        Helper method to determine if a manifest column name was mapped to
        column_enum

        Args:
            column_enum(Columns enum): identifies column (e.g. Columns.MD5)

        Returns:
            bool: False if a manifest column name was not mapped to
                column_enum and column_enum is required. Otherwise, True
        """
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
    """
    Logs which manifest column names were mapped to which validators. Logs an
    error if no manifest column names were mapped.

    Args:
        manifest_column_names_to_validators(dict): maps manifest column names
            to instances of subclasses of Validator. example:
            {
                "md5": <MD5Validator object>,
                "size": <SizeValidator object>,
                "urls": <URLValidator object>,
                "authz": <AuthzValidator object>
            }

    Returns:
        None
    """
    if not manifest_column_names_to_validators:
        logging.error("line 1, no manifest columns were mapped to validators")
    for manifest_column_name, validator in manifest_column_names_to_validators.items():
        logging.info(
            f'mapped manifest column "{manifest_column_name}" to "{validator.__class__.__name__}" class instance'
        )


def _validate_rows(dsv_reader, manifest_column_names_to_validators, line_limit=None):
    """
    Loops over manifest rows starting from line 2, validating each row's values
    by calling validate method on the corresponding Validator subclass
    instance. Errors raised by the validate method are logged with the line
    number and column in which the error occurred.

    Args:
        dsv_reader(csv.DictReader): reader object in a state such that calling
            next on it will return the second row in the manifest
        manifest_column_names_to_validators(dict): maps manifest column names
            to instances of subclasses of Validator. no validation is performed
            for columns that do not appear as keys in
            manifest_column_names_to_validators. example:
            {
                "md5": <MD5Validator object>,
                "size": <SizeValidator object>,
                "urls": <URLValidator object>,
                "authz": <AuthzValidator object>
            }
        line_limit(int): the line number in the manifest to validate
            up to (e.g. if line_number is 4, _validate_rows validates lines
            2 through 4 and stops). if line_number is None, then _validate_rows
            validates up to the last line in the manifest

    Returns:
        bool: true if no errors were found, false otherwise
    """
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
    """
    Logs a short summary of validation that potentially includes number of
    lines validated and whether errors were found.

    Args:
        manifest_is_valid(bool): whether manifest is valid
        manifest_path(str): the path to the manifest that was validated
        lines_validated(int or NoneType): if an int, the number of lines in
            manifest that were validated. if is None, indicates that every
            line was validated

    Returns:
        None
    """
    summary = f'finished validating "{manifest_path}" manifest'
    if type(lines_validated) == int:
        summary = f'finished validating first {lines_validated} lines in "{manifest_path}" manifest'
        if lines_validated == 1:
            summary = f'finished validating first line in "{manifest_path}" manifest'
    if manifest_is_valid:
        summary += ", no errors were found"
    logging.info(summary)
