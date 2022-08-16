from abc import ABC
from base64 import b64encode, b64decode

import csv
from enum import Enum, unique
import os
import string
import sys
from urllib.parse import urlparse
import warnings

from gen3.utils import (
    UUID_FORMAT,
    MD5_FORMAT,
    ACL_FORMAT,
    URL_FORMAT,
    AUTHZ_FORMAT,
    SIZE_FORMAT,
    _verify_format,
)
from cdislogging import get_logger

logging = get_logger("__name__")

# Pre-defined supported column names
RECORD_TYPE_STANDARD_KEY = "record_type"
RECORD_TYPE_ALLOWED_VALUES = ["object", "package"]

GUID_COLUMN_NAMES = ["guid", "GUID"]
GUID_STANDARD_KEY = "guid"

FILENAME_COLUMN_NAMES = ["filename", "file_name"]
FILENAME_STANDARD_KEY = "file_name"

SIZE_COLUMN_NAMES = ["size", "filesize", "file_size", "s3_file_size", "gs_file_size"]
SIZE_STANDARD_KEY = "size"

MD5_COLUMN_NAMES = ["md5", "md5_hash", "md5hash", "hash", "md5sum", "file_md5sum"]
MD5_STANDARD_KEY = "md5"

ACLS_COLUMN_NAMES = ["acl", "acls"]
ACL_STANDARD_KEY = "acl"

URLS_COLUMN_NAMES = ["url", "urls", "s3_path", "gs_path", "aws_uri", "gcp_uri"]
URLS_STANDARD_KEY = "urls"

AUTHZ_COLUMN_NAMES = ["authz"]
AUTHZ_STANDARD_KEY = "authz"

PREV_GUID_COLUMN_NAMES = ["previous_guid", "prev_guid", "prev_id"]
PREV_GUID_STANDARD_KEY = "prev_guid"

BUNDLENAME_COLUMN_NAME = ["bundle_name", "name"]

IDS_COLUMN_NAME = ["ids", "bundle_ids"]

DESCRIPTION_COLUMN_NAME = ["description"]

CHECKSUMS_COLUMN_NAME = ["checksums", "checksum"]

TYPE_COLUMN_NAME = ["type", "types"]

ALIASES_COLUMN_NAME = ["alias", "aliases"]


def get_and_verify_fileinfos_from_manifest(
    manifest_file, manifest_file_delimiter=None, include_additional_columns=False
):
    """
    Wrapper for above function to determine the delimiter based on file extention
    """
    # if delimiter not specified, try to get based on file ext
    if not manifest_file_delimiter:
        manifest_file_ext = os.path.splitext(manifest_file)
        if manifest_file_ext[-1].lower() == ".tsv":
            manifest_file_delimiter = "\t"
        else:
            # default, assume CSV
            manifest_file_delimiter = ","

    return get_and_verify_fileinfos_from_tsv_manifest(
        manifest_file=manifest_file,
        manifest_file_delimiter=manifest_file_delimiter,
        include_additional_columns=include_additional_columns,
    )


def get_and_verify_fileinfos_from_tsv_manifest(
    manifest_file, manifest_file_delimiter="\t", include_additional_columns=False
):
    """
    get and verify file infos from tsv manifest

    Args:
        manifest_file(str): the path to the input manifest
        manifest_file_delimiter(str): delimiter

    Returns:
        list(dict): list of file info
        [
            {
                "guid": "guid_example",
                "filename": "example",
                "size": 100,
                "acl": "['open']",
                "md5": "md5_hash",
            },
        ]
        headers(list(str)): field names

    """
    csv.field_size_limit(sys.maxsize)  # handle large values such as "package_contents"
    files = []
    with open(manifest_file, "r", encoding="utf-8-sig") as csvfile:
        csvReader = csv.DictReader(
            csvfile, delimiter=manifest_file_delimiter, restval=""
        )
        fieldnames = csvReader.fieldnames
        if len(fieldnames) < 2:
            logging.warning(
                f"The manifest delimiter ({manifest_file_delimiter}) does not seem to match the provided file"
            )

        logging.debug(f"got fieldnames from {manifest_file}: {fieldnames}")
        pass_verification = True
        is_row_valid = True
        for row_number, row in enumerate(csvReader, 1):
            output_row = {}
            for current_column_name in row.keys():
                output_column_name = None
                if (
                    current_column_name
                    and current_column_name.lower() in GUID_COLUMN_NAMES
                ):
                    fieldnames[
                        fieldnames.index(current_column_name)
                    ] = GUID_STANDARD_KEY
                    output_column_name = GUID_STANDARD_KEY
                elif (
                    current_column_name
                    and current_column_name.lower() in FILENAME_COLUMN_NAMES
                ):
                    fieldnames[
                        fieldnames.index(current_column_name)
                    ] = FILENAME_STANDARD_KEY
                    output_column_name = FILENAME_STANDARD_KEY
                elif (
                    current_column_name
                    and current_column_name.lower() in MD5_COLUMN_NAMES
                ):
                    fieldnames[fieldnames.index(current_column_name)] = MD5_STANDARD_KEY
                    output_column_name = MD5_STANDARD_KEY
                    if not _verify_format(row[current_column_name], MD5_FORMAT):
                        logging.error(
                            f"ERROR: {row[current_column_name]} is not in md5 format"
                        )
                        is_row_valid = False
                elif (
                    current_column_name
                    and current_column_name.lower() in ACLS_COLUMN_NAMES
                ):
                    fieldnames[fieldnames.index(current_column_name)] = ACL_STANDARD_KEY
                    output_column_name = ACL_STANDARD_KEY
                    if not _verify_format(row[current_column_name], ACL_FORMAT):
                        logging.error(
                            f"ERROR: {row[current_column_name]} is not in acl format"
                        )
                        is_row_valid = False
                elif (
                    current_column_name
                    and current_column_name.lower() in URLS_COLUMN_NAMES
                ):
                    fieldnames[
                        fieldnames.index(current_column_name)
                    ] = URLS_STANDARD_KEY
                    output_column_name = URLS_STANDARD_KEY
                    if not _verify_format(row[current_column_name], URL_FORMAT):
                        logging.error(
                            f"ERROR: {row[current_column_name]} is not in urls format"
                        )
                        is_row_valid = False
                elif (
                    current_column_name
                    and current_column_name.lower() in AUTHZ_COLUMN_NAMES
                ):
                    fieldnames[
                        fieldnames.index(current_column_name)
                    ] = AUTHZ_STANDARD_KEY
                    output_column_name = AUTHZ_STANDARD_KEY
                    if not _verify_format(row[current_column_name], AUTHZ_FORMAT):
                        logging.error(
                            f"ERROR: {row[current_column_name]} is not in authz format"
                        )
                        is_row_valid = False
                elif (
                    current_column_name
                    and current_column_name.lower() in SIZE_COLUMN_NAMES
                ):
                    fieldnames[
                        fieldnames.index(current_column_name)
                    ] = SIZE_STANDARD_KEY
                    output_column_name = SIZE_STANDARD_KEY
                    if not _verify_format(row[current_column_name], SIZE_FORMAT):
                        logging.error(
                            f"ERROR: {row[current_column_name]} is not in int format"
                        )
                        is_row_valid = False
                elif (
                    current_column_name
                    and current_column_name.lower() in PREV_GUID_COLUMN_NAMES
                ):
                    fieldnames[
                        fieldnames.index(current_column_name)
                    ] = PREV_GUID_STANDARD_KEY
                    output_column_name = PREV_GUID_STANDARD_KEY
                    # only validate format if value is provided (since this is optional)
                    if row[current_column_name] and not _verify_format(
                        row[current_column_name], UUID_FORMAT
                    ):
                        logging.error(
                            f"ERROR: {row[current_column_name]} is not in UUID_FORMAT format"
                        )
                        is_row_valid = False
                elif (
                    current_column_name
                    and current_column_name.lower() == RECORD_TYPE_STANDARD_KEY
                ):
                    output_column_name = RECORD_TYPE_STANDARD_KEY
                    if row[current_column_name] not in RECORD_TYPE_ALLOWED_VALUES:
                        logging.error(
                            f"ERROR: '{row[current_column_name]}' is not one of the valid record types: {RECORD_TYPE_ALLOWED_VALUES}"
                        )
                        is_row_valid = False
                elif include_additional_columns:
                    output_column_name = current_column_name

                if output_column_name:
                    try:
                        output_row[output_column_name] = (
                            int(row[current_column_name])
                            if output_column_name == SIZE_STANDARD_KEY
                            else row[current_column_name].strip()
                            if type(row[current_column_name]) == str
                            else row[current_column_name]
                        )
                    except ValueError:
                        # don't break
                        pass

            if not {URLS_STANDARD_KEY, MD5_STANDARD_KEY, SIZE_STANDARD_KEY}.issubset(
                set(output_row.keys())
            ):
                logging.error(
                    f"ERROR: '{row[current_column_name]}' (columns names: "
                    f"{set(output_row.keys())}) does not have some required rows: "
                    f"{URLS_STANDARD_KEY}, {MD5_STANDARD_KEY}, {SIZE_STANDARD_KEY}"
                )
                is_row_valid = False

            if not is_row_valid:
                logging.error(
                    f"row {row_number} with values {row} does not pass the validation"
                )

                # overall verification fails, but reset row validity
                pass_verification = False
                is_row_valid = True

            files.append(output_row)

    if not pass_verification:
        logging.error("The manifest is not in the correct format!!!")
        return [], []

    return files, fieldnames


"""
Classes to be used in the identification and validation of manifest columns
"""


@unique
class Columns(Enum):
    """
    Identifies manifest columns
    """

    MD5 = 1
    URL = 2
    SIZE = 3
    AUTHZ = 4


class EmptyWarning(Warning):
    """
    Warns of an empty value
    """

    pass


class MultiValueError(ValueError):
    """
    Indicates that an error occurred involving multiple values
    """

    pass


class Validator(ABC):
    """
    Should be derived to implement validation for a specific manifest column.
    Provides methods to handle value formatting such as double quotes and
    arrays
    """

    def validate(self, value):
        """
        Wraps _validate_single_value method which should be implemented by
        child classes. Essentially the purpose of validate even being
        implemented in the abstract class is to handle formatting in which a
        single value is enclosed by a pair of double quotes.

        Args:
            value(str): single value to be validated

        Returns:
            None
        """
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        self._validate_single_value(value)

    def _validate_mulitple_values(self, values):
        """
        Validates all individual values that can be extracted from values
        argument. In the case that only one individual value is found to be in
        error, a ValueError is raised. In the case that two or more individual
        values are found to be in error, a MultiValueError is raised.

        Args:
            values(str): values to be validated. can be formatted to include
                one or more individual values

                examples:
                "/a"
                "'/b'"
                "[/a, /b]"
                "['/a', '/b']"
                "/a /b"

                if values is at least one character long and considered to be
                empty, a ValueError is raised. examples of this:

                "''"
                '""'
                "[]"
                "[, ,]"
                "     "

                if values is completely empty (i.e. is ""), this is a special
                case in which _error_on_empty boolean attribute determines
                whether EmptyWarning or ValueError is raised

        Returns:
            None
        """
        if not values and not self._error_on_empty:
            warnings.warn(EmptyWarning())
            return

        parsed_values = self._parse_multiple_values(values)
        if not parsed_values:
            raise ValueError(f'"{values}" is empty, {self._expectation_message}')

        invalid_values = [
            v for v in parsed_values if not self._is_single_value_valid(v)
        ]

        if len(invalid_values) == 1:
            raise ValueError(
                f'"{invalid_values[0]}" is invalid, {self._expectation_message}'
            )
        elif len(invalid_values) > 1:
            formatted_invalid_values = ", ".join(f'"{s}"' for s in invalid_values)
            raise MultiValueError(
                f"{formatted_invalid_values} are invalid, {self._expectation_message}"
            )

    @staticmethod
    def _parse_multiple_values(values):
        """
        Extracts individual values out of string that is possibly formatted to
        contain multiple values

        Args:
            values(str): string that is possibly formatted to contain multiple
                values
                examples:
                "/a"
                "'/b'"
                "[/a, /b]"
                "['/a', '/b']"
                "/a /b"

        Returns:
            list(str): extracted, stripped values
                examples corresponding to example inputs:
                ['/a']
                ['/b']
                ['/a', '/b']
                ['/a', '/b']
                ['/a', '/b']
        """
        values = values.translate(values.maketrans("[]\"'", "    "))
        return values.split()


class MD5Validator(Validator):
    """
    Implements validation for the md5 manifest column. Validation is performed
    using the validate instance method, which raises a ValueError if the md5
    value is invalid. Note below that a valid md5 value can be enclosed with a
    single pair of double quotes and no error will be raised

    Examples:
    ```
    md5_validator = MD5Validator()
    md5_validator.validate("1596f493ba9ec53023fca640fb69bd3b")     # does not raise any errors # pragma: allowlist secret
    md5_validator.validate('"1596f493ba9ec53023fca640fb69bd3b"')   # does not raise any errors # pragma: allowlist secret
    md5_validator.validate("[1596f493ba9ec53023fca640fb69bd3b]")   # raises ValueError         # pragma: allowlist secret
    md5_validator.validate("42")                                   # raises ValueError
    ```
    """

    ALLOWED_COLUMN_NAMES = MD5_COLUMN_NAMES

    def __init__(self, allow_base64_encoding=False):
        """
        Performs basic initialization.

        Args:
            allow_base64_encoding(bool): if False, only allow hexadecimal
            encoding. if True, allow both hexadecimal encoding and Base64
            encoding
        """
        self._allow_base64_encoding = allow_base64_encoding

    def _validate_single_value(self, md5_hash):
        """
        Validates an md5 hash by raising a ValueError if an error was found.
        Requires _allow_base64_encoding attribute to be initialized on
        instance.

        Args:
            md5_hash(str): md5 hash that gets validated

        Returns:
            None
        """
        if all(c in string.hexdigits for c in md5_hash) and len(md5_hash) == 32:
            return
        hex_error_message = (
            f'"{md5_hash}" is invalid, expecting 32 hexadecimal characters'
        )
        if not self._allow_base64_encoding:
            raise ValueError(hex_error_message)

        base64_error_message = f"{hex_error_message} or Base64 encoding"
        try:
            md5_hash_in_bytes = bytes(md5_hash, encoding="utf-8")
            base64_decoded_md5 = b64decode(md5_hash_in_bytes)
            if b64encode(base64_decoded_md5) != md5_hash_in_bytes:
                raise Exception
            if len(base64_decoded_md5) * 8 != 128:
                base64_error_message = f'{base64_error_message}. Base64 decoded value for "{md5_hash}" contains {len(base64_decoded_md5)*8} bits, expecting 128 bits'
                raise Exception
        except Exception:
            raise ValueError(base64_error_message)


class SizeValidator(Validator):
    """
    Implements validation for the size manifest column. Validation is performed
    using the validate instance method, which raises a ValueError if the size
    value is invalid. Note below that a valid size value can be enclosed with a
    single pair of double quotes and no error will be raised

    Examples:
    ```
    size_validator = SizeValidator()
    size_validator.validate("3")     # does not raise any errors
    size_validator.validate('"3"')   # does not raise any errors
    size_validator.validate("[3]")   # raises ValueError
    size_validator.validate("3.4")   # raises ValueError
    ```
    """

    ALLOWED_COLUMN_NAMES = SIZE_COLUMN_NAMES

    @staticmethod
    def _validate_single_value(size):
        """
        Validates file size by raising a ValueError if an error was found.

        Args:
            size(str): size that gets validated

        Returns:
            None
        """
        try:
            x = int(size)
        except Exception:
            raise ValueError(f'"{size}" is not an integer')
        if x < 0:
            raise ValueError(f'"{size}" is negative, expecting non-negative integer')


class URLValidator(Validator):
    """
    Implements validation for the url manifest column. Validation is performed
    using the validate instance method, which raises a ValueError if a single
    extracted url is invalid and raises a MultiValueError if two or more
    extracted urls are invalid

    Examples:
    ```
    url_validator = URLValidator()
    url_validator.validate("s3://test_bucket/test_object")                                              # does not raise any errors
    url_validator.validate('["s3://test_bucket/test_object", "s3://test_bucket/test_object2"]')         # does not raise any errors
    url_validator.validate('s3://test_bucket/test_object s3://test_bucket/test_object2')                # does not raise any errors
    url_validator.validate("wrong://test_bucket/test_object")                                           # raises ValueError
    url_validator.validate('["wrong://test_bucket/test_object", "wrong://test_bucket/test_object2"]')   # raises MultiValueError
    ```
    """

    ALLOWED_COLUMN_NAMES = URLS_COLUMN_NAMES

    def __init__(self, allowed_protocols=["s3", "gs"], error_on_empty=False):
        """
        Performs basic initialization

        Args:
            allowed_protocols(list(str), optional): allowed protocols. note
                that if allowed_protocols is provided by caller, url values will
                only be validated using the provided protocols (e.g. if a
                URLValidator instance was initialized with
                allowed_protocols=["http", "https"], an error would be raised
                when validating "s3://valid_bucket/valid_key" url)
            error_on_empty(bool, optional): if False, raise EmptyWarning when
                validating an empty url value.  if True, raise ValueError when
                validating an empty url value
        """
        self._allowed_protocols = allowed_protocols
        self._error_on_empty = error_on_empty
        self._expectation_message = f'expecting URL in format "<protocol>://<hostname>/<path>", with protocol being one of {self._allowed_protocols}'

    def validate(self, urls):
        """
        Overrides Validator's validate method, and wraps inherited
        _validate_mulitple_values method

        Args:
            urls(str): the urls to validate

        Returns:
            NoneType
        """
        self._validate_mulitple_values(urls)

    def _is_single_value_valid(self, url):
        """
        Determines if a single url is valid. Requires _allowed_protocols
        attribute to be initialized on instance

        Args:
            url(str): the url whose validity is being determined

        Returns:
            bool: True if valid. False if not
        """
        try:
            result = urlparse(url)
            if (
                result.scheme not in self._allowed_protocols
                or not all([result.netloc, result.path])
                or result.path == "/"
            ):
                raise Exception()
        except Exception:
            return False
        return True


class AuthzValidator(Validator):
    """
    Implements validation for the authz manifest column. Validation is performed
    using the validate instance method, which raises a ValueError if a single
    extracted authz resource is invalid and raises a MultiValueError if two or more
    extracted authz resources are invalid

    Examples:
    ```
    authz_validator = AuthzValidator()
    authz_validator.validate("/resource/subresource")                                   # does not raise any errors
    authz_validator.validate('["/resource/subresource1", "/resource/subresource2"]')    # does not raise any errors
    authz_validator.validate('/resource/subresource1 /resource/subresource2')           # does not raise any errors
    authz_validator.validate("wrong")                                                   # raises ValueError
    authz_validator.validate('["wrong1", "wrong2"]')                                    # raises MultiValueError
    ```
    """

    ALLOWED_COLUMN_NAMES = AUTHZ_COLUMN_NAMES

    def __init__(self):
        """
        Performs basic initialization
        """
        self._error_on_empty = False
        self._expectation_message = f'expecting authz resource in format "/<resource>/<subresource>/.../<subresource>"'

    def validate(self, authz_resources):
        """
        Overrides Validator's validate method, and wraps inherited
        _validate_mulitple_values method

        Args:
            authz_resources(str): the authz resources to validate

        Returns:
            NoneType
        """
        self._validate_mulitple_values(authz_resources)

    @staticmethod
    def _is_single_value_valid(authz_resource):
        """
        Determines if a single authz resource is valid

        Args:
            authz_resource(str): the authz resource whose validity is being
                determined

        Returns:
            bool: True if valid. False if not
        """
        return authz_resource.startswith("/") and all(
            authz_resource.strip("/").split("/")
        )
