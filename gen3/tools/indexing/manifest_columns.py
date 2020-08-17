import warnings
from abc import ABC
from enum import Enum, unique

import string
from urllib.parse import urlparse
from base64 import b64encode, b64decode


@unique
class Columns(Enum):
    MD5 = 1
    URL = 2
    SIZE = 3
    AUTHZ = 4


class EmptyWarning(Warning):
    pass


class MultiValueError(ValueError):
    pass


class Validator(ABC):
    def validate(self, value):
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        self._validate_single_value(value)

    def _validate_mulitple_values(self, values):
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
        values = values.translate(values.maketrans("[],\"'", "     "))
        return values.split()


class MD5Validator(Validator):

    ALLOWED_COLUMN_NAMES = ("md5", "md5_hash", "md5hash", "hash")

    def __init__(self, allow_base64_encoding=False):
        self._allow_base64_encoding = allow_base64_encoding

    def _validate_single_value(self, md5_hash):
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
        except:
            raise ValueError(base64_error_message)


class SizeValidator(Validator):

    ALLOWED_COLUMN_NAMES = ("size", "file_size", "filesize", "file size")

    @staticmethod
    def _validate_single_value(size):
        try:
            x = int(size)
        except:
            raise ValueError(f'"{size}" is not an integer')
        if x < 0:
            raise ValueError(f'"{size}" is negative, expecting non-negative integer')


class URLValidator(Validator):

    ALLOWED_COLUMN_NAMES = ("url", "urls")

    def __init__(self, allowed_protocols=["s3", "gs"], error_on_empty=False):
        self._allowed_protocols = allowed_protocols
        self._error_on_empty = error_on_empty
        self._expectation_message = f'expecting URL in format "<protocol>://<hostname>/<path>", with protocol being one of {self._allowed_protocols}'

    def validate(self, urls):
        self._validate_mulitple_values(urls)

    def _is_single_value_valid(self, url):
        try:
            result = urlparse(url)
            if (
                result.scheme not in self._allowed_protocols
                or not all([result.netloc, result.path])
                or result.path == "/"
            ):
                raise Exception()
        except:
            return False
        return True


class AuthzValidator(Validator):

    ALLOWED_COLUMN_NAMES = (
        "authz",
        "authz_resource",
        "authz_resources",
        "authz resource",
        "authz resources",
    )

    def __init__(self):
        self._error_on_empty = False
        self._expectation_message = f'expecting authz resource in format "/<resource>/<subresource>/.../<subresource>"'

    def validate(self, authz_resources):
        self._validate_mulitple_values(authz_resources)

    @staticmethod
    def _is_single_value_valid(authz_resource):
        return authz_resource.startswith("/") and all(
            authz_resource.strip("/").split("/")
        )
