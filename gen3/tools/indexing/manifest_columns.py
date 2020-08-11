#  XXX reorder
import string
from enum import Enum, unique
import pdb
from urllib.parse import urlparse
from base64 import b64encode, b64decode


@unique
class Columns(Enum):
    MD5 = 1
    URL = 2
    SIZE = 3
    AUTHZ = 4
    ACL = 5


class Validator:
    #  XXX change to variable?
    def allowed_column_names(self):
        return []

    #  XXX abstract?
    def validate(self, value):
        return False

    #  XXX catch errors?
    #  XXX rename
    def _parse_values(self, values):
        values.translate(values.maketrans("[],\"'", "     "))
        return values.split()


class MD5Validator(Validator):
    def __init__(self, require_non_base64_md5=True):
        self._require_non_base64_md5 = require_non_base64_md5

    def allowed_column_names(self):
        return ["md5"]

    def validate(self, md5_hash):
        error_message = None
        if not self._require_non_base64_md5:
            try:
                #  pdb.set_trace()
                md5_hash_in_bytes = bytes(md5_hash, encoding="utf-8")
                base64_decoded_md5 = b64decode(md5_hash_in_bytes)
                if len(base64_decoded_md5) * 8 != 128:
                    error_message = f'base64 decoded value for "{md5_hash}" contains {len(base64_decoded_md5)*8} bits, expecting 128 bits'
                if b64encode(b64decode(md5_hash_in_bytes)) != md5_hash_in_bytes:
                    raise Exception
            except Exception:
                error_message = f'"{md5_hash}" is not properly base64 encoded'
        else:
            if any(c not in string.hexdigits for c in md5_hash):
                error_message = f'"{md5_hash}" contains non-hexadecimal characters, expecting each character to be one of "{string.hexdigits}"'
            if len(md5_hash) != 32:
                error_message = f'"{md5_hash}" has {len(md5_hash)} hexadecimal characters, expecting 32'

        if error_message:
            raise ValueError(error_message)


class URLValidator(Validator):
    def __init__(self, allowed_protocols=["s3", "gs"]):
        self._allowed_protocols = allowed_protocols

    def allowed_column_names(self):
        return ["url", "urls"]

    def validate(self, urls):
        error_messages = []
        for url in self._parse_values(urls):
            try:
                result = urlparse(url)
                if result.scheme not in self._allowed_protocols or not all(
                    [result.netloc, result.path]
                ):
                    raise ValueError()
            except:
                error_messages.append(f'"{url}" is not a valid URL')
        if error_messages:
            error_messages.append(
                f'Expecting URL in format "<protocol>://<hostname>/<path>", with protocol being one of {self._allowed_protocols}'
            )
            raise ValueError(". ".join(error_messages))


class SizeValidator(Validator):
    def allowed_column_names(self):
        return ["size", "file_size"]

    def validate(self, size):
        error_message = None
        try:
            x = int(size)
            if x < 0:
                error_message = f'"{size}" is negative, expecting non-negative'
        except:
            error_message = f'"{size}" is not of int type'

        if error_message:
            raise ValueError(error_message)


class AuthzValidator(Validator):
    def allowed_column_names(self):
        return ["authz"]

    def validate(self, authz_resources):
        error_messages = []
        for authz_resource in self._parse_values(authz_resources):
            if not authz_resource.startswith("/") or not all(
                authz_resource.strip("/").split("/")
            ):
                error_messages.append(
                    f'"{authz_resource}" is not a valid authz resource'
                )

        if error_messages:
            error_messages.append(
                f'Expecting authz resource in format "/<resource>/<subresource>"'
            )
            raise ValueError(". ".join(error_messages))
