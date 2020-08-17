#  XXX reorder
import string
from enum import Enum, unique
import pdb
from urllib.parse import urlparse
from base64 import b64encode, b64decode
import warnings
from abc import ABC


@unique
class Columns(Enum):
    MD5 = 1
    URL = 2
    SIZE = 3
    AUTHZ = 4


class EmptyWarning(Warning):
    pass


#  XXX use
#  class SingleValueError(ValueError):
#  pass


class MultiValueError(ValueError):
    pass


class Validator(ABC):
    #  @blah
    #  @staticmethod
    #  @abstractmethod
    #  def allowed_column_names():
    #  return []

    def validate(self, value):
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        self._validate_single_value(value)

    #  XXX abstract
    #  @abstractmethod
    #  def _validate_single_value(self, value):
    #  pass

    def _validate_mulitple_values(self, values):
        if not values and not self._error_on_empty:
            warnings.warn(EmptyWarning())
            return

        split_values = self._parse_values(values)
        if not split_values:
            raise ValueError(f'"{values}" is empty, {self._expectation_message}')

        invalid_values = [v for v in split_values if not self._is_single_value_valid(v)]

        if len(invalid_values) == 1:
            raise ValueError(
                f'"{invalid_values[0]}" is invalid, {self._expectation_message}'
            )
        elif len(invalid_values) > 1:
            formatted_invalid_values = ", ".join(f'"{s}"' for s in invalid_values)
            raise MultiValueError(
                f"{formatted_invalid_values} are invalid, {self._expectation_message}"
            )

    #  XXX abstract
    #  XXX classmethod or static
    #  def _is_single_value_valid(self, value):
    #  pass

    #  XXX catch errors?
    #  XXX rename
    #  XXX class method or static
    @staticmethod
    def _parse_values(values):
        values = values.translate(values.maketrans("[],\"'", "     "))
        return values.split()


class MD5Validator(Validator):
    ALLOWED_COLUMN_NAMES = ("md5", "md5_hash", "md5hash", "hash")

    def __init__(self, allow_base64_encoding=False):
        self._allow_base64_encoding = allow_base64_encoding

    #  @classmethod
    #  @staticmethod
    #  def allowed_column_names():
    #  return ["md5", "md5_hash", "md5hash", "hash"]

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

    #  def allowed_column_names(self):
    #  return ["size", "file_size", "file size", "filesize"]

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

    #  def allowed_column_names(self):
    #  return ["url", "urls"]

    #  def validate(self, urls):
    #  error_messages = []
    #  if not urls and not self._error_on_empty:
    #  warnings.warn(Warning("is empty"))
    #  return

    #  split_urls = self._parse_values(urls)
    #  if not split_urls:
    #  error_messages.append(f'"{urls}" does not contain any urls')
    #  for url in split_urls:
    #  try:
    #  result = urlparse(url)
    #  if result.scheme not in self._allowed_protocols or not all(
    #  [result.netloc, result.path]
    #  ):
    #  raise ValueError()

    #  except:
    #  error_messages.append(f'"{url}" is not a valid URL')
    #  if error_messages:
    #  error_messages.append(
    #  f'Expecting URL in format "<protocol>://<hostname>/<path>", with protocol being one of {self._allowed_protocols}'
    #  )
    #  raise ValueError(". ".join(error_messages))

    #  def validate(self, urls):
    #  if not urls and not self._error_on_empty:
    #  warnings.warn(EmptyWarning())
    #  return

    #  split_urls = self._parse_values(urls)
    #  if not split_urls:
    #  raise ValueError(f'"{urls}" is empty')

    #  invalid_urls = []
    #  for url in split_urls:
    #  try:
    #  result = urlparse(url)
    #  if result.scheme not in self._allowed_protocols or not all(
    #  [result.netloc, result.path]
    #  ):
    #  raise Exception()
    #  except:
    #  invalid_urls.append(url)

    #  suffix = f'expecting URL in format "<protocol>://<hostname>/<path>", with protocol being one of {self._allowed_protocols}'
    #  if len(invalid_urls) == 1:
    #  raise ValueError(f'"{invalid_urls[0]}" is invalid, {suffix}')
    #  elif len(invalid_urls) > 1:
    #  formatted_invalid_urls = ', '.join(f'"{s}"' for s in invalid_urls)
    #  raise MultiValueError(f'{formatted_invalid_urls} are invalid, {suffix}')

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

    #  def allowed_column_names(self):
    #  return ["authz"]

    #  def validate(self, authz_resources):
    #  error_messages = []
    #  for authz_resource in self._parse_values(authz_resources):
    #  if not authz_resource.startswith("/") or not all(
    #  authz_resource.strip("/").split("/")
    #  ):
    #  error_messages.append(
    #  f'"{authz_resource}" is not a valid authz resource'
    #  )

    #  if error_messages:
    #  #  XXX /.../
    #  error_messages.append(
    #  f'Expecting authz resource in format "/<resource>/<subresource>/.../<subresource>"'
    #  )
    #  raise ValueError(". ".join(error_messages))

    def validate(self, authz_resources):
        self._validate_mulitple_values(authz_resources)

    @staticmethod
    def _is_single_value_valid(authz_resource):
        #  def _is_single_value_valid(self, authz_resource):
        return authz_resource.startswith("/") and all(
            authz_resource.strip("/").split("/")
        )
