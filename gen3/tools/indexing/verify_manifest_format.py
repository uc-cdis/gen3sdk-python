import csv

import logging
from enum import Enum, unique
from collections import OrderedDict, defaultdict
from urllib.parse import urlparse
from base64 import b64encode, b64decode
import string


@unique
class Columns(Enum):
    MD5 = 1
    URL = 2
    SIZE = 3
    AUTHZ = 4
    ACL = 5


#  XXX move to separate file?
class Validator:
    #  XXX change to variable?
    def allowed_headers(self):
        return []

    #  XXX abstract?
    def validate(self, value):
        return False

    #  XXX change to variable?
    def error_message(self):
        return ". ".join(self._error_messages)

    #  XXX catch errors?
    def _parse_values(self, values):
        values.translate(values.maketrans("[],\"'", "     "))
        return values.split()


class MD5Validator(Validator):
    def __init__(self, require_non_base64_md5):
        self._require_non_base64_md5 = require_non_base64_md5

    def allowed_headers(self):
        return ["md5"]

    def validate(self, md5_hash):
        self._error_messages = []
        #  XXX allow for both types of md5
        if not self._require_non_base64_md5:
            try:
                md5_hash_bytes = bytes(md5_hash, encoding="utf-8")
                base64_decoded_md5 = b64decode(md5_hash_bytes)
                if len(base64_decoded_md5) != 16:
                    self._error_messages.append(
                        f"base64 encoded md5 hash {md5_hash} contains {len(base64_decoded_md5)*8} bits, expecting 128 bits"
                    )
                if b64encode(b64decode(md5_hash_bytes)) != md5_hash_bytes:
                    raise Exception
            except Exception:
                self._error_messages.append(
                    f"md5 hash {md5_hash} could not be decoded using base64"
                )
        else:
            if len(md5_hash) != 32:
                self._error_messages.append(
                    f"md5 hash {md5_hash} has incorrect number of hexadecimal values, expecting 32"
                )
            if any(c not in string.hexdigits for c in md5_hash):
                self._error_messages.append(
                    f'md5 hash {md5_hash} contains non-hexadecimal values, expecting each value to be one of "{string.hexdigits}"'
                )
        #  if not re.match(pattern, md5_hash):
        #  self._error_messages.append(f'')
        return not self._error_messages


class URLValidator(Validator):
    def __init__(self, allowed_protocols):
        self._allowed_protocols = allowed_protocols

    def validate(self, urls):
        self._error_messages = []
        for url in self._parse_values(urls):
            try:
                result = urlparse(url)
                if result.scheme not in self._allowed_protocols:
                    self._error_messages.append(
                        f'protocol "{result.scheme}" for URL "{url}" is invalid, expecting one of: {self._allowed_protocols}'
                    )
                if not result.netloc:
                    self._error_messages.append(f'URL "{url}" is missing bucket name')
            except:
                self._error_messages.append(f"URL {url} could not be parsed")

        #  self._error_message = ". ".join(messages)
        return not self._error_messages


class SizeValidator(Validator):
    def allowed_headers(self):
        return ["size", "file_size"]

    def validate(self, size):
        self._error_messages = []
        try:
            x = int(size)
            if x < 0:
                self._error_messages.append(f"{size} must be non-negative")
        except:
            self._error_messages.append(f'{size} must be of "int" type')

        return not self._error_messages


class AuthzValidator(Validator):
    def allowed_headers(self):
        return ["authz"]

    def validate(self, authz_resources):
        self._error_messages = []
        for authz_resource in self._parse_values(authz_resources):
            if len(authz_resource) < 2 or not authz_resource.startswith("/"):
                self._error_messages.append(
                    f'authz resource "{authz_resource}" is invalid, expecting it to begin with "/" and contain at least 2 characters'
                )

        return not self._error_messages


#  XXX implement
def update_allowed_column_names(validators, allowed_column_names):
    pass


#  XXX configurable set of protocols
#  XXX handle incorrect number of values in rows
def is_valid_manifest_format(
    manifest_path,
    allowed_column_names=None,
    #  XXX implement
    error_on_empty_url=False,
    required_columns=[Columns.MD5, Columns.SIZE],
    require_non_base64_md5=True,
):

    validators = {
        Columns.MD5: MD5Validator(require_non_base64_md5),
        Columns.URL: URLValidator(["s3", "gs"]),
        Columns.SIZE: SizeValidator(),
        Columns.AUTHZ: AuthzValidator(),
        #  Columns.ACL: ACLValidator(),
    }
    update_allowed_column_names(validators, allowed_column_names)

    #  XXX move closures to global
    #  XXX prefix with _
    def order_validators(normalized_headers, validators):
        ordered_validators = len(normalized_headers) * [None]
        for i, column in enumerate(normalized_headers.values()):
            if column in validators:
                ordered_validators[i] = validators[column]
        return ordered_validators

    def get_csv_reader(csv_file):
        dialect = csv.Sniffer().sniff(dsv_file.readline())
        dsv_file.seek(0)
        return csv.reader(dsv_file, dialect)

    def normalize_headers(manifest_headers, validators):
        all_allowed_headers = defaultdict(lambda: None)
        for column, validator in validators.items():
            for allowed_header in validator.allowed_headers():
                all_allowed_headers[allowed_header] = column

        return OrderedDict([(h, all_allowed_headers[h]) for h in manifest_headers])

    def log_error(line_number, message):
        logging.error(f"error: line {line_number}, message")

    def validate_headers(normalized_headers, required_headers):
        is_header_valid = True
        manifest_columns = normalized_headers.values()
        for required_header in required_headers:
            if required_header not in manifest_columns:
                is_header_valid = False
                #  XXX will probably fail
                #  log_error(0, f"required header {required_header} missing")
                logging.error(f"line 0, required header {required_header} missing")
        return is_header_valid

    def validate_rows(dsv_reader, ordered_validators):
        print("Validating rows")
        rows_are_valid = True
        #  XXX rename row
        for line_number, row in enumerate(dsv_reader, 1):
            for i, value in enumerate(row):
                validator = ordered_validators[i]
                if validator and not validator.validate(value):
                    rows_are_valid = False
                    #  log_error(line_number, validator.error_message)
                    logging.error(f"line {line_number}, {validator.error_message()}")
        return rows_are_valid

    with open(manifest_path) as dsv_file:
        #  XXX be consistent with csv/dsv
        dsv_reader = get_csv_reader(dsv_file)
        manifest_headers = next(dsv_reader)
        normalized_headers = normalize_headers(manifest_headers, validators)
        manifest_is_valid = validate_headers(normalized_headers, required_columns)
        #  print("Completed validating headers")

        ordered_validators = order_validators(normalized_headers, validators)
        #  print("manifest headers: " + str(manifest_headers))
        #  print("noramlized headers: " + str(normalized_headers))
        #  print("ordered_validators: " + str(ordered_validators))
        manifest_is_valid = (
            validate_rows(dsv_reader, ordered_validators) and manifest_is_valid
        )

    return manifest_is_valid


if __name__ == "__main__":
    print(
        is_valid_manifest_format(
            #  "manifest_cdistest-giangb-bucket1-databucket-gen3_05_11_20_23_32_13.tsv",
            #  "tests/test_manifest.csv",
            "test.tsv",
            #  "tests/merge_manifests/input_manifests/manifest1.tsv",
            #  required_columns=[Columns.MD5, Columns.SIZE, Columns.ACL],
        )
    )
