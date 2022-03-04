import os
import pytest
import logging as default_logging
from gen3 import logging, LOG_FORMAT

from gen3.tools.indexing import is_valid_manifest_format
from gen3.tools.indexing.manifest_columns import Columns


@pytest.fixture(autouse=True)
def set_log_level_to_warning():
    """
    By default, only log errors and setup a log output file to read from later
    """
    logging.setLevel(default_logging.WARNING)

    if os.path.exists("gen3tests.logs"):
        os.remove("gen3tests.logs")
    logfile_handler = default_logging.FileHandler("gen3tests.logs")
    logfile_handler.setFormatter(default_logging.Formatter(LOG_FORMAT))
    logging.addHandler(logfile_handler)
    yield


@pytest.fixture()
def logfile():
    """
    Read from log output file
    """

    class Logfile(object):
        def __init__(self, filename, *args, **kwargs):
            super(Logfile, self).__init__(*args, **kwargs)
            self.filename = filename
            self.logs = ""

        def read(self):
            with open(self.filename) as file:
                for line in file:
                    self.logs += line
            return self.logs

    yield Logfile(filename="gen3tests.logs")

    # cleanup after each use
    if os.path.exists("gen3tests.logs"):
        os.remove("gen3tests.logs")


def test_is_valid_manifest_format_with_no_errors(logfile):
    """
    Test that no errors occur for manifest without errors
    """
    assert (
        is_valid_manifest_format(
            "tests/validate_manifest_format/manifests/manifest_with_no_errors.tsv"
        )
        == True
    )
    assert logfile.read() == ""


def test_is_valid_manifest_format_with_csv(logfile):
    """
    Test that alternative delimiter can be automatically detected
    """
    logging.setLevel(default_logging.ERROR)

    assert is_valid_manifest_format("tests/test_data/test_manifest.csv") == True
    assert logfile.read() == ""

    logging.setLevel(default_logging.WARNING)


def manifest_with_many_types_of_errors_helper(error_log):
    """
    Helper method to assert that invalid values appear in error log generated
    after validating:
        "tests/validate_manifest_format/manifests/manifest_with_many_types_of_errors.tsv"
        "tests/validate_manifest_format/manifests/manifest_with_custom_column_names.tsv"
    """
    assert '"invalid_authz"' in error_log
    assert '"invalid_int"' in error_log
    assert '"invalid_md5"' in error_log
    assert '"invalid_url"' in error_log


def test_is_valid_manifest_format_with_many_types_of_errors(logfile):
    """
    Test that errors with md5, file size, url, and authz all get detected and
    error logged
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_many_types_of_errors.tsv",
    )
    error_log = logfile.read()
    manifest_with_many_types_of_errors_helper(error_log)
    assert result == False


def test_is_valid_manifest_format_using_column_names_to_enums(logfile):
    """
    Test that custom manifest column names can be used
    """
    column_names_to_enums = {
        "md5_with_underscores": Columns.MD5,
        "file size with spaces": Columns.SIZE,
        "Urls With Caps": Columns.URL,
        "authz with special chars!@*&": Columns.AUTHZ,
    }
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_custom_column_names.tsv",
        column_names_to_enums=column_names_to_enums,
    )
    error_log = logfile.read()
    manifest_with_many_types_of_errors_helper(error_log)
    assert result == False


def manifest_with_invalid_md5_values_helper(error_log):
    """
    Helper method to assert that invalid values appear in error log generated
    after validating:
    "tests/validate_manifest_format/manifests/manifest_with_invalid_md5_values.tsv"
    """
    valid_md5 = '"1596f493ba9ec53023fca640fb69bd3b"'  # pragma: allowlist secret
    assert valid_md5 not in error_log

    short_md5 = '"1596f493ba9ec53023fca640fb69bd3"'  # pragma: allowlist secret
    long_md5 = '"d9a68f3d5d9ce03f8a08f509242472234"'  # pragma: allowlist secret
    md5_with_non_hexadecimal = (
        '"5J1bf75c48761b2e755adc1340e5a9259"'  # pragma: allowlist secret
    )
    short_base64_encoded_md5 = '"aGVsbG8="'
    assert short_md5 in error_log
    assert long_md5 in error_log
    assert md5_with_non_hexadecimal in error_log
    assert short_base64_encoded_md5 in error_log


def test_is_valid_manifest_format_with_invalid_md5_values(logfile):
    """
    Test that invalid md5 errors are detected and error logged
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_invalid_md5_values.tsv"
    )

    error_log = logfile.read()
    manifest_with_invalid_md5_values_helper(error_log)
    base64_encoded_md5 = '"jd2L5LF5pSmvpfL/rkuYWA=="'
    assert base64_encoded_md5 in error_log
    assert result == False


def test_is_valid_manifest_format_allowing_base64_encoded_md5(logfile):
    """
    Test that valid Base64 encoded md5 does not get reported in error log when
    allow_base64_encoded_md5 is used
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_invalid_md5_values.tsv",
        allow_base64_encoded_md5=True,
    )

    error_log = logfile.read()
    manifest_with_invalid_md5_values_helper(error_log)
    base64_encoded_md5 = '"jd2L5LF5pSmvpfL/rkuYWA=="'
    assert base64_encoded_md5 not in error_log
    assert result == False


def test_is_valid_manifest_format_with_invalid_sizes(logfile):
    """
    Test that invalid sizes are detected and error logged
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_invalid_sizes.tsv"
    )
    error_log = logfile.read()
    assert "-1" in error_log
    assert "not_an_int" in error_log
    assert "3.34" in error_log
    assert "string_with_42" in error_log
    assert result == False


def test_is_valid_manifest_format_with_invalid_urls(logfile):
    """
    Test that invalid urls are detected and error logged
    Test that empty arrays and empty quote pairs are detected and error logged
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_invalid_urls.tsv"
    )
    error_log = logfile.read()
    assert '"wrong_protocol://test_bucket/test.txt"' in error_log
    assert '"test/test.txt"' in error_log
    assert '"testaws/aws/test.txt"' in error_log
    assert '"://test_bucket/test.txt"' in error_log
    assert '"s3://"' in error_log
    assert '"gs://"' in error_log
    assert '"s3://bucket_without_object"' in error_log
    assert '"s3://bucket_without_object/"' in error_log
    assert '"test_bucket/aws/test.txt"' in error_log
    assert '"s3:/test_bucket/aws/test.txt"' in error_log
    assert '"s3:test_bucket/aws/test.txt"' in error_log
    assert '"://test_bucket/aws/test.txt"' in error_log
    assert '"s3test_bucket/aws/test.txt"' in error_log
    assert '"https://www.uchicago.edu"' in error_log
    assert '"https://www.uchicago.edu/about"' in error_log
    assert '"google.com/path"' in error_log

    # if the url resolves to nothing after replacing characters, the log may just say
    # "is empty" and not list the original value
    assert '""""' in error_log or "is empty" in error_log
    assert "\"''\"" in error_log or "is empty" in error_log
    assert '"[]"' in error_log or "is empty" in error_log
    assert "\"['']\"" in error_log or "is empty" in error_log
    assert '"[""]"' in error_log or "is empty" in error_log
    assert '"["" ""]"' in error_log or "is empty" in error_log
    assert '"["" \'\']"' in error_log or "is empty" in error_log
    assert result == False


def test_is_valid_manifest_format_using_allowed_protocols(logfile):
    """
    Test that user defined protocols can be used
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_custom_url_protocols.tsv",
        allowed_protocols=["s3", "gs", "http", "https"],
    )
    error_log = logfile.read()
    assert "gs://test/test.txt" not in error_log
    assert "s3://testaws/aws/test.txt" not in error_log
    assert "https://www.uchicago.edu/about" not in error_log
    assert "http://en.wikipedia.org/wiki/University_of_Chicago" not in error_log

    assert '"s3://bucket_without_path"' in error_log
    assert '"wrong_protocol://test_bucket/test.txt"' in error_log
    assert result == False


def test_is_valid_manifest_format_with_invalid_authz_resources(logfile):
    """
    Test that invalid authz resources are detected and reported in error log
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_invalid_authz_resources.tsv",
    )
    error_log = logfile.read()
    assert '"invalid_authz"' in error_log
    assert '"/"' in error_log
    assert '"//"' in error_log
    assert '"///"' in error_log
    assert '"invalid_authz2"' in error_log
    assert result == False


def test_is_valid_manifest_format_using_line_limit(logfile):
    """
    Test that only first few lines of manifest can be validated
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_invalid_sizes.tsv",
        line_limit=3,
    )
    error_log = logfile.read()
    assert "line 2" in error_log
    assert "line 3" in error_log
    assert "line 4" not in error_log
    assert "line 5" not in error_log
    assert result == False


def test_is_valid_manifest_format_with_empty_url(logfile):
    """
    Test that by default, completely empty url values are allowed
    """
    logging.setLevel(default_logging.ERROR)

    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_empty_url.tsv",
    )
    assert logfile.read() == ""
    assert result == True

    logging.setLevel(default_logging.WARNING)


def test_is_valid_manifest_format_using_error_on_empty_url(logfile):
    """
    Test that completely empty urls are detected and reported in error log when
    using error_on_empty_url
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_empty_url.tsv",
        error_on_empty_url=True,
    )
    assert '""' in logfile.read()
    assert result == False


def test_is_valid_manifest_with_wide_row(logfile):
    """
    Test that warning is generated for a wide row with an extra value
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_wide_row.tsv",
    )
    wide_warning = f"line 3, number of fields (6) in row is unequal to number of column names in manifest (5)"
    assert wide_warning in logfile.read()
    assert result == True


def test_is_valid_manifest_with_missing_md5_column(logfile):
    """
    Test that completely missing md5 column is detected and reported in error
    log
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_missing_md5_column.tsv",
    )
    missing_md5_message = (
        'could not find a column name corresponding to required "Columns.MD5"'
    )
    assert missing_md5_message in logfile.read()
    assert result == False


def test_is_valid_manifest_with_missing_size_column(logfile):
    """
    Test that completely missing size column is detected and reported in error
    log
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_missing_size_column.tsv",
    )
    missing_size_message = (
        'could not find a column name corresponding to required "Columns.SIZE"'
    )
    assert missing_size_message in logfile.read()
    assert result == False


def test_is_valid_manifest_with_missing_url_column(logfile):
    """
    Test that a warning is generated for completely missing url column by
    default
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_missing_url_column.tsv",
    )
    missing_size_message = (
        'could not find a column name corresponding to required "Columns.URL"'
    )
    assert missing_size_message in logfile.read()
    assert result == True


def test_is_valid_manifest_with_missing_url_column_and_error_on_empty_url(logfile):
    """
    Test that an error is generated for completely missing url column when using
    error_on_empty_url
    """
    result = is_valid_manifest_format(
        "tests/validate_manifest_format/manifests/manifest_with_missing_url_column.tsv",
        error_on_empty_url=True,
    )
    missing_size_message = (
        'could not find a column name corresponding to required "Columns.URL"'
    )
    assert missing_size_message in logfile.read()
    assert result == False
