import sys
import pytest
import logging

from gen3.tools.indexing.verify_manifest_format import is_valid_manifest_format
from gen3.tools.indexing.manifest_columns import Columns


@pytest.fixture(autouse=True)
def set_log_level_to_error():
    logging.getLogger().setLevel(logging.ERROR)
    yield


def test_is_valid_manifest_format_with_no_errors(caplog):
    assert (
        is_valid_manifest_format(
            "tests/verify_manifest_format/manifests/manifest_with_no_errors.tsv"
        )
        == True
    )
    assert caplog.text == ""


def test_is_valid_manifest_format_with_csv(caplog):
    assert is_valid_manifest_format("tests/test_manifest.csv") == True
    assert caplog.text == ""


def manifest_with_many_types_of_errors_helper(error_log):
    assert '"invalid_authz"' in error_log
    assert '"invalid_int"' in error_log
    assert '"invalid_md5"' in error_log
    assert '"invalid_url"' in error_log


def test_is_valid_manifest_format_with_many_types_of_errors(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_many_types_of_errors.tsv",
    )
    error_log = caplog.text
    manifest_with_many_types_of_errors_helper(error_log)
    assert result == False


def test_is_valid_manifest_format_using_column_names_to_enums(caplog):
    column_names_to_enums = {
        "md5_with_underscores": Columns.MD5,
        "file size with spaces": Columns.SIZE,
        "Urls With Caps": Columns.URL,
        "authz with special chars!@*&": Columns.AUTHZ,
    }
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_custom_column_names.tsv",
        column_names_to_enums=column_names_to_enums,
    )
    error_log = caplog.text
    manifest_with_many_types_of_errors_helper(error_log)
    assert result == False


def manifest_with_invalid_md5_values_helper(error_log):
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


def test_is_valid_manifest_format_with_invalid_md5_values(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_md5_values.tsv"
    )

    error_log = caplog.text
    manifest_with_invalid_md5_values_helper(error_log)
    base64_encoded_md5 = '"jd2L5LF5pSmvpfL/rkuYWA=="'
    assert base64_encoded_md5 in error_log
    assert result == False


def test_is_valid_manifest_format_allowing_base64_encoded_md5(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_md5_values.tsv",
        allow_base64_encoded_md5=True,
    )

    error_log = caplog.text
    manifest_with_invalid_md5_values_helper(error_log)
    base64_encoded_md5 = '"jd2L5LF5pSmvpfL/rkuYWA=="'
    assert base64_encoded_md5 not in error_log
    assert result == False


def test_is_valid_manifest_format_with_invalid_sizes(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_sizes.tsv"
    )
    error_log = caplog.text
    assert "-1" in error_log
    assert "not_an_int" in error_log
    assert "3.34" in error_log
    assert "string_with_42" in error_log
    assert result == False


def test_is_valid_manifest_format_with_invalid_urls(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_urls.tsv"
    )
    error_log = caplog.text
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
    assert '""""' in error_log
    assert "\"''\"" in error_log
    assert '"[]"' in error_log
    assert "\"['']\"" in error_log
    assert '"[""]"' in error_log
    assert '"["", ""]"' in error_log
    assert '"["", \'\']"' in error_log
    assert result == False


def test_is_valid_manifest_format_using_allowed_protocols(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_custom_url_protocols.tsv",
        allowed_protocols=["s3", "gs", "http", "https"],
    )
    error_log = caplog.text
    assert "gs://test/test.txt" not in error_log
    assert "s3://testaws/aws/test.txt" not in error_log
    assert "https://www.uchicago.edu/about" not in error_log
    assert "http://en.wikipedia.org/wiki/University_of_Chicago" not in error_log

    assert '"s3://bucket_without_path"' in error_log
    assert '"wrong_protocol://test_bucket/test.txt"' in error_log
    assert result == False


def test_is_valid_manifest_format_with_invalid_authz_resources(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_authz_resources.tsv",
    )
    error_log = caplog.text
    assert '"invalid_authz"' in error_log
    assert '"/"' in error_log
    assert '"//"' in error_log
    assert '"///"' in error_log
    assert '"invalid_authz2"' in error_log
    assert result == False


def test_is_valid_manifest_format_using_line_limit(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_sizes.tsv",
        line_limit=3,
    )
    error_log = caplog.text
    assert "line 2" in error_log
    assert "line 3" in error_log
    assert "line 4" not in error_log
    assert "line 5" not in error_log
    assert result == False


def test_is_valid_manifest_format_with_empty_url(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_empty_url.tsv",
    )
    assert caplog.text == ""
    assert result == True


def test_is_valid_manifest_format_using_error_on_empty_url(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_empty_url.tsv",
        error_on_empty_url=True,
    )
    assert '""' in caplog.text
    assert result == False


def test_is_valid_manifest_with_wide_row(caplog):
    logging.getLogger().setLevel(logging.WARNING)
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_wide_row.tsv",
    )
    wide_warning = f"line 3, number of fields (6) in row is unequal to number of column names in manifest (5)"
    assert wide_warning in caplog.text
    assert result == True


def test_is_valid_manifest_with_missing_md5_column(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_missing_md5_column.tsv",
    )
    missing_md5_message = (
        'could not find a column name corresponding to required "Columns.MD5"'
    )
    assert missing_md5_message in caplog.text
    assert result == False


def test_is_valid_manifest_with_missing_size_column(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_missing_size_column.tsv",
    )
    missing_size_message = (
        'could not find a column name corresponding to required "Columns.SIZE"'
    )
    assert missing_size_message in caplog.text
    assert result == False


def test_is_valid_manifest_with_missing_url_column(caplog):
    logging.getLogger().setLevel(logging.WARNING)
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_missing_url_column.tsv",
    )
    missing_size_message = (
        'could not find a column name corresponding to required "Columns.URL"'
    )
    assert missing_size_message in caplog.text
    assert result == True


def test_is_valid_manifest_with_missing_url_column_and_error_on_empty_url(caplog):
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_missing_url_column.tsv",
        error_on_empty_url=True,
    )
    missing_size_message = (
        'could not find a column name corresponding to required "Columns.URL"'
    )
    assert missing_size_message in caplog.text
    assert result == False
