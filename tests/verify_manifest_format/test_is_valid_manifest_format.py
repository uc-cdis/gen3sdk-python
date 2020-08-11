from gen3.tools.indexing.verify_manifest_format import is_valid_manifest_format

#  from gen3.tools.indexing.manifest_columns import *
#  XXX take out
import pdb
import logging
import sys

#  from manifest_columns import *


def test_is_valid_manifest_format():
    #  XXX need to add more edge cases
    assert (
        is_valid_manifest_format(
            "tests/verify_manifest_format/manifests/manifest_with_no_errors.tsv"
        )
        == True
    )


def test_is_valid_manifest_format_with_csv():
    assert is_valid_manifest_format("tests/test_manifest.csv") == True


def test_is_valid_manifest_format_with_invalid_md5_values(capsys):
    logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_md5_values.tsv"
    )
    #  XXX quotes around error
    short_md5 = "1596f493ba9ec53023fca640fb69bd3"  # pragma: allowlist secret
    long_md5 = "d9a68f3d5d9ce03f8a08f509242472234"  # pragma: allowlist secret
    md5_with_non_hexadecimal = (
        "5J1bf75c48761b2e755adc1340e5a9259"  # pragma: allowlist secret
    )
    _, error = capsys.readouterr()
    assert short_md5 in error
    assert long_md5 in error
    assert md5_with_non_hexadecimal in error
    assert result == False


def test_is_valid_manifest_format_with_invalid_sizes(capsys):
    logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_sizes.tsv"
    )
    _, error = capsys.readouterr()
    assert "-1" in error
    assert "not_an_int" in error
    assert "3.34" in error
    assert "string_with_42" in error
    assert result == False


def test_is_valid_manifest_format_with_invalid_urls(capsys):
    logging.getLogger().addHandler(logging.StreamHandler(sys.stderr))
    result = is_valid_manifest_format(
        "tests/verify_manifest_format/manifests/manifest_with_invalid_urls.tsv"
    )
    _, error = capsys.readouterr()
    assert '"wrong_protocol://test_bucket/test.txt"' in error
    assert '"s3://"' in error
    assert '"gs://"' in error
    assert '"test_bucket/aws/test.txt"' in error
    assert result == False
