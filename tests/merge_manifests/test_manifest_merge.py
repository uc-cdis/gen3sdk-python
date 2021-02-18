import csv
import pytest
from gen3.tools.indexing import merge_bucket_manifests
from gen3.tools.indexing.manifest_columns import MD5_STANDARD_KEY, GUID_STANDARD_KEY


def test_regular_merge_bucket_manifests():
    """
    Test that the output manifest produced by merge_bucket_manifests for a
    given input directory matches the expected output manifest.
    """
    merge_bucket_manifests(
        directory="tests/merge_manifests/regular/input/",
        output_manifest="merged-output-test-manifest.tsv",
    )
    assert _get_tsv_data("merged-output-test-manifest.tsv") == _get_tsv_data(
        "tests/merge_manifests/regular/expected-merged-output-manifest.tsv"
    )


def test_writing_to_csv():
    """
    Test that output manifest can be written as csv.
    """
    merge_bucket_manifests(
        directory="tests/merge_manifests/regular/input/",
        output_manifest="merged-output-test-manifest.csv",
    )
    assert _get_tsv_data("merged-output-test-manifest.csv", ",") == _get_tsv_data(
        "tests/merge_manifests/regular/expected-merged-output-manifest.tsv"
    )


def test_multiple_guids_per_hash():
    """
    Test multiple guids per hash.
    """
    merge_bucket_manifests(
        directory="tests/merge_manifests/multiple_guids_per_hash/input",
        output_manifest="merged-output-test-manifest.tsv",
        allow_mult_guids_per_hash=True,
    )
    assert _get_tsv_data("merged-output-test-manifest.tsv") == _get_tsv_data(
        "tests/merge_manifests/multiple_guids_per_hash/expected-merged-output-manifest.tsv"
    )


def test_same_guid_for_same_hash():
    """
    Test input manifests with rows having matching guids, md5, and size.
    """
    merge_bucket_manifests(
        directory="tests/merge_manifests/same_guid_for_same_hash/input",
        output_manifest="merged-output-test-manifest.tsv",
        allow_mult_guids_per_hash=True,
    )
    assert _get_tsv_data("merged-output-test-manifest.tsv") == _get_tsv_data(
        "tests/merge_manifests/same_guid_for_same_hash/expected-merged-output-manifest.tsv"
    )


def test_column_mismatch():
    """
    Test that rearranged columns within manifests with different names match up
    """

    merge_bucket_manifests(
        directory="tests/merge_manifests/column_mismatch/input",
        output_manifest="merged-output-test-manifest.tsv",
    )
    tsv_data = _get_tsv_data("merged-output-test-manifest.tsv")
    expected = _get_tsv_data(
        "tests/merge_manifests/column_mismatch/expected-merged-output-manifest.tsv"
    )
    assert tsv_data == expected


def test_multiple_urls():
    """
    Test input manifest having a row with multiple urls.
    """
    merge_bucket_manifests(
        directory="tests/merge_manifests/multiple_urls/input",
        output_manifest="merged-output-test-manifest.tsv",
    )
    assert _get_tsv_data("merged-output-test-manifest.tsv") == _get_tsv_data(
        "tests/merge_manifests/multiple_urls/expected-merged-output-manifest.tsv"
    )


def test_duplicate_values():
    """
    Test two input manifests having duplicate values ("sushi" in manifest2.tsv
    and manifest3.tsv)
    """
    merge_bucket_manifests(
        directory="tests/merge_manifests/duplicate_values/input",
        output_manifest="merged-output-test-manifest.tsv",
    )
    assert _get_tsv_data("merged-output-test-manifest.tsv") == _get_tsv_data(
        "tests/merge_manifests/duplicate_values/expected-merged-output-manifest.tsv"
    )


def test_size_mismatch():
    """
    Test that an error is raised when two manifests have rows with same md5 but
    different sizes.
    """
    with pytest.raises(csv.Error):
        merge_bucket_manifests(
            directory="tests/merge_manifests/size_mismatch/input",
            output_manifest="merged-output-test-manifest.tsv",
        )


def _get_tsv_data(manifest, delimiter="\t"):
    """
    Returns a list of rows sorted by md5 for the given manifest.
    """
    csv_data = list()
    with open(manifest) as f:
        rows = []
        reader = csv.DictReader(f, delimiter=delimiter)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(dict({k: sorted(v.split(" ")) for k, v in row.items()}))

    return sorted(
        rows, key=lambda row: (row[MD5_STANDARD_KEY][0], row[GUID_STANDARD_KEY][0])
    )
