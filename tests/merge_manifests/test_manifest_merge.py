import csv
from gen3.tools.indexing import merge_bucket_manifests


def test_merge_bucket_manifests():
    """
    Test that the output manifest produced by merge_bucket_manifests for a
    given input directory matches the expected output manifest.
    """
    merge_bucket_manifests(
        directory="tests/merge_manifests/input_manifests/",
        output_manifest="merged-output-test-manifest.tsv",
    )
    assert _get_tsv_data("merged-output-test-manifest.tsv") == _get_tsv_data(
        "tests/merge_manifests/expected-merged-output-manifest.tsv"
    )


def _get_tsv_data(manifest):
    """
    Returns a list of rows for the given manifest.
    """
    csv_data = list()
    with open(manifest) as f:
        rows = []
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(dict(row))

    return rows
