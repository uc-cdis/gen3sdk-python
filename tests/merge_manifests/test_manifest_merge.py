import csv
from gen3.tools.indexing import merge_bucket_manifests


def test_merge_bucket_manifests():
    """
    Test that the output manifest produced by merge_bucket_manifests for a
    given input directory matches the expected output manifest. Since
    merge_bucket_manifests doesn't guarantee any specific file order in its
    output manifest, _get_ordered_csv_data is utilized for the comparison
    between actual and expected output manifest data.
    """
    merge_bucket_manifests(
        directory="tests/merge_manifests/input_manifests/",
        output_manifest="merged-output-test-manifest.tsv",
    )
    assert _get_ordered_csv_data(
        "merged-output-test-manifest.tsv"
    ) == _get_ordered_csv_data(
        "tests/merge_manifests/expected-merged-output-manifest.tsv"
    )


def _get_ordered_csv_data(manifest):
    """
    Returns a list of rows ordered by md5 for the given manifest.
    """
    csv_data = list()
    with open(manifest) as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)
        for url, size, md5, authz, acl in reader:
            csv_data.append(
                tuple((set(url.split(" ")), size, md5, set(authz.split(" "))))
            )
    csv_data.sort(key=lambda t: t[2])
    return csv_data
