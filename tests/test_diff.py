import pytest
import csv
import os
from gen3.tools.diff import manifest_diff
from pathlib import Path

cwd = os.path.dirname(os.path.realpath(__file__))


def test_directory_input_diff():
    """
    Test that the output manifest produced by manifest_diff for a
    given input directory matches the expected output manifest.
    """

    manifest_diff(
        directory=f"{cwd}/test_data/diff_manifests",
        output_manifest=f"{cwd}/test_data/manifest_diff1.tsv",
        key_column="guid",
    )
    assert check_diff(
        file=f"{cwd}/test_data/manifest_diff1.tsv",
        expected={
            "255e396f-f1f8-11e9-9a07-0a80fada010c": [
                "473d83400bc1bc9dc635e334fadde33c",
                "363455714",
                "['Open']",
                "['s3://pdcdatastore/test4.raw']",
            ],
            "255e396f-f1f8-11e9-9a07-0a80fada012c": [
                "473d83400bc1bc9dc635e334fadde33c",
                "363455715",
                "['Open']",
                "['s3://pdcdatastore/test6.raw']",
            ],
        },
    )


def test_file_input_diff():
    """
    Test that the output manifest produced by manifest_diff for a
    given file strings matches the expected output manifest.
    """

    manifest_diff(
        files=[f"{cwd}/test_data/manifest1.csv", f"{cwd}/test_data/manifest2.csv"],
        output_manifest=f"{cwd}/test_data/manifest_diff2.csv",
        key_column="guid",
    )
    assert check_diff(
        file=f"{cwd}/test_data/manifest_diff2.csv",
        expected={
            "255e396f-f1f8-11e9-9a07-0a80fada099c": [
                "473d83400bc1bc9dc635e334faddf33d",
                "363455714",
                "['Open']",
                "['s3://pdcdatastore/test1.raw']",
            ],
            "255e396f-f1f8-11e9-9a07-0a80fada098d": [
                "473d83400bc1bc9dc635e334faddd33c",
                "343434344",
                "['Open']",
                "['s3://pdcdatastore/test2.raw']",
            ],
            "255e396f-f1f8-11e9-9a07-0a80fada097c": [
                "473d83400bc1bc9dc635e334fadd433c",
                "543434443",
                "['phs0001']",
                "['s3://pdcdatastore/test3.raw']",
            ],
        },
    )


def test_file_input_mismatch():
    """
    Test for fail due to different file types.
    """

    with pytest.raises(Exception):
        manifest_diff(
            files=[
                "tests/test_data/manifest1.csv",
                "tests/test_data/diff_manifests/manifest3.tsv",
            ],
        )


def check_diff(
    file,
    expected,
    **kwargs,
):
    """
    Check resulting diff file with given dict of expected change.
    """

    if ".tsv" in file.lower():
        file_delimiter = "\t"
    else:
        file_delimiter = ","

    equivalent = True
    with open(file, "r", encoding="utf-8-sig") as csvfile:
        file_reader = csv.DictReader(csvfile, delimiter=file_delimiter)
        next(file_reader)

        for row in file_reader:
            diff_guid = row["guid"]
            expected_values = expected[diff_guid]
            for column in row:
                if column != "guid" and row[column] not in expected_values:
                    equivalent = False

    remove_manifest(file)
    return equivalent


def remove_manifest(file):
    if os.path.exists(file):
        os.remove(file)
    else:
        print("The file does not exist")
