import asyncio
import csv
import json
import tempfile
from unittest.mock import patch
import pytest

from gen3.tools.metadata.discovery_objects import (
    output_discovery_objects,
    BASE_CSV_PARSER_SETTINGS,
    REQUIRED_OBJECT_FIELDS,
    OPTIONAL_OBJECT_FIELDS,
)


MOCK_METADATA_1 = {
    "str_key": "str_val \n \t \\",
    "listval": ["v1", "v2"],
    "dictval": {"k1": "v1", "k2": "v2"},
    "objects": [
        {
            "guid": "drs://dg.FOOBAR:082a288e-3da2-4806-9438-bc974cdb1cd7",
            "display_name": "Clinical TSV",
            "description": "TSV containing info about subjects.",
            "type": "null",
        },
        {
            "guid": "drs://dg.FOOBAR:f060149a-8a35-421e-802a-873612ee4874",
            "display_name": "Summary PDF",
        },
    ],
}

MOCK_METADATA_2 = {"other_key": "other_val", "objects": []}

MOCK_METADATA_SIDE_EFFECT = lambda *_, **__: {
    "guid1": {
        "_guid_type": "discovery_metadata",
        "gen3_discovery": MOCK_METADATA_1,
    },
    "guid2": {
        "_guid_type": "discovery_metadata",
        "gen3_discovery": MOCK_METADATA_2,
    },
}

EXPECTED_DISCOVERY_OBJECTS = [
    {
        "guid": "drs://dg.FOOBAR:082a288e-3da2-4806-9438-bc974cdb1cd7",
        "dataset_guid": "guid1",
        "description": "TSV containing info about subjects.",
        "display_name": "Clinical TSV",
        "type": "null",
    },
    {
        "guid": "drs://dg.FOOBAR:f060149a-8a35-421e-802a-873612ee4874",
        "dataset_guid": "guid1",
        "description": "",
        "display_name": "Summary PDF",
        "type": "",
    },
]


@patch("gen3.tools.metadata.discovery_objects._create_discovery_objects_filename")
@patch("gen3.metadata.Gen3Metadata.query")
def test_discovery_objects_read(
    metadata_query_patch, metadata_file_patch, gen3_auth, guid_type="discovery_metadata"
):
    """
    Test that when reading discovery objects with specified guids, the resulting output
    has the expected data with required columns.
    """
    guid1_discovery_metadata = MOCK_METADATA_1
    guid2_discovery_metadata = MOCK_METADATA_2
    metadata_query_patch.side_effect = MOCK_METADATA_SIDE_EFFECT

    # TSV output tests
    with tempfile.NamedTemporaryFile(suffix=".tsv", mode="a+") as outfile:
        metadata_file_patch.side_effect = lambda *_, **__: outfile.name
        dataset_guids = ["guid1", "guid2"]
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            output_discovery_objects(
                gen3_auth,
                dataset_guids=dataset_guids,
                endpoint="excommons.org",
                guid_type=guid_type,
            )
        )
        tsv_reader = csv.DictReader(outfile, **BASE_CSV_PARSER_SETTINGS)
        tsv_rows = list(tsv_reader)
        assert len(tsv_rows) == 2

        # output should have required columns, and values in them
        required_columns = REQUIRED_OBJECT_FIELDS
        tsv_column_names = tsv_reader.fieldnames
        assert required_columns.issubset(set(tsv_column_names))
        for row in tsv_rows:
            for r in required_columns:
                assert row[r]

        # output object guids should come from the correct dataset as per data in mds
        obj1_row, obj2_row = tsv_rows
        assert any(
            obj["guid"] == obj1_row["guid"]
            for obj in guid1_discovery_metadata["objects"]
        )
        assert any(
            obj["guid"] == obj2_row["guid"]
            for obj in guid1_discovery_metadata["objects"]
        )
        assert tsv_rows == EXPECTED_DISCOVERY_OBJECTS


@patch("gen3.tools.metadata.discovery_objects._create_discovery_objects_filename")
@patch("gen3.metadata.Gen3Metadata.query")
def test_discovery_objects_read_all(
    metadata_query_patch, metadata_file_patch, gen3_auth, guid_type="discovery_metadata"
):
    """
    Test that when reading discovery objects with no guids specified, the resulting output
    has all objects from all datsets from the mds query.
    """
    guid1_discovery_metadata = MOCK_METADATA_1
    guid2_discovery_metadata = MOCK_METADATA_2
    metadata_query_patch.side_effect = MOCK_METADATA_SIDE_EFFECT

    with tempfile.NamedTemporaryFile(suffix=".tsv", mode="a+") as outfile:
        # output with no dataset_args should return all objects from all datasets
        metadata_file_patch.side_effect = lambda *_, **__: outfile.name
        dataset_guids = None
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            output_discovery_objects(
                gen3_auth,
                dataset_guids=dataset_guids,
                endpoint="excommons.org",
                guid_type=guid_type,
            )
        )
        tsv_reader2 = csv.DictReader(outfile, **BASE_CSV_PARSER_SETTINGS)
        tsv_rows2 = list(tsv_reader2)
        assert tsv_rows2 == EXPECTED_DISCOVERY_OBJECTS


@patch("gen3.tools.metadata.discovery_objects._create_discovery_objects_filename")
@patch("gen3.metadata.Gen3Metadata.query")
def test_discovery_objects_read_template(
    metadata_query_patch, metadata_file_patch, gen3_auth, guid_type="discovery_metadata"
):
    """
    Test that when reading discovery objects with the template option, the resulting output
    has just the template columns and no object rows.
    """
    guid1_discovery_metadata = MOCK_METADATA_1
    guid2_discovery_metadata = MOCK_METADATA_2
    metadata_query_patch.side_effect = MOCK_METADATA_SIDE_EFFECT

    with tempfile.NamedTemporaryFile(suffix=".tsv", mode="a+") as outfile:
        # output when reading with --template flag,should just be required columns
        metadata_file_patch.side_effect = lambda *_, **__: outfile.name
        dataset_guids = None
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            output_discovery_objects(
                gen3_auth,
                dataset_guids=dataset_guids,
                endpoint="excommons.org",
                guid_type=guid_type,
                template=True,
            )
        )
        tsv_reader3 = csv.DictReader(outfile, **BASE_CSV_PARSER_SETTINGS)
        assert set(tsv_reader3.fieldnames) == REQUIRED_OBJECT_FIELDS.union(
            OPTIONAL_OBJECT_FIELDS
        )
        tsv_rows3 = list(tsv_reader3)
        assert len(tsv_rows3) == 0


@patch("gen3.tools.metadata.discovery_objects._create_discovery_objects_filename")
@patch("gen3.metadata.Gen3Metadata.query")
def test_discovery_objects_read_empty_dataset(
    metadata_query_patch, metadata_file_patch, gen3_auth, guid_type="discovery_metadata"
):
    """
    Test that when reading discovery objects from a dataset with no objects, the resulting output
    has just the required columns and no object rows.
    """
    guid1_discovery_metadata = MOCK_METADATA_1
    guid2_discovery_metadata = MOCK_METADATA_2
    metadata_query_patch.side_effect = MOCK_METADATA_SIDE_EFFECT

    with tempfile.NamedTemporaryFile(suffix=".tsv", mode="a+") as outfile:
        # output when reading with --template flag, or when reading a dataset with no objects, should just be required columns
        metadata_file_patch.side_effect = lambda *_, **__: outfile.name
        dataset_guids = ["guid2"]
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            output_discovery_objects(
                gen3_auth,
                dataset_guids=dataset_guids,
                endpoint="excommons.org",
                guid_type=guid_type,
            )
        )
        tsv_reader4 = csv.DictReader(outfile, **BASE_CSV_PARSER_SETTINGS)
        assert set(tsv_reader4.fieldnames) == REQUIRED_OBJECT_FIELDS
        tsv_rows4 = list(tsv_reader4)
        assert len(tsv_rows4) == 0


@patch("gen3.tools.metadata.discovery_objects._create_discovery_objects_filename")
@patch("gen3.metadata.Gen3Metadata.query")
def test_discovery_objects_read_JSON(
    metadata_query_patch, metadata_file_patch, gen3_auth, guid_type="discovery_metadata"
):
    """
    Test that when reading discovery objects to a JSON file, the resulting output
    matches the expected output format.
    """
    # JSON output tests
    metadata_query_patch.side_effect = MOCK_METADATA_SIDE_EFFECT
    with tempfile.NamedTemporaryFile(suffix=".json", mode="a+") as outfile:
        expected_output = [
            {
                "dataset_guid": "guid1",
                "guid": "drs://dg.FOOBAR:082a288e-3da2-4806-9438-bc974cdb1cd7",
                "display_name": "Clinical TSV",
                "description": "TSV containing info about subjects.",
                "type": "null",
            },
            {
                "dataset_guid": "guid1",
                "guid": "drs://dg.FOOBAR:f060149a-8a35-421e-802a-873612ee4874",
                "display_name": "Summary PDF",
            },
        ]
        metadata_file_patch.side_effect = lambda *_, **__: outfile.name
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            output_discovery_objects(
                gen3_auth,
                endpoint="excommons.org",
                guid_type=guid_type,
                output_format="json",
            )
        )
        outfile.seek(0)
        assert json.load(outfile) == expected_output
