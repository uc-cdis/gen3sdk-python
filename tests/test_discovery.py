import asyncio
import csv
import json
import tempfile
from unittest.mock import patch
from csv import DictReader

import pandas as pd
import pytest

from gen3.tools.metadata.discovery import (
    output_expanded_discovery_metadata,
    publish_discovery_metadata,
)


@patch("gen3.tools.metadata.discovery._metadata_file_from_auth")
@patch("gen3.metadata.Gen3Metadata.query")
def test_discovery_read(metadata_query_patch, metadata_file_patch, gen3_auth):
    guid1_discovery_metadata = {
        "str_key": "str_val",
        # tags should flatten
        "tags": [
            {"name": "first tag", "category": "category 1"},
            {"name": "second tag", "category": "category 2"},
        ],
        # other list/object vals should not flatten
        "listval": ["v1", "v2"],
        "dictval": {"k1": "v1", "k2": "v2"},
    }
    guid2_discovery_metadata = {"other_key": "other_val", "tags": []}
    metadata_query_patch.side_effect = lambda *_, **__: {
        "guid1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": guid1_discovery_metadata,
        },
        "guid2": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": guid2_discovery_metadata,
        },
    }

    with tempfile.NamedTemporaryFile(suffix=".csv", mode="a+") as outfile:
        metadata_file_patch.side_effect = lambda *_, **__: outfile.name
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            output_expanded_discovery_metadata(gen3_auth, endpoint="excommons.org")
        )
        outfile.seek(0)
        csv_rows = list(csv.DictReader(outfile, delimiter="\t"))
        assert len(csv_rows) == 2

        guid1_row, guid2_row = csv_rows

        guid1_keys = set(guid1_discovery_metadata.keys())
        guid2_keys = set(guid2_discovery_metadata.keys())
        metadata_keys = guid1_keys | guid2_keys
        metadata_columns = set(guid1_row.keys())

        # output should have all columns from both discovery metadata, with tags expanded and a "guid" column
        assert metadata_keys - metadata_columns == set(["tags"])
        assert metadata_columns - metadata_keys == set(["_tag_0", "_tag_1", "guid"])

        # output should jsonify all dicts/lists, leave everthing else the same
        assert guid1_row["str_key"] == guid1_discovery_metadata["str_key"]
        assert guid1_row["listval"] == json.dumps(guid1_discovery_metadata["listval"])
        assert guid1_row["dictval"] == json.dumps(guid1_discovery_metadata["dictval"])
        assert guid2_row["other_key"] == guid2_discovery_metadata["other_key"]

        # all keys not present in a metadata object should get null values
        assert all(guid1_row[key] == "" for key in list(guid2_keys - guid1_keys))
        assert all(guid2_row[key] == "" for key in list(guid1_keys - guid2_keys))

        # should also be able to handle empty discovery metadata
        metadata_query_patch.side_effect = lambda *_, **__: {
            "guid1": {"_guid_type": "discovery_metadata", "gen3_discovery": {}}
        }

        outfile.truncate(0)
        loop.run_until_complete(
            output_expanded_discovery_metadata(gen3_auth, endpoint="excommons.org")
        )
        outfile.seek(0)
        assert outfile.read() == "guid\nguid1\n"


@patch("gen3.metadata.Gen3Metadata.async_create")
@pytest.mark.parametrize("ignore_empty_columns", [True, False])
def test_discovery_publish_omit_empty_columns(
    create_metadata_patch, ignore_empty_columns, gen3_auth
):
    async def mock_async_create_metadata(guid, metadata, *_, **__):
        assert metadata["_guid_type"] == "discovery_metadata"
        discovery_metadata = metadata["gen3_discovery"]

        base_expected_schema = (
            {}
            if ignore_empty_columns
            else {
                header: ""
                for header in headers
                if header not in ["guid", "_tag_0", "_tag_1"]
            }
        )

        if guid == "guid1":
            assert discovery_metadata == {
                **base_expected_schema,
                "c1": "x",
                "exlist": ["v1", "v2"],
                "exdict": {"k": "v"},
                "tags": [{"name": "name", "category": "category"}] * 2,
            }
        else:
            assert discovery_metadata == {
                **base_expected_schema,
                "c2": "y",
                "tags": [{"name": "name", "category": "category"}],
            }

    create_metadata_patch.side_effect = mock_async_create_metadata
    headers = ["guid", "c1", "c2", "exlist", "exdict", "_tag_0", "_tag_1"]
    ex_tag = "category: name"

    with tempfile.NamedTemporaryFile(suffix=".tsv") as mocked_manifest:
        mocked_csv_lines = "\n".join(
            [
                "\t".join(row)
                for row in [
                    headers,
                    [
                        "guid1",
                        "x",
                        "",
                        json.dumps(["v1", "v2"]),
                        json.dumps({"k": "v"}),
                        ex_tag,
                        ex_tag,
                    ],
                    ["guid2", "", "y", "", "", ex_tag, ""],
                ]
            ]
        )
        mocked_manifest.write(mocked_csv_lines.encode())
        mocked_manifest.seek(0)

        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            publish_discovery_metadata(
                gen3_auth,
                mocked_manifest.name,
                endpoint="excommons.org",
                omit_empty_values=ignore_empty_columns,
            )
        )
