import asyncio
import csv
import json
import tempfile
from unittest.mock import patch
import pytest

from gen3.tools.metadata.discovery import (
    output_expanded_discovery_metadata,
    combine_discovery_metadata,
    publish_discovery_metadata,
    BASE_CSV_PARSER_SETTINGS,
    _try_parse,
)


@patch("gen3.tools.metadata.discovery._create_metadata_output_filename")
@patch("gen3.metadata.Gen3Metadata.query")
def test_discovery_read(
    metadata_query_patch, metadata_file_patch, gen3_auth, guid_type="discovery_metadata"
):
    guid1_discovery_metadata = {
        # this value should be written to the file exactly as shown
        "str_key": "str_val \n \t \\",
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
            "_guid_type": guid_type,
            "gen3_discovery": guid1_discovery_metadata,
        },
        "guid2": {
            "_guid_type": guid_type,
            "gen3_discovery": guid2_discovery_metadata,
        },
    }

    # TSV output tests
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="a+") as outfile:
        metadata_file_patch.side_effect = lambda *_, **__: outfile.name
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            output_expanded_discovery_metadata(
                gen3_auth, endpoint="excommons.org", guid_type=guid_type
            )
        )
        outfile.seek(0)
        csv_rows = list(csv.DictReader(outfile, **BASE_CSV_PARSER_SETTINGS))
        assert len(csv_rows) == 2

        guid1_row, guid2_row = csv_rows

        guid1_keys = set(guid1_discovery_metadata.keys())
        guid2_keys = set(guid2_discovery_metadata.keys())
        metadata_keys = guid1_keys | guid2_keys
        metadata_columns = set(guid1_row.keys())

        # output should have all columns from both discovery metadata, with tags expanded and a "guid" column
        assert metadata_keys - metadata_columns == set(["tags"])
        assert metadata_columns - metadata_keys == set(["_tag_0", "_tag_1", "guid"])

        # output should jsonify all dicts/lists, leave everything else the same
        assert guid1_row["listval"] == json.dumps(guid1_discovery_metadata["listval"])
        assert guid1_row["dictval"] == json.dumps(guid1_discovery_metadata["dictval"])
        assert guid2_row["other_key"] == guid2_discovery_metadata["other_key"]

        # output will double-encode newlines, but this should return to original data after
        assert _try_parse(guid1_row["str_key"]) == guid1_discovery_metadata["str_key"]

        # all keys not present in a metadata object should get null values
        assert all(guid1_row[key] == "" for key in list(guid2_keys - guid1_keys))
        assert all(guid2_row[key] == "" for key in list(guid1_keys - guid2_keys))

        # should also be able to handle empty discovery metadata
        metadata_query_patch.side_effect = lambda *_, **__: {
            "guid1": {"_guid_type": guid_type, "gen3_discovery": {}}
        }

        outfile.truncate(0)
        loop.run_until_complete(
            output_expanded_discovery_metadata(gen3_auth, endpoint="excommons.org")
        )
        outfile.seek(0)
        assert outfile.read() == "guid\nguid1\n"

        # test discovering data from aggregate MDS
        metadata_query_patch.side_effect = lambda *_, **__: {
            "commons1": [
                {
                    "guid1": {
                        "_guid_type": guid_type,
                        "gen3_discovery": guid1_discovery_metadata,
                    }
                }
            ],
            "commons2": [
                {
                    "guid2": {
                        "_guid_type": guid_type,
                        "gen3_discovery": guid2_discovery_metadata,
                    }
                }
            ],
        }

        outfile.truncate(0)
        loop.run_until_complete(
            output_expanded_discovery_metadata(
                gen3_auth, endpoint="excommons.org", use_agg_mds=True
            )
        )
        outfile.seek(0)
        agg_csv_rows = list(csv.DictReader(outfile, **BASE_CSV_PARSER_SETTINGS))

        # agg mds should parse identical to single-commons mds
        assert agg_csv_rows == csv_rows

    # JSON output tests
    metadata_query_patch.side_effect = lambda *_, **__: {
        "guid1": {
            "_guid_type": guid_type,
            "gen3_discovery": guid1_discovery_metadata,
        },
        "guid2": {
            "_guid_type": guid_type,
            "gen3_discovery": guid2_discovery_metadata,
        },
    }
    with tempfile.NamedTemporaryFile(suffix=".json", mode="a+") as outfile:
        expected_output = [
            {
                "guid": "guid1",
                "_guid_type": guid_type,
                "gen3_discovery": guid1_discovery_metadata,
            },
            {
                "guid": "guid2",
                "_guid_type": guid_type,
                "gen3_discovery": guid2_discovery_metadata,
            },
        ]
        metadata_file_patch.side_effect = lambda *_, **__: outfile.name
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            output_expanded_discovery_metadata(
                gen3_auth,
                endpoint="excommons.org",
                guid_type=guid_type,
                output_format="json",
            )
        )
        outfile.seek(0)
        assert json.load(outfile) == expected_output

        # test discovering data from aggregate MDS
        metadata_query_patch.side_effect = lambda *_, **__: {
            "commons1": [
                {
                    "guid1": {
                        "_guid_type": guid_type,
                        "gen3_discovery": guid1_discovery_metadata,
                    }
                }
            ],
            "commons2": [
                {
                    "guid2": {
                        "_guid_type": guid_type,
                        "gen3_discovery": guid2_discovery_metadata,
                    }
                }
            ],
        }

        outfile.truncate(0)
        loop.run_until_complete(
            output_expanded_discovery_metadata(
                gen3_auth,
                endpoint="excommons.org",
                use_agg_mds=True,
                output_format="json",
            )
        )
        outfile.seek(0)

        # agg mds should parse identical to single-commons mds
        assert json.load(outfile) == expected_output

        # illegal output_format value
        with pytest.raises(ValueError):
            loop = asyncio.new_event_loop()
            loop.run_until_complete(
                output_expanded_discovery_metadata(
                    gen3_auth,
                    endpoint="excommons.org",
                    guid_type=guid_type,
                    output_format="blah",
                )
            )


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


def test_discovery_combine():
    """
    Test the underlying logic for combining metadata manifests.
    """
    current_discovery_metadata_file = (
        "tests/merge_manifests/discovery_combine/discovery.tsv"
    )
    metadata_filename = "tests/merge_manifests/discovery_combine/metadata_file.tsv"
    discovery_column_to_map_on = "guid"
    metadata_column_to_map = "Id"
    output_filename = "test_combined_discovery_metadata.tsv"
    metadata_prefix = "DBGAP_FHIR_"

    output_file = combine_discovery_metadata(
        current_discovery_metadata_file,
        metadata_filename,
        discovery_column_to_map_on,
        metadata_column_to_map,
        output_filename,
        metadata_prefix=metadata_prefix,
    )

    assert _get_tsv_data(output_file) == _get_tsv_data(
        "tests/merge_manifests/discovery_combine/combined_discovery_metadata.tsv"
    )

    _remove_temporary_file(output_file)


def test_discovery_combine_exact_match():
    """
    Test the underlying logic for combining metadata manifests when there's a column
    with an exact match.
    """
    current_discovery_metadata_file = (
        "tests/merge_manifests/discovery_combine/discovery.tsv"
    )
    metadata_filename = (
        "tests/merge_manifests/discovery_combine/metadata_file_exact_match.tsv"
    )
    discovery_column_to_map_on = "guid"
    metadata_column_to_map = "guid_exact_match"
    output_filename = "test_combined_discovery_metadata_exact_match.tsv"
    metadata_prefix = "DBGAP_FHIR_"

    output_file = combine_discovery_metadata(
        current_discovery_metadata_file,
        metadata_filename,
        discovery_column_to_map_on,
        metadata_column_to_map,
        output_filename,
        metadata_prefix=metadata_prefix,
        exact_match=True,
    )

    assert _get_tsv_data(output_file) == _get_tsv_data(
        "tests/merge_manifests/discovery_combine/combined_discovery_metadata_exact_match.tsv"
    )

    _remove_temporary_file(output_file)


def _remove_temporary_file(filename):
    try:
        os.remove(filename)
    except Exception as exc:
        pass


def _get_tsv_data(manifest, delimiter="\t"):
    """
    Returns a list of rows sorted by guid for the given manifest.
    """
    csv_data = list()
    with open(manifest, "r", encoding="utf-8-sig") as f:
        rows = []
        reader = csv.DictReader(f, delimiter=delimiter)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append({key: value for key, value in row.items()})

    return sorted(rows, key=lambda row: row.get("guid", ""))
