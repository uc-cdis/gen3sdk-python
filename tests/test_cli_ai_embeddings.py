"""
Tests for the embeddings commands for:
`gen3 ai embeddings ...`.
"""

import csv
import json
from unittest.mock import AsyncMock, MagicMock, patch

from gen3.cli.ai.main import ai


def _created_embeddings_response(embeddings_with_metadata, authz):
    """
    Build a create embeddings API response echoing back the request.
    """
    return {
        "embeddings": [
            {
                "embedding_id": f"id-{index}",
                "vector": item["embedding"],
                "info": {
                    "collection_id": 1,
                    "authz": authz or "/vectorstore/collections/test_collection",
                    "metadata": item["metadata"],
                    "self": f"/vectorstore/collections/test_collection/embeddings/id-{index}",
                },
            }
            for index, item in enumerate(embeddings_with_metadata)
        ]
    }


def _write_manifest(path, header, rows):
    with open(path, "w") as file:
        writer = csv.writer(file, delimiter="\t")
        writer.writerow(header)
        writer.writerows(rows)


@patch("gen3.cli.ai.embeddings._get_collection_id_and_name")
@patch("gen3.cli.ai.utils.EmbeddingsClient")
def test_publish_embeddings_success(
    patched_embeddings_client, patched_get_collection_id, runner, tmp_path, mock_ctx_obj
):
    """
    Tests successful publication of embeddings from a manifest file.

    Note: tmp_path is a pytest built-in fixture
    """
    mocked_embeddings_client = AsyncMock()
    mocked_embeddings_client.create_embeddings.side_effect = (
        lambda **kwargs: _created_embeddings_response(
            kwargs["embeddings_with_metadata"], kwargs["authz"]
        )
    )
    patched_embeddings_client.return_value = mocked_embeddings_client
    patched_get_collection_id.return_value = 1, "test_collection"

    manifest_file = tmp_path / "embeddings.tsv"
    mock_json_metadata = json.dumps({"key": "value"})
    _write_manifest(
        manifest_file,
        ["embedding", "collection_name", "other_data", "mock_json_metadata"],
        [
            [
                json.dumps([0.1, 0.2, 0.3]),
                "test_collection",
                "foobar",
                mock_json_metadata,
            ]
        ],
    )

    result = runner.invoke(
        ai,
        [
            "embeddings",
            "publish",
            str(manifest_file),
            "--default-collection",
            "test_collection",
        ],
        obj=mock_ctx_obj,
    )
    assert result.exit_code == 0, result.output

    mocked_embeddings_client.create_embeddings.assert_called_once()
    _, kwargs = mocked_embeddings_client.create_embeddings.call_args
    assert (
        kwargs["embeddings_with_metadata"][0]["metadata"].get("other_data") == "foobar"
    )
    assert (
        kwargs["embeddings_with_metadata"][0]["metadata"].get("mock_json_metadata")
        == mock_json_metadata
    )
    assert kwargs["collection_name"] == "test_collection"
    assert kwargs["authz"] is None

    with open(tmp_path / "embeddings_output.tsv") as file:
        rows = list(csv.DictReader(file, delimiter="\t"))
    assert len(rows) == 1
    assert rows[0]["embedding_id"] == "id-0"
    assert rows[0]["other_data"] == "foobar"


@patch("gen3.cli.ai.embeddings._get_collection_id_and_name")
@patch("gen3.cli.ai.utils.EmbeddingsClient")
def test_publish_embeddings_batches_by_authz(
    patched_embeddings_client, patched_get_collection_id, runner, tmp_path, mock_ctx_obj
):
    """
    Tests that the authz column is sent per request, with one request per authz,
    since the API ignores authz set on individual embeddings.
    """
    mocked_embeddings_client = AsyncMock()
    mocked_embeddings_client.create_embeddings.side_effect = (
        lambda **kwargs: _created_embeddings_response(
            kwargs["embeddings_with_metadata"], kwargs["authz"]
        )
    )
    patched_embeddings_client.return_value = mocked_embeddings_client
    patched_get_collection_id.return_value = 1, "test_collection"

    manifest_file = tmp_path / "embeddings.tsv"
    _write_manifest(
        manifest_file,
        ["embedding", "collection_name", "authz", "file"],
        [
            [json.dumps([0.1, 0.2]), "test_collection", "/programs/a", "a1"],
            [json.dumps([0.3, 0.4]), "test_collection", "/programs/b", "b1"],
            [json.dumps([0.5, 0.6]), "test_collection", "/programs/a", "a2"],
            [json.dumps([0.7, 0.8]), "test_collection", "", "default"],
        ],
    )

    result = runner.invoke(
        ai, ["embeddings", "publish", str(manifest_file)], obj=mock_ctx_obj
    )
    assert result.exit_code == 0, result.output

    requests_by_authz = {
        call.kwargs["authz"]: call.kwargs["embeddings_with_metadata"]
        for call in mocked_embeddings_client.create_embeddings.call_args_list
    }
    assert set(requests_by_authz) == {"/programs/a", "/programs/b", None}
    assert [item["metadata"]["file"] for item in requests_by_authz["/programs/a"]] == [
        "a1",
        "a2",
    ]
    for embeddings in requests_by_authz.values():
        assert all("authz" not in item for item in embeddings)

    with open(tmp_path / "embeddings_output.tsv") as file:
        rows = list(csv.DictReader(file, delimiter="\t"))
    assert {row["file"]: row["authz"] for row in rows} == {
        "a1": "/programs/a",
        "a2": "/programs/a",
        "b1": "/programs/b",
        "default": "/vectorstore/collections/test_collection",
    }


@patch("gen3.cli.ai.utils.EmbeddingsClient")
def test_publish_embeddings_collection_id_without_name(
    patched_embeddings_client, runner, tmp_path, mock_ctx_obj
):
    """
    Tests that a row with only a collection_id fails clearly instead of posting
    to a collection with an empty name.
    """
    mocked_embeddings_client = AsyncMock()
    patched_embeddings_client.return_value = mocked_embeddings_client

    manifest_file = tmp_path / "embeddings.tsv"
    _write_manifest(
        manifest_file,
        ["embedding", "collection_id"],
        [[json.dumps([0.1, 0.2]), "1"]],
    )

    result = runner.invoke(
        ai, ["embeddings", "publish", str(manifest_file)], obj=mock_ctx_obj
    )
    assert result.exit_code == 1
    assert "no collection_name" in result.output
    mocked_embeddings_client.create_embeddings.assert_not_called()


@patch("gen3.cli.ai.embeddings._get_collection_id_and_name")
@patch("gen3.cli.ai.utils.EmbeddingsClient")
def test_publish_embeddings_collection_id_mismatch(
    patched_embeddings_client, patched_get_collection_id, runner, tmp_path, mock_ctx_obj
):
    """
    Tests that a row whose collection_id doesn't match the named collection fails.
    """
    mocked_embeddings_client = AsyncMock()
    patched_embeddings_client.return_value = mocked_embeddings_client
    patched_get_collection_id.return_value = 1, "test_collection"

    manifest_file = tmp_path / "embeddings.tsv"
    _write_manifest(
        manifest_file,
        ["embedding", "collection_name", "collection_id"],
        [[json.dumps([0.1, 0.2]), "test_collection", "2"]],
    )

    result = runner.invoke(
        ai, ["embeddings", "publish", str(manifest_file)], obj=mock_ctx_obj
    )
    assert result.exit_code == 1
    assert "collection_id '2'" in result.output
    mocked_embeddings_client.create_embeddings.assert_not_called()


@patch("gen3.cli.ai.embeddings._get_collection_id_and_name")
@patch("gen3.cli.ai.utils.EmbeddingsClient")
def test_publish_embeddings_invalid_json(
    patched_embeddings_client, patched_get_collection_id, runner, tmp_path, mock_ctx_obj
):
    """
    Tests handling of invalid JSON in the embedding column of the manifest.

    Note: tmp_path is a pytest built-in fixture
    """
    mocked_embeddings_client = AsyncMock()
    patched_embeddings_client.return_value = mocked_embeddings_client
    patched_get_collection_id.return_value = 1, "test_collection"

    manifest_file = tmp_path / "embeddings.tsv"
    _write_manifest(
        manifest_file,
        ["embedding", "collection_name", "guid"],
        [["not-a-json", "test_collection", "test_guid"]],
    )

    result = runner.invoke(
        ai,
        [
            "embeddings",
            "publish",
            str(manifest_file),
            "--default-collection",
            "test_collection",
        ],
        obj=mock_ctx_obj,
    )
    assert result.exit_code == 0

    mocked_embeddings_client.create_embeddings.assert_not_called()


@patch("gen3.cli.ai.embeddings.Gen3Index")
@patch("gen3.cli.ai.utils.EmbeddingsClient")
def test_convert_embeddings_without_url_prefix(
    patched_embeddings_client, patched_gen3_index, runner, tmp_path, mock_ctx_obj
):
    """
    Tests that convert works without --url-prefix and passes `self` through as the url.
    """
    patched_gen3_index.return_value.get_valid_guids.return_value = ["guid-1"]

    manifest_file = tmp_path / "embeddings_output.tsv"
    self_url = "https://example.com/ai/vectorstore/collections/c/embeddings/id-0"
    _write_manifest(
        manifest_file,
        ["embedding_id", "embedding", "collection_id", "authz", "self"],
        [["id-0", json.dumps([0.1, 0.2]), "1", "/programs/a", self_url]],
    )

    result = runner.invoke(
        ai, ["embeddings", "convert", str(manifest_file)], obj=mock_ctx_obj
    )
    assert result.exit_code == 0, result.output

    with open(tmp_path / "embeddings_output_converted.tsv") as file:
        rows = list(csv.DictReader(file, delimiter="\t"))
    assert len(rows) == 1
    assert rows[0]["guid"] == "guid-1"
    assert rows[0]["url"] == self_url
    assert rows[0]["authz"] == "/programs/a"


@patch("gen3.cli.ai.embeddings.Gen3Index")
@patch("gen3.cli.ai.utils.EmbeddingsClient")
def test_convert_embeddings_with_url_prefix(
    patched_embeddings_client, patched_gen3_index, runner, tmp_path, mock_ctx_obj
):
    """
    Tests that --url-prefix is added to urls that don't already have it.
    """
    patched_gen3_index.return_value.get_valid_guids.return_value = ["guid-1", "guid-2"]

    manifest_file = tmp_path / "embeddings_output.tsv"
    prefix = "https://example.com/ai"
    _write_manifest(
        manifest_file,
        ["embedding", "authz", "self"],
        [
            [json.dumps([0.1]), "/programs/a", "/vectorstore/a"],
            [json.dumps([0.2]), "/programs/a", f"{prefix}/vectorstore/b"],
        ],
    )

    result = runner.invoke(
        ai,
        ["embeddings", "convert", str(manifest_file), "--url-prefix", prefix],
        obj=mock_ctx_obj,
    )
    assert result.exit_code == 0, result.output

    with open(tmp_path / "embeddings_output_converted.tsv") as file:
        rows = list(csv.DictReader(file, delimiter="\t"))
    assert [row["url"] for row in rows] == [
        f"{prefix}/vectorstore/a",
        f"{prefix}/vectorstore/b",
    ]


@patch("gen3.cli.ai.embeddings.LocalEmbeddingClient")
def test_chunk_and_embed_text_success(
    patched_local_emb_client, runner, tmp_path, mock_ctx_obj
):
    """
    Tests successful chunking and embedding of text files.

    Note: tmp_path is a pytest built-in fixture
    """
    test_file = tmp_path / "test.txt"
    test_file.write_text("This is a test content for embedding.")

    mock_local_client = MagicMock()
    mock_local_client.embed.return_value = [
        {
            "embedding": [0.1, 0.2],
            "metadata": {"file": str(test_file)},
            "authz": "/foo/bar",
        }
    ]
    patched_local_emb_client.return_value = mock_local_client

    out_manifest = tmp_path / "output.tsv"

    result = runner.invoke(
        ai,
        [
            "embeddings",
            "embed-files",
            str(test_file),
            "--collection-name",
            "test_collection",
            "--out-manifest-file",
            str(out_manifest),
            "--strategy",
            "text",
        ],
        obj=mock_ctx_obj,
    )
    assert result.exit_code == 0

    with open(out_manifest, "r") as file:
        reader = csv.DictReader(file, delimiter="\t")
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["collection_name"] == "test_collection"
        assert rows[0]["authz"] == "/foo/bar"
        assert rows[0]["file"] == str(test_file)
        assert json.loads(rows[0]["embedding"]) == [0.1, 0.2]
