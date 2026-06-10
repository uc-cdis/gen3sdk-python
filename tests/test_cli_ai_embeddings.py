"""
Tests for the embeddings commands for:
`gen3 ai embeddings ...`.
"""

import csv
import json
from unittest.mock import AsyncMock, MagicMock, patch

from gen3.cli.ai.main import ai


@patch("gen3.cli.ai.embeddings._get_collection_id_and_name")
@patch("gen3.cli.ai.main.EmbeddingsClient")
def test_publish_embeddings_success(
    patched_embeddings_client, patched_get_collection_id, runner, tmp_path, mock_ctx_obj
):
    """
    Tests successful publication of embeddings from a manifest file.

    Note: tmp_path is a pytest built-in fixture
    """
    mocked_embeddings_client = AsyncMock()
    patched_embeddings_client.return_value = mocked_embeddings_client
    patched_get_collection_id.return_value = "test_collection_id", "test_collection"

    manifest_file = tmp_path / "embeddings.tsv"
    mock_json_metadata = json.dumps({"key": "value"})
    with open(manifest_file, "w") as file:
        writer = csv.writer(file, delimiter="\t")
        writer.writerow(
            ["embedding", "collection_name", "other_data", "mock_json_metadata"]
        )
        writer.writerow(
            [
                json.dumps([0.1, 0.2, 0.3]),
                "test_collection",
                "foobar",
                mock_json_metadata,
            ]
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

    mocked_embeddings_client.create_embeddings.assert_called()
    args, kwargs = mocked_embeddings_client.create_embeddings.call_args
    assert (
        kwargs["embeddings_with_metadata"][0]["metadata"].get("other_data") == "foobar"
    )
    assert (
        kwargs["embeddings_with_metadata"][0]["metadata"].get("mock_json_metadata")
        == mock_json_metadata
    )
    assert kwargs["collection_name"] == "test_collection"
    assert kwargs["collection_id"] == "test_collection_id"


@patch("gen3.cli.ai.embeddings._get_collection_id_and_name")
@patch("gen3.cli.ai.main.EmbeddingsClient")
def test_publish_embeddings_invalid_json(
    patched_embeddings_client, patched_get_collection_id, runner, tmp_path, mock_ctx_obj
):
    """
    Tests handling of invalid JSON in the embedding column of the manifest.

    Note: tmp_path is a pytest built-in fixture
    """
    # patched_embeddings_client.return_value = mock_ctx_obj["client"]
    patched_get_collection_id.return_value = "test_collection_id", "test_collection"

    manifest_file = tmp_path / "embeddings.tsv"
    with open(manifest_file, "w") as file:
        writer = csv.writer(file, delimiter="\t")
        writer.writerow(["embedding", "collection_name", "guid"])
        writer.writerow(["not-a-json", "test_collection", "test_guid"])

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

    patched_embeddings_client.create_embeddings.assert_not_called()


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
