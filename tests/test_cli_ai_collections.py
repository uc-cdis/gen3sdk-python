"""
Tests for the embeddings collections CRUD commands for:
`gen3 ai embeddings collections`.

Note: These are pretty minimal, they basically just make sure the
      Embeddings client gets passed the right info from the command line.
"""

from unittest.mock import AsyncMock, patch

from gen3.cli.ai.main import ai


@patch("gen3.cli.ai.main.EmbeddingsClient")
def test_create_collection_success(patched_embeddings_client, runner, mock_ctx_obj):
    """Test successful creation of an embeddings collection."""
    mocked_client = AsyncMock()
    mocked_client.create_collection.return_value = {
        "name": "ctds-github-md",
        "dimensions": 384,
    }
    patched_embeddings_client.return_value = mocked_client

    result = runner.invoke(
        ai,
        [
            "embeddings",
            "collections",
            "create",
            "ctds-github-md",
            "--dimensions",
            "384",
            "--description",
            "All markdown from CTDS Github",
        ],
        obj=mock_ctx_obj,
    )
    assert result.exit_code == 0
    mocked_client.create_collection.assert_called_once_with(
        "ctds-github-md", 384, "All markdown from CTDS Github"
    )


@patch("gen3.cli.ai.main.EmbeddingsClient")
def test_read_collections_all_success(patched_embeddings_client, runner, mock_ctx_obj):
    """Test reading all collections."""
    mocked_client = AsyncMock()
    mocked_client.list_collections.return_value = {
        "collections": [
            {"name": "ctds-github-md", "dimensions": 384},
            {"name": "other", "dimensions": 256},
        ]
    }
    patched_embeddings_client.return_value = mocked_client

    result = runner.invoke(ai, ["embeddings", "collections", "read"], obj=mock_ctx_obj)
    assert result.exit_code == 0
    mocked_client.list_collections.assert_called_once_with()
    assert "ctds-github-md" in result.output
    assert "other" in result.output
    assert "384" in result.output
    assert "256" in result.output


@patch("gen3.cli.ai.main.EmbeddingsClient")
def test_read_collection_specific_success(
    patched_embeddings_client, runner, mock_ctx_obj
):
    """Test reading a specific collection."""
    mocked_client = AsyncMock()
    mocked_client.list_collections.return_value = {
        "name": "ctds-github-md",
        "dimensions": 384,
    }
    patched_embeddings_client.return_value = mocked_client

    result = runner.invoke(
        ai,
        ["embeddings", "collections", "read", "ctds-github-md"],
        obj=mock_ctx_obj,
    )
    assert result.exit_code == 0
    mocked_client.list_collections.assert_called_once_with(
        collection_name="ctds-github-md"
    )
    assert "ctds-github-md" in result.output
    assert "384" in result.output


@patch("gen3.cli.ai.main.EmbeddingsClient")
def test_delete_collection_success(patched_embeddings_client, runner, mock_ctx_obj):
    """Test successful deletion of a collection."""
    mocked_client = AsyncMock()
    patched_embeddings_client.return_value = mocked_client

    result = runner.invoke(
        ai,
        ["embeddings", "collections", "delete", "ctds-github-md"],
        obj=mock_ctx_obj,
    )
    assert result.exit_code == 0
    mocked_client.delete_collection.assert_called_once_with("ctds-github-md")
