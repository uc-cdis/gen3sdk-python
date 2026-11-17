"""
Tests for retrieving bulk content with `gen3 objects read`.
"""

import csv
import json
from unittest.mock import AsyncMock, patch

import numpy

from gen3.cli.content import get_bulk_content
from gen3.file import EmbeddingContent


@patch("gen3.cli.content.Gen3File")
def test_get_bulk_content_writes_publish_manifest_format(
    patched_gen3_file, runner, tmp_path, mock_ctx_obj
):
    """
    Tests that bulk content is written as full JSON vectors with ordered columns
    and metadata flattened into columns, matching the publish output manifest.
    """
    # over 1000 elements, where numpy's repr would truncate with "..."
    vector = numpy.arange(1500, dtype=numpy.float16) / 1024
    patched_gen3_file.return_value.get_bulk_content = AsyncMock(
        return_value={
            "guid-1": EmbeddingContent(
                guid="guid-1",
                embedding_id="id-1",
                embedding=vector,
                authz="/programs/a",
                collection_id=1,
                self="/vectorstore/collections/c/embeddings/id-1",
                metadata={"file": "a.md", "tags": ["x", "y"]},
            ),
            "guid-2": EmbeddingContent(
                guid="guid-2",
                embedding_id="",
                embedding=numpy.array([]),
                authz=[],
                collection_id=None,
                self="",
                metadata={},
            ),
        }
    )

    output_file = tmp_path / "content_output.tsv"
    result = runner.invoke(
        get_bulk_content,
        ["--guids", "guid-1", "--guids", "guid-2", "--output-file", str(output_file)],
        obj=mock_ctx_obj,
    )
    assert result.exit_code == 0, result.output

    with open(output_file) as file:
        reader = csv.DictReader(file, delimiter="\t")
        rows = list(reader)

    assert reader.fieldnames == [
        "guid",
        "embedding_id",
        "embedding",
        "collection_id",
        "authz",
        "self",
        "file",
        "tags",
    ]
    assert json.loads(rows[0]["embedding"]) == vector.tolist()
    assert rows[0]["authz"] == "/programs/a"
    assert rows[0]["collection_id"] == "1"
    assert rows[0]["file"] == "a.md"
    assert json.loads(rows[0]["tags"]) == ["x", "y"]

    assert rows[1]["embedding"] == "[]"
    assert rows[1]["authz"] == ""
    assert rows[1]["collection_id"] == ""
