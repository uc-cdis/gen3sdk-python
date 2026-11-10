"""
Tests for the Gen3 AI clients in `gen3.ai`.
"""

import asyncio
import json
from unittest.mock import MagicMock, patch

import httpx
import numpy
import pytest

from gen3.ai import EmbeddingsClient, LocalEmbeddingClient

REAL_ASYNC_CLIENT = httpx.AsyncClient


@pytest.fixture
def captured_requests():
    """
    Patch httpx.AsyncClient in gen3.ai so requests go to a mock transport, and
    return the list the requests are captured in.
    """
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"embeddings": []})

    with patch(
        "gen3.ai.httpx.AsyncClient",
        lambda: REAL_ASYNC_CLIENT(transport=httpx.MockTransport(handler)),
    ):
        yield requests


@pytest.fixture
def embeddings_client():
    auth = MagicMock()
    auth.get_access_token.return_value = "token"
    return EmbeddingsClient(auth=auth, endpoint="http://localhost:4142", api_prefix="")


def test_create_embeddings_sends_authz_in_body(embeddings_client, captured_requests):
    """
    Tests that authz is sent once for the whole request, where the API reads it.
    """
    asyncio.run(
        embeddings_client.create_embeddings(
            collection_name="test_collection",
            embeddings_with_metadata=[{"embedding": [0.1, 0.2], "metadata": {}}],
            authz="/programs/a",
        )
    )

    assert len(captured_requests) == 1
    request = captured_requests[0]
    assert request.method == "POST"
    assert request.url.path == "/vectorstore/collections/test_collection/embeddings"
    assert json.loads(request.content) == {
        "embeddings": [{"embedding": [0.1, 0.2], "metadata": {}}],
        "authz": "/programs/a",
    }


def test_create_embeddings_omits_authz_by_default(embeddings_client, captured_requests):
    asyncio.run(
        embeddings_client.create_embeddings(
            collection_name="test_collection",
            embeddings_with_metadata=[{"embedding": [0.1], "metadata": {}}],
            overwrite=True,
        )
    )

    request = captured_requests[0]
    assert request.method == "PUT"
    assert "authz" not in json.loads(request.content)


def test_create_embeddings_rejects_per_item_authz(embeddings_client, captured_requests):
    """
    Tests that authz on an item raises instead of being silently ignored by the API.
    """
    with pytest.raises(ValueError, match="authz cannot be set per embedding"):
        asyncio.run(
            embeddings_client.create_embeddings(
                collection_name="test_collection",
                embeddings_with_metadata=[
                    {"embedding": [0.1], "metadata": {}, "authz": "/programs/a"}
                ],
            )
        )

    assert captured_requests == []


def test_list_embeddings_exclude_info(embeddings_client, captured_requests):
    asyncio.run(embeddings_client.list_embeddings("test_collection", exclude_info=True))

    assert captured_requests[0].url.params["exclude_info"] == "true"


@pytest.mark.parametrize("keep_original_text", [True, False])
def test_local_embedding_client_keep_text(keep_original_text):
    """
    Tests that the original text ends up in the metadata only when asked to keep it.
    """
    local_client = LocalEmbeddingClient.__new__(LocalEmbeddingClient)
    local_client.model = MagicMock()
    local_client.model.encode.return_value = numpy.array([[0.1, 0.2]])

    result = local_client.embed(
        [{"text": "some text", "metadata": {"file": "a.md"}}],
        keep_original_text=keep_original_text,
    )

    assert result[0]["embedding"] == [0.1, 0.2]
    if keep_original_text:
        assert result[0]["metadata"] == {"file": "a.md", "text": "some text"}
    else:
        assert result[0]["metadata"] == {"file": "a.md"}
