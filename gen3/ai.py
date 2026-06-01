import logging
from typing import Any

import backoff
import httpx
from cdislogging import get_logger

from gen3.auth import Gen3Auth
from gen3.utils import DEFAULT_BACKOFF_SETTINGS

logger = get_logger("__name__")


class EmbeddingsClient:
    """
    Client for interacting with the gen3_embeddings API.

    For local:

    ```python
        auth = Gen3Auth(refresh_file="credentials.json")
        embeddings_client = EmbeddingClient(
            auth=auth,
            endpoint="localhost:4143",
            api_prefix="",
        )
    ````

    """

    def __init__(
        self,
        auth: Gen3Auth,
        endpoint: str | None = None,
        api_prefix: str = "ai",
    ):
        """
        Initialize the embedding client.
        """
        # prefer provided endpoint and fallback on one in creds
        endpoint = endpoint or auth.endpoint or ""
        endpoint = endpoint.strip("/")

        if not endpoint.endswith(api_prefix):
            endpoint += "/" + api_prefix

        self.endpoint = endpoint.rstrip("/")
        self._auth = auth

    def _get_headers(self) -> dict[str, str]:
        """Get headers for API requests."""
        auth_token = self._auth.get_access_token()

        if not auth_token:
            logger.warning(f"No auth token could be obtained.")

        headers = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        return headers

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def create_collection(
        self,
        collection_name: str,
        dimensions: int,
        description: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a new vectorstore collection.

        Args:
            collection_name (str): Name of the collection to create
            dimensions (int): Number of dimensions for embeddings
            description (str): Optional description for the collection

        Returns:
            Response from the API

        Raises:
            httpx.HTTPError: If the request fails
        """
        url = f"{self.endpoint}/vectorstore/collections"

        payload = {
            "collection_name": collection_name,
            "dimensions": dimensions,
        }

        # TODO: remove when the service does this automatically
        if dimensions > 2000:
            payload["vector_type"] = "halfvec"

        if description:
            payload["description"] = description

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=payload,
                headers=self._get_headers(),
            )
            response.raise_for_status()
            return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def create_embeddings(
        self,
        collection_name: str,
        embeddings_with_metadata: list[dict],
        collection_id: int | None = None,
        ai_model: str | None = None,
    ) -> dict[str, Any]:
        """
        Create and add embeddings to an existing collection.

        Args:
            collection_name (str): Name of the collection to add embeddings to
            embeddings_with_metadata (list[dict]): List of dictionaries containing the embedding and metadata
            ai_model (str): Optional AI model name

        Returns:
            Response from the API

        Raises:
            httpx.HTTPError: If the request fails
        """
        url = f"{self.endpoint}/vectorstore/collections/{collection_name}/embeddings"
        params = {}
        if ai_model:
            params["ai_model"] = ai_model

        body = {"embeddings": embeddings_with_metadata}

        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=body,
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()
            return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def list_collections(
        self, collection_name: str | None = None
    ) -> list[dict[str, Any]]:
        """
        List all vectorstore collections.

        Args:
            collection_name (str): optional collection_name to retrieve, if not provided will list all

        Returns:
            List of collection information

        Raises:
            httpx.HTTPError: If the request fails
        """
        url = f"{self.endpoint}/vectorstore/collections"
        collections = []

        if collection_name:
            url += f"/{collection_name}"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self._get_headers())
            response.raise_for_status()
            response_json = response.json()

        if collection_name:
            collections = [response_json]
        else:
            collections = response_json.get("collections", [])

        return collections

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def list_embeddings(
        self,
        collection_name: str,
        no_embeddings_info: bool = False,
    ) -> list[dict[str, Any]]:
        """
        List all embeddings in an collection.

        Args:
            collection_name (str): Name of the collection
            no_embeddings_info (bool): If True, omit info block in response to reduce payload size

        Returns:
            List of embedding information

        Raises:
            httpx.HTTPError: If the request fails
        """
        url = f"{self.endpoint}/vectorstore/collections/{collection_name}/embeddings"
        params = {}
        if no_embeddings_info:
            params["no_embeddings_info"] = "true"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def delete_collection(self, collection_name: str) -> None:
        """
        Delete a vectorstore collection.

        Args:
            collection_name (str): Name of the collection to delete

        Returns:
            Response from the API

        Raises:
            httpx.HTTPError: If the request fails
        """
        url = f"{self.endpoint}/vectorstore/collections/{collection_name}"

        async with httpx.AsyncClient() as client:
            response = await client.delete(url, headers=self._get_headers())
            response.raise_for_status()

            # 204 on success, so no body
            return None


class LocalEmbeddingClient:
    """
    Client for local embedding generation using sentence-transformers.
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """
        Initialize the local embedding client.

        Args:
            model_name (str): Name of the sentence-transformers model to use
        """
        # lazy load. moved from top-level to avoid extensive loading at module load
        # b/c we're lazy loading, we need to explicitly set the log level for this module
        from sentence_transformers import SentenceTransformer

        logging.getLogger("sentence_transformers").setLevel(logger.level)

        self.model = SentenceTransformer(model_name)
        self.dimensions = self.model.get_embedding_dimension()
        logger.info(f"Loaded model: {model_name} (dimensions: {self.dimensions})")

    def embed(
        self,
        texts_with_metadata: list[dict],
        keep_original_text: bool = True,
        show_progress_bar: bool = False,
    ) -> list:
        """
        Generate embeddings for a list of texts.

        Args:
            texts_with_metadata (list[dict]): List of text strings to embed w/ metadata

        Returns:
            list of objects with embedding and metadata
        """
        texts = [item["text"] for item in texts_with_metadata]

        embeddings = self.model.encode(
            texts, convert_to_numpy=True, show_progress_bar=show_progress_bar
        ).tolist()

        # we can only have an embedding with metadata in the final output, so
        # if we need to keep the original text, move it to the metadata
        if not keep_original_text:
            for item in texts_with_metadata:
                item["metadata"].update({"text": item["text"]})
                del item["text"]

        # this format should be what the JSON body for bulk creation of embeddings
        # expects
        embeddings_with_metadata = [
            {
                "embedding": embedding,
                "metadata": text_with_metadata["metadata"],
                "authz": text_with_metadata.get("authz", ""),
            }
            for embedding, text_with_metadata in zip(embeddings, texts_with_metadata)
        ]

        return embeddings_with_metadata
