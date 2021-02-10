"""
Contains class for interacting with Gen3's Metadata Service.
"""
import aiohttp
import backoff
import requests
import urllib.parse
import logging
import sys

from gen3.utils import append_query_params, DEFAULT_BACKOFF_SETTINGS
from gen3.auth import Gen3Auth


class Gen3Metadata:
    """
    A class for interacting with the Gen3 Metadata services.

    Examples:
        This generates the Gen3Metadata class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(refresh_file="credentials.json")
        ... metadata = Gen3Metadata(auth)

    Attributes:
        endpoint (str): public endpoint for reading/querying metadata - only necessary if auth_provider not provided
        auth_provider (Gen3Auth): auth manager
    """

    def __init__(
        self,
        endpoint=None,
        auth_provider=None,
        service_location="mds",
        admin_endpoint_suffix="-admin",
    ):
        """
        Initialization for instance of the class to setup basic endpoint info.

        Args:
            endpoint (str): URL for a Data Commons that has metadata service deployed
            auth_provider (Gen3Auth, optional): Gen3Auth class to handle passing your
                token, required for admin endpoints
            service_location (str, optional): deployment location relative to the
                endpoint provided
        """
        # legacy interface required endpoint as 1st arg
        if endpoint and isinstance(endpoint, Gen3Auth):
            auth_provider = endpoint
            endpoint = None
        if auth_provider and isinstance(auth_provider, Gen3Auth):
            endpoint = auth_provider.endpoint
        endpoint = endpoint.strip("/")
        # if running locally, mds is deployed by itself without a location relative
        # to the commons
        if "http://localhost" in endpoint:
            service_location = ""
            admin_endpoint_suffix = ""

        if not endpoint.endswith(service_location):
            endpoint += "/" + service_location

        self.endpoint = endpoint.rstrip("/")
        self.admin_endpoint = endpoint.rstrip("/") + admin_endpoint_suffix
        self._auth_provider = auth_provider

    def is_healthy(self):
        """
        Return if is healthy or not

        Returns:
            bool: True if healthy
        """
        try:
            response = requests.get(
                self.endpoint + "/_status", auth=self._auth_provider
            )
            response.raise_for_status()
        except Exception as exc:
            logging.error(exc)
            return False

        return response.json().get("status") == "OK"

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_version(self):
        """
        Return the version

        Returns:
            str: the version
        """
        response = requests.get(self.endpoint + "/version", auth=self._auth_provider)
        response.raise_for_status()
        return response.text

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_index_key_paths(self):
        """
        List all the metadata key paths indexed in the database.

        Returns:
            List: list of metadata key paths
        """
        response = requests.get(
            self.admin_endpoint + "/metadata_index", auth=self._auth_provider
        )
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create_index_key_path(self, path):
        """
        Create a metadata key path indexed in the database.

        Args:
            path (str): metadata key path
        """
        response = requests.post(
            self.admin_endpoint + f"/metadata_index/{path}", auth=self._auth_provider
        )
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def delete_index_key_path(self, path):
        """
        List all the metadata key paths indexed in the database.

        Args:
            path (str): metadata key path
        """
        response = requests.delete(
            self.admin_endpoint + f"/metadata_index/{path}", auth=self._auth_provider
        )
        response.raise_for_status()
        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def query(self, query, return_full_metadata=False, limit=10, offset=0, **kwargs):
        """
        Query the metadata given a query.

        Query format is based off the logic used in the service:
            '''
            Without filters, this will return all data. Add filters as query strings like this:

            GET /metadata?a=1&b=2

        This will match all records that have metadata containing all of:
            {"a": 1, "b": 2}
            The values are always treated as strings for filtering. Nesting is supported:

            GET /metadata?a.b.c=3

        Matching records containing:
            {"a": {"b": {"c": 3}}}
            Providing the same key with more than one value filters records whose value of the given key matches any of the given values. But values of different keys must all match. For example:

            GET /metadata?a.b.c=3&a.b.c=33&a.b.d=4

        Matches these:
            {"a": {"b": {"c": 3, "d": 4}}}
            {"a": {"b": {"c": 33, "d": 4}}}
            {"a": {"b": {"c": "3", "d": 4, "e": 5}}}
            But won't match these:

            {"a": {"b": {"c": 3}}}
            {"a": {"b": {"c": 3, "d": 5}}}
            {"a": {"b": {"d": 5}}}
            {"a": {"b": {"c": "333", "d": 4}}}
            '''

        Args:
            query (str): mds query as defined by the metadata api
            return_full_metadata (bool, optional): if False will just return a list of guids
            limit (int, optional): max num records to return
            offset (int, optional): offset for output

        Returns:
            List: list of guids matching query
                OR if return_full_metadata=True
            Dict{guid: {metadata}}: Dictionary with GUIDs as keys and associated
                metadata JSON blobs as values
        """
        url = self.endpoint + f"/metadata?{query}"

        url_with_params = append_query_params(
            url, data=return_full_metadata, limit=limit, offset=offset, **kwargs
        )
        logging.debug(f"hitting: {url_with_params}")
        response = requests.get(url_with_params, auth=self._auth_provider)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_get(self, guid, _ssl=None, **kwargs):
        """
        Asynchronous function to get metadata

        Args:
            guid (str): guid to use
            _ssl (None, optional): whether or not to use ssl

        Returns:
            Dict: metadata for given guid
        """
        async with aiohttp.ClientSession() as session:
            url = self.endpoint + f"/metadata/{guid}"
            url_with_params = append_query_params(url, **kwargs)

            logging.debug(f"hitting: {url_with_params}")

            async with session.get(url_with_params, ssl=_ssl) as response:
                response.raise_for_status()
                response = await response.json()

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get(self, guid, **kwargs):
        """
        Get the metadata associated with the guid

        Args:
            guid (str): guid to use

        Returns:
            Dict: metadata for given guid
        """
        url = self.endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        response = requests.get(url_with_params, auth=self._auth_provider)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def batch_create(self, metadata_list, overwrite=True, **kwargs):
        """
        Create the list of metadata associated with the list of guids

        Args:
            metadata_list (List[Dict{"guid": "", "data": {}}]): list of metadata
                objects in a specific format. Expects a dict with "guid" and "data"
                fields where "data" is another JSON blob to add to the mds
            overwrite (bool, optional): whether or not to overwrite existing data
        """
        url = self.admin_endpoint + f"/metadata"

        if len(metadata_list) > 1 and (
            "guid" not in metadata_list[0] and "data" not in metadata_list[0]
        ):
            logging.warning(
                "it looks like your metadata list for bulk create is malformed. "
                "the expected format is a list of dicts that have 2 keys: 'guid' "
                "and 'data', where 'guid' is a string and 'data' is another dict. "
                f"The first element doesn't match that pattern: {metadata_list[0]}"
            )

        url_with_params = append_query_params(url, overwrite=overwrite, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {metadata_list}")
        response = requests.post(
            url_with_params, json=metadata_list, auth=self._auth_provider
        )
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create(self, guid, metadata, overwrite=False, **kwargs):
        """
        Create the metadata associated with the guid

        Args:
            guid (str): guid to use
            metadata (Dict): dictionary representing what will end up a JSON blob
                attached to the provided GUID as metadata
            overwrite (bool, optional): whether or not to overwrite existing data
        """
        url = self.admin_endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, overwrite=overwrite, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {metadata}")
        response = requests.post(
            url_with_params, json=metadata, auth=self._auth_provider
        )
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_create(self, guid, metadata, overwrite=False, _ssl=None, **kwargs):
        """
        Asynchronous function to create metadata

        Args:
            guid (str): guid to use
            metadata (Dict): dictionary representing what will end up a JSON blob
                attached to the provided GUID as metadata
            overwrite (bool, optional): whether or not to overwrite existing data
            _ssl (None, optional): whether or not to use ssl
        """
        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}"
            url_with_params = append_query_params(url, overwrite=overwrite, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            async with session.post(
                url_with_params, json=metadata, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()
                response = await response.json()

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def update(self, guid, metadata, **kwargs):
        """
        Update the metadata associated with the guid

        Args:
            guid (str): guid to use
            metadata (Dict): dictionary representing what will end up a JSON blob
                attached to the provided GUID as metadata
        """
        url = self.admin_endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {metadata}")
        response = requests.put(
            url_with_params, json=metadata, auth=self._auth_provider
        )
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_update(self, guid, metadata, _ssl=None, **kwargs):
        """
        Asynchronous function to update metadata

        Args:
            guid (str): guid to use
            metadata (Dict): dictionary representing what will end up a JSON blob
                attached to the provided GUID as metadata
            _ssl (None, optional): whether or not to use ssl
        """
        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}"
            url_with_params = append_query_params(url, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            async with session.put(
                url_with_params, json=metadata, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()
                response = await response.json()

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def delete(self, guid, **kwargs):
        """
        Delete the metadata associated with the guid

        Args:
            guid (str): guid to use
        """
        url = self.admin_endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        response = requests.delete(url_with_params, auth=self._auth_provider)
        response.raise_for_status()

        return response.json()
