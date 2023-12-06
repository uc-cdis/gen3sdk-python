"""
Contains class for interacting with Gen3's Metadata Service.
"""
import aiohttp
import backoff
from datetime import datetime
import requests
import json
import os
from urllib.parse import urlparse

from gen3.utils import (
    append_query_params,
    DEFAULT_BACKOFF_SETTINGS,
    BACKOFF_NO_LOG_IF_NOT_RETRIED,
    _verify_schema,
)
from gen3.auth import Gen3Auth
from gen3.tools.utils import (
    RECORD_TYPE_STANDARD_KEY,
    GUID_COLUMN_NAMES,
    FILENAME_COLUMN_NAMES,
    SIZE_COLUMN_NAMES,
    MD5_COLUMN_NAMES,
    ACLS_COLUMN_NAMES,
    URLS_COLUMN_NAMES,
    AUTHZ_COLUMN_NAMES,
    PREV_GUID_COLUMN_NAMES,
)

from cdislogging import get_logger

logging = get_logger("__name__")


PACKAGE_CONTENTS_STANDARD_KEY = "package_contents"
PACKAGE_CONTENTS_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "file_name": {
                "type": "string",
            },
            "size": {
                "type": "integer",
            },
            "hashes": {
                "type": "object",
            },
        },
        "required": ["file_name"],
        "additionalProperties": True,
    },
}


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
    def query(
        self,
        query,
        return_full_metadata=False,
        limit=10,
        offset=0,
        use_agg_mds=False,
        **kwargs,
    ):
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

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
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

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
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

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    def create(self, guid, metadata, aliases=None, overwrite=False, **kwargs):
        """
        Create the metadata associated with the guid

        Args:
            guid (str): guid to use
            metadata (Dict): dictionary representing what will end up a JSON blob
                attached to the provided GUID as metadata
            overwrite (bool, optional): whether or not to overwrite existing data
        """
        aliases = aliases or []

        url = self.admin_endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, overwrite=overwrite, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {metadata}")
        response = requests.post(
            url_with_params, json=metadata, auth=self._auth_provider
        )
        response.raise_for_status()

        if aliases:
            try:
                self.create_aliases(guid=guid, aliases=aliases, merge=overwrite)
            except Exception:
                logging.error(
                    "Error while attempting to create aliases: "
                    f"'{aliases}' to GUID: '{guid}' with merge={overwrite}. "
                    "GUID metadata record was created successfully and "
                    "will NOT be deleted."
                )

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    async def async_create(
        self,
        guid,
        metadata,
        aliases=None,
        overwrite=False,
        _ssl=None,
        **kwargs,
    ):
        """
        Asynchronous function to create metadata

        Args:
            guid (str): guid to use
            metadata (Dict): dictionary representing what will end up a JSON blob
                attached to the provided GUID as metadata
            overwrite (bool, optional): whether or not to overwrite existing data
            _ssl (None, optional): whether or not to use ssl
        """
        aliases = aliases or []

        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}"
            url_with_params = append_query_params(url, overwrite=overwrite, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            logging.debug(f"hitting: {url_with_params}")
            logging.debug(f"data: {metadata}")
            async with session.post(
                url_with_params, json=metadata, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()
                response = await response.json()

            if aliases:
                logging.info(f"creating aliases: {aliases}")
                try:
                    await self.async_create_aliases(
                        guid=guid, aliases=aliases, _ssl=_ssl
                    )
                except Exception:
                    logging.error(
                        "Error while attempting to create aliases: "
                        f"'{aliases}' to GUID: '{guid}'. "
                        "GUID metadata record was created successfully and "
                        "will NOT be deleted."
                    )

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def update(self, guid, metadata, aliases=None, merge=False, **kwargs):
        """
        Update the metadata associated with the guid

        Args:
            guid (str): guid to use
            metadata (Dict): dictionary representing what will end up a JSON blob
                attached to the provided GUID as metadata
        """
        aliases = aliases or []

        url = self.admin_endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {metadata}")
        response = requests.put(
            url_with_params, json=metadata, auth=self._auth_provider
        )
        response.raise_for_status()

        if aliases:
            try:
                self.update_aliases(guid=guid, aliases=aliases, merge=merge)
            except Exception:
                logging.error(
                    "Error while attempting to update aliases: "
                    f"'{aliases}' to GUID: '{guid}'. "
                    "GUID metadata record was created successfully and "
                    "will NOT be deleted."
                )

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_update(
        self, guid, metadata, aliases=None, merge=False, _ssl=None, **kwargs
    ):
        """
        Asynchronous function to update metadata

        Args:
            guid (str): guid to use
            metadata (Dict): dictionary representing what will end up a JSON blob
                attached to the provided GUID as metadata
            aliases (list[str], optional): List of aliases to update the GUID with
            merge (bool, optional): Whether or not to merge metadata AND aliases
              with existing values
            _ssl (None, optional): whether or not to use ssl
            **kwargs: Description
        """
        aliases = aliases or []

        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}"
            url_with_params = append_query_params(url, merge=merge, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            async with session.put(
                url_with_params, json=metadata, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()
                response = await response.json()

            if aliases:
                try:
                    await self.async_update_aliases(
                        guid=guid, aliases=aliases, merge=merge, _ssl=_ssl
                    )
                except Exception:
                    logging.error(
                        "Error while attempting to update aliases: "
                        f"'{aliases}' to GUID: '{guid}' with merge={merge}. "
                        "GUID metadata record was created successfully and "
                        "will NOT be deleted."
                    )

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

    #
    # Alias Support
    #

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    def get_aliases(self, guid, **kwargs):
        """
        Get Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to get aliases
        """
        url = self.endpoint + f"/metadata/{guid}/aliases"
        url_with_params = append_query_params(url, **kwargs)

        logging.debug(f"hitting: {url_with_params}")
        response = requests.get(url_with_params, auth=self._auth_provider)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    async def async_get_aliases(self, guid, _ssl=None, **kwargs):
        """
        Asyncronously get Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            _ssl (None, optional): whether or not to use ssl
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to get aliases
        """
        async with aiohttp.ClientSession() as session:
            url = self.endpoint + f"/metadata/{guid}/aliases"
            url_with_params = append_query_params(url, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            logging.debug(f"hitting: {url_with_params}")
            async with session.get(
                url_with_params, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()

            return await response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    def delete_alias(self, guid, alias, **kwargs):
        """
        Delete single Alias for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to delete aliases
        """
        url = self.admin_endpoint + f"/metadata/{guid}/aliases/{alias}"
        url_with_params = append_query_params(url, **kwargs)

        logging.debug(f"hitting: {url_with_params}")
        response = requests.delete(url_with_params, auth=self._auth_provider)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    async def async_delete_alias(self, guid, alias, _ssl=None, **kwargs):
        """
        Asyncronously delete single Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            _ssl (None, optional): whether or not to use ssl
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to delete aliases
        """
        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}/aliases/{alias}"
            url_with_params = append_query_params(url, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            logging.debug(f"hitting: {url_with_params}")
            async with session.delete(
                url_with_params, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()

            return await response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    def create_aliases(self, guid, aliases, **kwargs):
        """
        Create Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            aliases (list[str]): Aliases to set for the guid
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to create aliases
        """
        url = self.admin_endpoint + f"/metadata/{guid}/aliases"
        url_with_params = append_query_params(url, **kwargs)

        data = {"aliases": aliases}

        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {data}")
        response = requests.post(url_with_params, json=data, auth=self._auth_provider)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    async def async_create_aliases(self, guid, aliases, _ssl=None, **kwargs):
        """
        Asyncronously create Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            aliases (list[str]): Aliases to set for the guid
            _ssl (None, optional): whether or not to use ssl
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to create aliases
        """
        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}/aliases"
            url_with_params = append_query_params(url, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            data = {"aliases": aliases}

            logging.debug(f"hitting: {url_with_params}")
            logging.debug(f"data: {data}")
            async with session.post(
                url_with_params, json=data, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()
                return await response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    def update_aliases(self, guid, aliases, merge=False, **kwargs):
        """
        Update Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            aliases (list[str]): Aliases to set for the guid
            merge (bool, optional): Whether or not to aliases with existing values
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to update aliases
        """
        url = self.admin_endpoint + f"/metadata/{guid}/aliases"
        url_with_params = append_query_params(url, merge=merge, **kwargs)

        data = {"aliases": aliases}

        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {data}")
        response = requests.put(url_with_params, json=data, auth=self._auth_provider)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    async def async_update_aliases(
        self, guid, aliases, merge=False, _ssl=None, **kwargs
    ):
        """
        Asyncronously update Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            aliases (list[str]): Aliases to set for the guid
            merge (bool, optional): Whether or not to aliases with existing values
            _ssl (None, optional): whether or not to use ssl
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to update aliases
        """
        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}/aliases"
            url_with_params = append_query_params(url, merge=merge, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            data = {"aliases": aliases}

            logging.debug(f"hitting: {url_with_params}")
            logging.debug(f"data: {data}")
            async with session.put(
                url_with_params, json=data, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()

                return await response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    def delete_aliases(self, guid, **kwargs):
        """
        Delete all Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to delete aliases
        """
        url = self.admin_endpoint + f"/metadata/{guid}/aliases"
        url_with_params = append_query_params(url, **kwargs)

        logging.debug(f"hitting: {url_with_params}")
        response = requests.delete(url_with_params, auth=self._auth_provider)
        response.raise_for_status()

        return response.text

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    async def async_delete_aliases(self, guid, _ssl=None, **kwargs):
        """
        Asyncronously delete all Aliases for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            _ssl (None, optional): whether or not to use ssl
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to delete aliases
        """
        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}/aliases"
            url_with_params = append_query_params(url, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            logging.debug(f"hitting: {url_with_params}")
            async with session.delete(
                url_with_params, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()

                return await response.text

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    def delete_alias(self, guid, alias, **kwargs):
        """
        Delete single Alias for the given guid

        Args:
            guid (TYPE): Globally unique ID for the metadata blob
            alias (str): alternative identifier (alias) to delete
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to delete aliases
        """
        url = self.admin_endpoint + f"/metadata/{guid}/aliases/{alias}"
        url_with_params = append_query_params(url, **kwargs)

        logging.debug(f"hitting: {url_with_params}")
        response = requests.delete(url_with_params, auth=self._auth_provider)
        response.raise_for_status()

        return response.text

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_NO_LOG_IF_NOT_RETRIED)
    async def async_delete_alias(self, guid, alias, _ssl=None, **kwargs):
        """
        Asyncronously delete single Aliases for the given guid

        Args:
            guid (str): Globally unique ID for the metadata blob
            alias (str): alternative identifier (alias) to delete
            _ssl (None, optional): whether or not to use ssl
            **kwargs: additional query params

        Returns:
            requests.Response: response from the request to delete aliases
        """
        async with aiohttp.ClientSession() as session:
            url = self.admin_endpoint + f"/metadata/{guid}/aliases/{alias}"
            url_with_params = append_query_params(url, **kwargs)

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self._auth_provider._get_auth_value()}

            logging.debug(f"hitting: {url_with_params}")
            async with session.delete(
                url_with_params, headers=headers, ssl=_ssl
            ) as response:
                response.raise_for_status()

                return await response.text

    def _prepare_metadata(
        self, metadata, indexd_doc, force_metadata_columns_even_if_empty
    ):
        """
        Validate and generate the provided metadata for submission to the metadata
        service.

        If the record is of type "package", also prepare package metadata.

        Args:
            metadata (dict): metadata provided by the submitter
            indexd_doc (dict): the indexd document created for this data
            force_metadata_columns_even_if_empty (bool): see description in calling function

        Returns:
            dict: metadata ready to be submitted to the metadata service
        """

        def _extract_non_indexd_metadata(metadata):
            """
            Get the "additional metadata": metadata that was provided but is
            not stored in indexd, so should be stored in the metadata service.
            """
            return {
                k: v
                for k, v in metadata.items()
                if k.lower()
                not in GUID_COLUMN_NAMES
                + FILENAME_COLUMN_NAMES
                + SIZE_COLUMN_NAMES
                + MD5_COLUMN_NAMES
                + ACLS_COLUMN_NAMES
                + URLS_COLUMN_NAMES
                + AUTHZ_COLUMN_NAMES
                + PREV_GUID_COLUMN_NAMES
            }

        to_submit = _extract_non_indexd_metadata(metadata)

        # some additional metadata columns must be validated
        valid = True

        # validate package columns
        record_type = to_submit.pop(RECORD_TYPE_STANDARD_KEY, "").strip().lower()
        package_contents = to_submit.pop(PACKAGE_CONTENTS_STANDARD_KEY, None)
        if record_type == "package":
            if package_contents:
                package_contents = json.loads(package_contents)
                if not _verify_schema(package_contents, PACKAGE_CONTENTS_SCHEMA):
                    logging.error(
                        f"ERROR: {package_contents} is not in package contents format"
                    )
                    valid = False
            # generate package metadata
            package_metadata = self._get_package_metadata(
                metadata,
                indexd_doc.file_name,
                indexd_doc.size,
                indexd_doc.hashes,
                indexd_doc.urls,
                package_contents,
            )
            to_submit.update(package_metadata)
        elif package_contents:
            logging.error(
                f"ERROR: tried to set '{PACKAGE_CONTENTS_STANDARD_KEY}' for a non-package row. Ignoring '{PACKAGE_CONTENTS_STANDARD_KEY}'. Set '{RECORD_TYPE_STANDARD_KEY}' to 'package' to create packages."
            )
            valid = False

        if not valid:
            raise Exception(f"Metadata is not valid: {metadata}")

        if not force_metadata_columns_even_if_empty:
            # remove any empty columns if we're not being forced to include them
            to_submit = {
                key: value
                for key, value in to_submit.items()
                if value is not None and value != ""
            }

        return to_submit

    def _get_package_metadata(
        self, submitted_metadata, file_name, file_size, hashes, urls, contents
    ):
        """
        The MDS Objects API currently expects files that have not been
        uploaded yet. For files we only needs to index, not upload, create
        object records manually by generating the expected object fields.
        TODO: update the MDS objects API to not create upload URLs if the
        relevant data is provided.
        """

        def _get_filename_from_urls(submitted_metadata, urls):
            file_name = ""
            if not urls:
                logging.warning(f"No URLs provided for: {submitted_metadata}")
            for url in urls:
                _file_name = os.path.basename(url)
                if not file_name:
                    file_name = _file_name
                else:
                    if file_name != _file_name:
                        logging.warning(
                            f"Received multiple URLs with different file names; will use the first URL (file name '{file_name}'): {submitted_metadata}"
                        )
            return file_name

        file_name_from_url = _get_filename_from_urls(submitted_metadata, urls)
        if not file_name:
            file_name = file_name_from_url

        now = str(datetime.utcnow())
        metadata = {
            "type": "package",
            "package": {
                "version": "0.1",
                "file_name": file_name,
                "created_time": now,
                "updated_time": now,
                "size": file_size,
                "hashes": hashes,
                "contents": contents or None,
            },
            "_upload_status": "uploaded",
        }
        return metadata
