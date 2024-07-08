import aiohttp
import backoff
import requests
import urllib.parse
from cdislogging import get_logger

import sys

import indexclient.client as client

from gen3.utils import DEFAULT_BACKOFF_SETTINGS, raise_for_status_and_print_error
from gen3.auth import Gen3Auth

logging = get_logger("__name__")


class Gen3Index:
    """

    A class for interacting with the Gen3 Index services.

    Args:
        endpoint (str): public endpoint for reading/querying indexd - only necessary if auth_provider not provided
        auth_provider (Gen3Auth): A Gen3Auth class instance or indexd basic creds tuple

    Examples:
        This generates the Gen3Index class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(refresh_file="credentials.json")
        ... index = Gen3Index(auth)

    """

    def __init__(self, endpoint=None, auth_provider=None, service_location="index"):
        # legacy interface required endpoint as 1st arg
        if endpoint and isinstance(endpoint, Gen3Auth):
            auth_provider = endpoint
            endpoint = None
        if auth_provider and isinstance(auth_provider, Gen3Auth):
            endpoint = auth_provider.endpoint
        endpoint = endpoint.strip("/")
        # if running locally, indexd is deployed by itself without a location relative
        # to the commons
        if "http://localhost" in endpoint:
            service_location = ""

        if not endpoint.endswith(service_location):
            endpoint += "/" + service_location

        self.client = client.IndexClient(endpoint, auth=auth_provider)

    ### Get Requests
    def is_healthy(self):
        """

        Return if indexd is healthy or not

        """
        try:
            response = self.client._get("_status")
            response.raise_for_status()
        except Exception:
            return False
        return response.text == "Healthy"

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_version(self):
        """

        Return the version of indexd

        """
        response = self.client._get("_version")
        raise_for_status_and_print_error(response)
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_stats(self):
        """

        Return basic info about the records in indexd

        """
        response = self.client._get("_stats")
        raise_for_status_and_print_error(response)
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_all_records(self, limit=None, paginate=False):
        """

        Get a list of all records

        """
        all_records = []
        url = "index/"

        if limit:
            url += f"?limit={limit}"

        response = self.client._get(url)
        raise_for_status_and_print_error(response)

        records = response.json().get("records")
        all_records.extend(records)

        if paginate and records:
            previous_did = None
            start_did = records[-1].get("did")

            while start_did != previous_did:
                previous_did = start_did

                params = {"start": f"{start_did}"}
                url_parts = list(urllib.parse.urlparse(url))
                query = dict(urllib.parse.parse_qsl(url_parts[4]))
                query.update(params)

                url_parts[4] = urllib.parse.urlencode(query)

                url = urllib.parse.urlunparse(url_parts)
                response = self.client._get(url)
                raise_for_status_and_print_error(response)

                records = response.json().get("records")
                all_records.extend(records)

                if records:
                    start_did = response.json().get("records")[-1].get("did")

        return all_records

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_records_on_page(self, limit=None, page=None):
        """

        Get a list of all records given the page and page size limit

        """
        params = {}
        url = "index/"

        if limit is not None:
            params["limit"] = limit

        if page is not None:
            params["page"] = page

        query = urllib.parse.urlencode(params)

        response = self.client._get(url + "?" + query)
        raise_for_status_and_print_error(response)

        return response.json().get("records")

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_get_record(self, guid=None, _ssl=None):
        """
        Asynchronous function to request a record from indexd.

        Args:
            guid (str): record guid

        Returns:
            dict: indexd record
        """
        url = f"{self.client.url}/index/{guid}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=_ssl) as response:
                raise_for_status_and_print_error(response)
                response = await response.json()

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_get_records_on_page(self, limit=None, page=None, _ssl=None):
        """
        Asynchronous function to request a page from indexd.

        Args:
            page (int/str): indexd page to request

        Returns:
            List[dict]: List of indexd records from the page
        """
        all_records = []
        params = {}

        if limit is not None:
            params["limit"] = limit

        if page is not None:
            params["page"] = page

        query = urllib.parse.urlencode(params)

        url = f"{self.client.url}/index" + "?" + query
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=_ssl) as response:
                response = await response.json()

        return response.get("records")

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_get_records_from_checksum(
        self, checksum, checksum_type="md5", _ssl=None
    ):
        """
        Asynchronous function to request records from indexd matching checksum.

        Args:
            checksum (str): indexd checksum to request
            checksum_type (str): type of checksum, defaults to md5

        Returns:
            List[dict]: List of indexd records
        """
        all_records = []
        params = {}

        params["hash"] = f"{checksum_type}:{checksum}"

        query = urllib.parse.urlencode(params)

        url = f"{self.client.url}/index" + "?" + query
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=_ssl) as response:
                response = await response.json()

        return response.get("records")

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get(self, guid, dist_resolution=True):
        """

        Get the metadata associated with the given id, alias, or
        distributed identifier

        Args:
            guid: string
                - record id
            dist_resolution: boolean
            - *optional* Specify if we want distributed dist_resolution or not

        """
        rec = self.client.global_get(guid, dist_resolution)

        if not rec:
            return rec

        return rec.to_json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_urls(self, size=None, hashes=None, guids=None):
        """

        Get a list of urls that match query params

        Args:
            size: integer
                - object size
            hashes: string
                - hashes specified as algorithm:value
            guids: list
                - list of ids

        """
        if guids:
            guids = ",".join(guids)
        p = {"size": size, "hash": hashes, "ids": guids}
        urls = self.client._get("urls", params=p).json()
        return [url for _, url in urls.items()]

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_record(self, guid):
        """

        Get the metadata associated with a given id

        """
        rec = self.client.get(guid)

        if not rec:
            return rec

        return rec.to_json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_record_doc(self, guid):
        """

        Get the metadata associated with a given id

        """
        return self.client.get(guid)

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_with_params(self, params=None):
        """

        Return a document object corresponding to the supplied parameters, such
        as ``{'hashes': {'md5': '...'}, 'size': '...', 'metadata': {'file_state': '...'}}``.

            - need to include all the hashes in the request
            - index client like signpost or indexd will need to handle the
              query param `'hash': 'hash_type:hash'`

        """
        rec = self.client.get_with_params(params)

        if not rec:
            return rec

        return rec.to_json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_get_with_params(self, params, _ssl=None):
        """

        Return a document object corresponding to the supplied parameter

        - need to include all the hashes in the request
        - need to handle the query param `'hash': 'hash_type:hash'`

        Args:
            params (dict): params to search with
            _ssl (None, optional): whether or not to use ssl

        Returns:
            Document: json representation of an entry in indexd

        """
        query_params = urllib.parse.urlencode(params)
        url = f"{self.client.url}/index/?{query_params}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, ssl=_ssl) as response:
                await response.raise_for_status()
                response = await response.json()

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_latest_version(self, guid, has_version=False):
        """

        Get the metadata of the latest index record version associated
        with the given id

        Args:
            guid: string
                - record id
            has_version: boolean
                - *optional* exclude entries without a version

        """
        rec = self.client.get_latest_version(guid, has_version)

        if not rec:
            return rec

        return rec.to_json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_versions(self, guid):
        """

        Get the metadata of index record version associated with the
        given id

        Args:
            guid: string
                - record id

        """
        response = self.client._get(f"/index/{guid}/versions")
        raise_for_status_and_print_error(response)
        versions = response.json()

        return [r for _, r in versions.items()]

    ### Post Requests

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create_record(
        self,
        hashes,
        size,
        did=None,
        urls=None,
        file_name=None,
        metadata=None,
        baseid=None,
        acl=None,
        urls_metadata=None,
        version=None,
        authz=None,
        description=None,
        content_created_date=None,
        content_updated_date=None,
    ):
        """

        Create a new record and add it to the index

        Args:
            hashes (dict): {hash type: hash value,}
                eg ``hashes={'md5': ab167e49d25b488939b1ede42752458b'}``
            size (int): file size metadata associated with a given uuid
            did (str): provide a UUID for the new indexd to be made
            urls (list): list of URLs where you can download the UUID
            acl (list): access control list
            authz (list): RBAC strings
            file_name (str): name of the file associated with a given UUID
            metadata (dict): additional key value metadata for this entry
            urls_metadata (dict): metadata attached to each url
            baseid (str): optional baseid to group with previous entries versions
            version (str): entry version string
            description (str): optional description of the object
            content_created_date (datetime): optional creation date and time of the content being indexed
            content_updated_date (datetime): optional update date and time of the content being indexed
        Returns:
            Document: json representation of an entry in indexd

        """
        if urls is None:
            urls = []
        json = {
            "urls": urls,
            "hashes": hashes,
            "size": size,
            "file_name": file_name,
            "metadata": metadata,
            "urls_metadata": urls_metadata,
            "baseid": baseid,
            "acl": acl,
            "authz": authz,
            "version": version,
            "description": description,
            "content_created_date": content_created_date,
            "content_updated_date": content_updated_date,
        }
        if did:
            json["did"] = did
        rec = self.client.create(**json)

        return rec.to_json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_create_record(
        self,
        hashes,
        size,
        did=None,
        urls=None,
        file_name=None,
        metadata=None,
        baseid=None,
        acl=None,
        urls_metadata=None,
        version=None,
        authz=None,
        _ssl=None,
        description=None,
        content_created_date=None,
        content_updated_date=None,
    ):
        """
        Asynchronous function to create a record in indexd.

        Args:
            hashes (dict): {hash type: hash value,}
                eg ``hashes={'md5': ab167e49d25b488939b1ede42752458b'}``
            size (int): file size metadata associated with a given uuid
            did (str): provide a UUID for the new indexd to be made
            urls (list): list of URLs where you can download the UUID
            acl (list): access control list
            authz (str): RBAC string
            file_name (str): name of the file associated with a given UUID
            metadata (dict): additional key value metadata for this entry
            urls_metadata (dict): metadata attached to each url
            baseid (str): optional baseid to group with previous entries versions
            version (str): entry version string
            description (str): optional description of the object
            content_created_date (datetime): optional creation date and time of the content being indexed
            content_updated_date (datetime): optional update date and time of the content being indexed

        Returns:
            Document: json representation of an entry in indexd
        """
        async with aiohttp.ClientSession() as session:
            if urls is None:
                urls = []

            json = {
                "form": "object",
                "hashes": hashes,
                "size": size,
                "urls": urls or [],
            }
            if did:
                json["did"] = did
            if file_name:
                json["file_name"] = file_name
            if metadata:
                json["metadata"] = metadata
            if baseid:
                json["baseid"] = baseid
            if acl:
                json["acl"] = acl
            if urls_metadata:
                json["urls_metadata"] = urls_metadata
            if version:
                json["version"] = version
            if authz:
                json["authz"] = authz
            if description:
                json["description"] = description
            if content_created_date:
                json["content_created_date"] = content_created_date
            if content_updated_date:
                json["content_updated_date"] = content_updated_date
            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self.client.auth._get_auth_value()}

            async with session.post(
                f"{self.client.url}/index/",
                json=json,
                headers=headers,
                ssl=_ssl,
            ) as response:
                assert response.status == 200, await response.json()
                response = await response.json()

        return response

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create_blank(self, uploader, file_name=None):
        """

        Create a blank record

        Args:
            json - json in the format:
            {
                'uploader': type(string)
                'file_name': type(string) (optional*)
            }

        """
        json = {"uploader": uploader, "file_name": file_name}
        response = self.client._post(
            "index/blank",
            headers={"content-type": "application/json"},
            auth=self.client.auth,
            data=client.json_dumps(json),
        )
        raise_for_status_and_print_error(response)
        rec = response.json()

        return self.get_record(rec["did"])

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def create_new_version(
        self,
        guid,
        hashes,
        size,
        did=None,
        urls=None,
        file_name=None,
        metadata=None,
        acl=None,
        urls_metadata=None,
        version=None,
        authz=None,
        description=None,
        content_created_date=None,
        content_updated_date=None,
    ):
        """

        Add new version for the document associated to the provided uuid

        Since data content is immutable, when you want to change the
        size or hash, a new index document with a new uuid needs to be
        created as its new version. That uuid is returned in the did
        field of the response. The old index document is not deleted.

        Args:
            guid: (string): record id
            hashes (dict): {hash type: hash value,}
                eg ``hashes={'md5': ab167e49d25b488939b1ede42752458b'}``
            size (int): file size metadata associated with a given uuid
            did (str): provide a UUID for the new indexd to be made
            urls (list): list of URLs where you can download the UUID
            file_name (str): name of the file associated with a given UUID
            metadata (dict): additional key value metadata for this entry
            acl (list): access control list
            urls_metadata (dict): metadata attached to each url
            version (str): entry version string
            authz (str): RBAC string
            description (str): optional description of the object
            content_created_date (datetime): optional creation date and time of the content being indexed
            content_updated_date (datetime): optional update date and time of the content being indexed

            body: json/dictionary format
            - Metadata object that needs to be added to the store.
              Providing size and at least one hash is necessary and
              sufficient. Note: it is a good idea to add a version
              number

        """
        if urls is None:
            urls = []
        json = {
            "urls": urls,
            "form": "object",
            "hashes": hashes,
            "size": size,
            "file_name": file_name,
            "metadata": metadata,
            "urls_metadata": urls_metadata,
            "acl": acl,
            "authz": authz,
            "version": version,
            "description": description,
            "content_created_date": content_created_date,
            "content_updated_date": content_updated_date,
        }
        if did:
            json["did"] = did
        response = self.client._post(
            "index",
            guid,
            headers={"content-type": "application/json"},
            data=client.json_dumps(json),
            auth=self.client.auth,
        )
        raise_for_status_and_print_error(response)
        rec = response.json()

        if rec and "did" in rec:
            return self.get_record(rec["did"])
        return None

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_records(self, dids):
        """

        Get a list of documents given a list of dids

        Args:
            dids: list
                 - a list of record ids

        Returns:
            list: json representing index records

        """
        try:
            response = self.client._post(
                "bulk/documents", json=dids, auth=self.client.auth
            )
        except requests.HTTPError as exception:
            if exception.response.status_code == 404:
                return None
            else:
                raise exception

        return response.json()

    ### Put Requests

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def update_blank(self, guid, rev, hashes, size, urls=None, authz=None):
        """

        Update only hashes and size for a blank index

        Args:
            guid (string): record id
            rev (string): data revision - simple consistency mechanism
            hashes (dict): {hash type: hash value,}
                eg ``hashes={'md5': ab167e49d25b488939b1ede42752458b'}``
            size (int): file size metadata associated with a given uuid

        """
        params = {"rev": rev}
        json = {"hashes": hashes, "size": size}
        if urls:
            json["urls"] = urls
        if authz:
            json["authz"] = authz

        response = self.client._put(
            "index/blank",
            guid,
            headers={"content-type": "application/json"},
            params=params,
            auth=self.client.auth,
            data=client.json_dumps(json),
        )
        raise_for_status_and_print_error(response)
        rec = response.json()

        return self.get_record(rec["did"])

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def update_record(
        self,
        guid,
        file_name=None,
        urls=None,
        version=None,
        metadata=None,
        acl=None,
        authz=None,
        urls_metadata=None,
        description=None,
        content_created_date=None,
        content_updated_date=None,
    ):
        """

        Update an existing entry in the index

        Args:
             guid: string
                 - record id
             body: json/dictionary format
                 - index record information that needs to be updated.
                 - can not update size or hash, use new version for that

        """
        updatable_attrs = {
            "file_name": file_name,
            "urls": urls,
            "version": version,
            "metadata": metadata,
            "acl": acl,
            "authz": authz,
            "urls_metadata": urls_metadata,
            "description": description,
            "content_created_date": content_created_date,
            "content_updated_date": content_updated_date,
        }
        rec = self.client.get(guid)
        for k, v in updatable_attrs.items():
            if v is not None:
                exec(f"rec.{k} = v")
        rec.patch()
        return rec.to_json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_update_record(
        self,
        guid,
        file_name=None,
        urls=None,
        version=None,
        metadata=None,
        acl=None,
        authz=None,
        urls_metadata=None,
        _ssl=None,
        description=None,
        content_created_date=None,
        content_updated_date=None,
        **kwargs,
    ):
        """
        Asynchronous function to update a record in indexd.

        Args:
             guid: string
                 - record id
             body: json/dictionary format
                 - index record information that needs to be updated.
                 - can not update size or hash, use new version for that
        """
        async with aiohttp.ClientSession() as session:
            updatable_attrs = {
                "file_name": file_name,
                "urls": urls,
                "version": version,
                "metadata": metadata,
                "acl": acl,
                "authz": authz,
                "urls_metadata": urls_metadata,
                "description": description,
                "content_created_date": content_created_date,
                "content_updated_date": content_updated_date,
            }
            record = await self.async_get_record(guid)
            revision = record.get("rev")

            for key, value in updatable_attrs.items():
                if value is not None:
                    record[key] = value

            del record["created_date"]
            del record["rev"]
            del record["updated_date"]
            del record["version"]
            del record["uploader"]
            del record["form"]
            del record["urls_metadata"]
            del record["baseid"]
            del record["size"]
            del record["hashes"]
            del record["did"]

            logging.info(f"PUT-ing record: {record}")

            # aiohttp only allows basic auth with their built in auth, so we
            # need to manually add JWT auth header
            headers = {"Authorization": self.client.auth._get_auth_value()}

            async with session.put(
                f"{self.client.url}/index/{guid}?rev={revision}",
                json=record,
                headers=headers,
                ssl=_ssl,
            ) as response:
                assert response.status == 200, await response.json()
                response = await response.json()

        return response

    ### Delete Requests

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def delete_record(self, guid):
        """

        Delete an entry from the index

        Args:
            guid: string
                - record id

        Returns: Nothing

        """
        rec = self.client.get(guid)
        if rec:
            rec.delete()
        return rec

    ### Query Requests

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def query_urls(self, pattern):
        """

        Query all record URLs for given pattern

        Args:
            pattern (str): pattern to match against indexd urls

        Returns:
            List[records]: indexd records with urls matching pattern
        """
        response = self.client._get(f"/_query/urls/q?include={pattern}")
        raise_for_status_and_print_error(response)
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    async def async_query_urls(self, pattern, _ssl=None):
        """
        Asynchronous function to query urls from indexd.

        Args:
            pattern (str): pattern to match against indexd urls

        Returns:
            List[records]: indexd records with urls matching pattern
        """
        url = f"{self.client.url}/_query/urls/q?include={pattern}"
        async with aiohttp.ClientSession() as session:
            logging.debug(f"request: {url}")
            async with session.get(url, ssl=_ssl) as response:
                raise_for_status_and_print_error(response)
                response = await response.json()

        return response

    ## Mint GUID Requests

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_valid_guids(self, count=None):
        """
        Get a list of valid GUIDs without indexing
        Args:
            count (int): number of GUIDs to request
        Returns:
            List[str]: list of valid indexd GUIDs
        """
        url = "/guid/mint"
        if count:
            url += f"?count={count}"

        response = self.client._get(url)
        response.raise_for_status()
        return response.json().get("guids", [])

    @backoff.on_exception(backoff.expo, Exception, **DEFAULT_BACKOFF_SETTINGS)
    def get_guids_prefix(self):
        """
        Get the prefix for GUIDs if there is one
        Returns:
            str: prefix for this instance
        """
        response = self.client._get("/guid/prefix")
        response.raise_for_status()
        return response.json().get("prefix")


def _print_func_name(function):
    return "{}.{}".format(function.__module__, function.__name__)


def _print_kwargs(kwargs):
    return ", ".join("{}={}".format(k, repr(v)) for k, v in list(kwargs.items()))
