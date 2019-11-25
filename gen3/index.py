import requests
import indexclient.client as client

import urllib.parse


class Gen3Index:
    """

    A class for interacting with the Gen3 Index services.

    Args:
        endpoint (str): The URL of the data commons.
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Index class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... sub = Gen3Submission(endpoint, auth)

    """

    def __init__(self, endpoint, auth_provider=None, service_location="index"):
        endpoint = endpoint.strip("/")
        if not endpoint.endswith(service_location):
            endpoint += "/" + service_location
        endpoint += "/"
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

    def get_version(self):
        """

        Return the version of indexd

        """
        response = self.client._get("_version")
        response.raise_for_status()
        return response.json()

    def get_stats(self):
        """

        Return basic info about the records in indexd

        """
        response = self.client._get("_stats")
        response.raise_for_status()
        return response.json()

    def get_all_records(self, limit=None, paginate=False, start=None):
        """

        Get a list of all records

        """
        all_records = []
        url = "index/"

        if limit:
            url += f"?limit={limit}"

        response = self.client._get(url)
        response.raise_for_status()

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
                response.raise_for_status()

                records = response.json().get("records")
                all_records.extend(records)

                if records:
                    start_did = response.json().get("records")[-1].get("did")

        return all_records

    def get_records_on_page(self, limit=None, page=None):
        """

        Get a list of all records given the page and page size limit

        """
        all_records = []
        params = {}
        url = "index/"

        if limit is not None:
            params["limit"] = limit

        if page is not None:
            params["page"] = page

        query = urllib.parse.urlencode(params)

        response = self.client._get(url + "?" + query)
        response.raise_for_status()

        return response.json().get("records")

    def get(self, guid, no_dist=False):
        """

        Get the metadata associated with the given id, alias, or
        distributed identifier

        Args:
             guid: string
                 - record id
             no_dist: boolean
                - *optional* Specify if we want distributed search or not

        """
        rec = self.client.global_get(guid, no_dist)
        return rec.to_json()

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

    def get_record(self, guid):
        """

        Get the metadata associated with a given id

        """
        rec = self.client.get(guid)
        return rec.to_json()

    def get_with_params(self, params=None):
        """

        Return a document object corresponding to the supplied parameters, such
        as ``{'hashes': {'md5': '...'}, 'size': '...', 'metadata': {'file_state': '...'}}``.

            - need to include all the hashes in the request
            - index client like signpost or indexd will need to handle the
              query param `'hash': 'hash_type:hash'`

        """
        rec = self.client.get_with_params(params)
        return rec.to_json()

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
        return rec.to_json()

    def get_versions(self, guid):
        """

        Get the metadata of index record version associated with the
        given id

        Args:
            guid: string
                - record id

        """
        response = self.client._get(f"/index/{guid}/versions")
        response.raise_for_status()
        versions = response.json()

        return [r for _, r in versions.items()]

    ### Post Requests

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
            authz (str): RBAC string
            file_name (str): name of the file associated with a given UUID
            metadata (dict): additional key value metadata for this entry
            urls_metadata (dict): metadata attached to each url
            baseid (str): optional baseid to group with previous entries versions
            version (str): entry version string
        Returns:
            Document: json representation of an entry in indexd

        """
        rec = self.client.create(
            hashes,
            size,
            did,
            urls,
            file_name,
            metadata,
            baseid,
            acl,
            urls_metadata,
            version,
            authz,
        )
        return rec.to_json()

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
        response.raise_for_status()
        rec = response.json()

        return self.get_record(rec["did"])

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
    ):
        """

        Add new version for the document associated to the provided uuid

        - Since data content is immutable, when you want to change the
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
        response.raise_for_status()
        rec = response.json()

        if rec and "did" in rec:
            return self.get_record(rec["did"])
        return None

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

    def update_blank(self, guid, rev, hashes, size):
        """

        Update only hashes and size for a blank index

        Args:
            guid (string): record id
            rev (string): data revision - simple consistency mechanism
            hashes (dict): {hash type: hash value,}
                eg ``hashes={'md5': ab167e49d25b488939b1ede42752458b'}``
            size (int): file size metadata associated with a given uuid

        """
        p = {"rev": rev}
        json = {"hashes": hashes, "size": size}
        response = self.client._put(
            "index/blank/",
            guid,
            headers={"content-type": "application/json"},
            params=p,
            auth=self.client.auth,
            data=client.json_dumps(json),
        )
        response.raise_for_status()
        rec = response.json()

        return self.get_record(rec["did"])

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
        }
        rec = self.client.get(guid)
        for k, v in updatable_attrs.items():
            if v:
                exec(f"rec.{k} = v")
        rec.patch()
        return rec.to_json()

    ### Delete Requests

    def delete_record(self, guid):
        """

        Delete an entry from the index

        Args:
            guid: string
                 - record id

        Returns: Nothing

        """
        rec = self.client.get(guid)
        rec.delete()
        return rec
