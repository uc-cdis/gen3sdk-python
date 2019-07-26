import json
import requests
from urllib.parse import urljoin

class Gen3Error(Exception):
    pass

class Gen3UserError(Gen3Error):
    pass

def handle_error(resp):
    if 400 <= resp.status_code < 600:
        try:
            json = resp.json()
            resp.reason = json["error"]
        except KeyError:
            pass
        finally:
            resp.raise_for_status()

def json_dumps(data):
    return ({k: v for (k, v) in data.items() if v is not None})

class record_schema:
    ''' A class to contain a the record schema

        Schema:
            hashes (dict): {hash type: hash value,}
                eg ``hashes={'md5': ab167e49d25b488939b1ede42752458b'}``
            size (int): file size metadata associated with a given uuid
            did (str): provide a UUID for the new indexd to be made
            urls (list): list of URLs where you can download the UUID
            acl (list): access control list
            authz (list): list of RBAC strings
            file_name (str): name of the file associated with a given UUID
            metadata (dict): additional key value metadata for this entry
            urls_metadata (dict): metadata attached to each url
            baseid (str): optional baseid to group with previous entries versions
            version (str): entry version string
    '''

    def __init__(self, rdata):
        '''
            rdata is a dictionary of the record data
        '''
        if all (k in rdata for k in ['hashes', 'size']) \
                  and isinstance(rdata['hashes'], dict):
            self.acl = rdata.get('acl', None)
            self.authz = rdata.get('authz', None)
            self.baseid = rdata.get('baseid', None)
            self.did = rdata.get('did', None)
            self.file_name = rdata.get('file_name', None)
            self.form = rdata.get('form', 'object')
            self.hashes = rdata['hashes']
            self.metadata = rdata.get('metadata',None)
            self.size = rdata['size']
            self.uploader = rdata.get('uploader', None)
            self.urls = rdata.get('urls', [])
            self.urls_metadata = rdata.get('urls_metadata', None)
            self.version = rdata.get('version', None)
        else:
            raise Gen3UserError("record_schema must be initialized with a" \
                                    " dictionary containing at least 'hashes'(dict)" \
                                " and 'size'(int)")

    def to_json(self):
        '''
            Make the schema into json
        '''
        return json_dumps({
            'acl': self.acl,
            'authz': self.authz,
            'baseid': self.baseid,
            'did': self.did,
            'file_name': self.file_name,
            'form': self.form,
            'hashes': self.hashes,
            'metadata': self.metadata,
            'size': self.size,
            'uploader': self.uploader,
            'urls': self.urls,
            'urls_metadata': self.metadata,
            'version': self.version
        })

# ------------------------------------------------------------------------------

class Gen3Index:
    """

    A class for interacting with the Gen3 Index services.
    Supports
    Supports

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

    def __init__(self, endpoint, auth_provider):
        self._auth = auth_provider
        self._endpoint = (endpoint + '/index/')

    def make_url(self, *path):
        # Creates a full url
        return urljoin(self._endpoint, '/'.join(path))

### Get Requests

    def get_status(self):
        ''' Return if indexd is healthy or not

        '''
        response = self._get('_status')
        if response.status_code == 200:
            return response, "Indexd is Healthy!"
        else:
            return response, "Something is Wrong!"

    def get_version(self):
        ''' Return the version of indexd

        '''
        return self._get('_version')

    def get_stats(self):
        ''' Return basic info about the records in indexd

        '''
        return self._get('_stats')

    def global_get(self, guid):
        ''' Get the metadata associated with the given id, alias, or
            distributed identifier

        Args:
             guid: string
                 - record id

        '''
        return self._get(f'{guid}')

    def get_urls(self, **params):
        ''' Get a list of urls that match query params

        Args:
            size: integer
                - object size
            hash: string
                - hashes specified as algorithm:value
            ids: string
                - comma delimited ids

        '''
        return self._get('index/urls', params=params) 


    def get_index(self):
        ''' Get a list of all records

        '''
        return self._get('index/') 

    def get_record(self, guid):
        ''' Get the metadata associated with a given id

        '''
        return self._get(f'/index/{guid}')

    def get_latestversion(self, guid, has_version='true'):
        ''' Get the metadata of the latest index record version associated
            with the given id

        Args:
            guid: string
                - record id
            has_version: optional boolean string
                - filter by latest doc that has the version value populated

        '''
        return self._get(f'/index/{guid}/latest', params=has_version)

    def get_versions(self, guid):
        ''' Get the metadata of index record version associated with the
            given id

        Args:
            guid: string
                - record id

        '''
        return self._get(f'/index/{guid}/versions')


### Post Requests

    def add_record(self, body):
        ''' Create a new record and add it to the index

        Args:
            body: json/dictionary format
                - Metadata object that needs to be added to the store.
                  Providing size and at least one hash is necessary and
                  sufficient.

        return value: 
            { 
                "did": "string",
                "baseid": "string",
                "rev": "string"
            }
        '''
        rec_data = record_schema(body)
        return self._post(
            'index',
            auth=self._auth,
            json=rec_data.to_json())

    def create_blank(self, json):
        ''' Create a blank record

        Args:
            json - json in the format:
                    {
                        'uploader': type(string)
                        'file_name': type(string) (optional*)
                      }

        '''
        return self._post('index/blank', auth=self._auth, json=json)


    def add_new_version(self, guid, body):
        ''' Add new version for the document associated to the provided uuid

            - Since data content is immutable, when you want to change the
              size or hash, a new index document with a new uuid needs to be
              created as its new version. That uuid is returned in the did
              field of the response. The old index document is not deleted.

        Args:
            guid: string
                - record id
            body: json/dictionary format
                - Metadata object that needs to be added to the store.
                  Providing size and at least one hash is necessary and
                  sufficient. Note: it is a good idea to add a version 
                  number of description

        '''
        rec_data = record_schema(body)
        return self._post(
            f'index/{guid}',
            auth=self._auth,
            json=rec_data.to_json())

    def get_record_bulk(self, body):
        ''' Get a list of documents given a list of dids

        Args:
            body: list
                 - a list of record ids

        '''
        return self._post('bulk/documents', auth=self._auth, json=body) 

### Put Requests

    def update_blank(self, guid, rev, body):
        ''' Update only hashes and size for a blank index

        Args:
             guid: string
                  - record id
             rev: string
                - data revision - simple consistency mechanism
             body: json/dictionary format
                 - index record that needs to be updated.
                   *Size and hashes are required fields.

        '''
        rec_data = record_schema(body)
        p = {'rev': rev}
        return self._put(
                f'index/blank/{guid}',
                params=p,
                auth=self._auth,
                json=rec_data.to_json()) 

    def update_record(self, guid, rev, body):
        ''' Update an existing entry in the index

        Args:
             guid: string
                 - record id
            rev: string
                 - data revision - simple consistency mechanism
             body: json/dictionary format
                 - index record information that needs to be updated.
                 
            the body schema is:
            {'acl': {'items': {'type': 'string'}, 'type': 'array'},
                    'authz': {'items': {'type': 'string'},
                              'type': 'array'},
                    'file_name': {'type': ['string', 'null']},
                    'metadata': {'type': 'object'},
                    'uploader': {'type': ['string', 'null']},
                    'urls': {'items': {'type': 'string'},
                             'type': 'array'},
                    'urls_metadata': {'type': 'object'},
                    'version': {'type': ['string', 'null']}}

        return value: 
            { 
                "did": "string",
                "baseid": "string",
                "rev": "string"
            }

        '''
        p = {'rev': rev}
        return self._put(
                f'index/{guid}',
                params=p,
                auth=self._auth,
                json=body)

### Delete Requests

    def delete_record(self, guid, rev):
        ''' Delete an entry from the index

        Args:
            guid: string
                 - record id
            rev: string
                 - data revision - simple consistency mechanism

        '''
        p = {'rev': rev}
        return self._delete(f'index/{guid}', auth=self._auth, params=p)

### Useful Helper functions for http requests
    def _get(self, *path, **kwargs):
        resp = requests.get(self.make_url(*path), **kwargs)
        handle_error(resp)
        return resp

    def _post(self, *path, **kwargs):
        resp = requests.post(self.make_url(*path), **kwargs)
        handle_error(resp)
        return resp

    def _put(self, *path, **kwargs):
        resp = requests.put(self.make_url(*path), **kwargs)
        handle_error(resp)
        return resp

    def _delete(self, *path, **kwargs):
        resp = requests.delete(self.make_url(*path), **kwargs)
        handle_error(resp)
        return resp
