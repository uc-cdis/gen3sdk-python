"""
Module for downloading and listing JSON DRS manifest and DRS objects. The main classes in
this module for downloading DRS objects are DownloadManager and Manifest.

    Examples:
        This generates the Gen3Jobs class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> datafiles = Manifest.load('sample/manifest_1.json')
            downloadManager = DownloadManager("source.my_commons.org",
                              Gen3Auth(refresh_file="~.gen3/my_credentials.json"), datafiles)
            for i in datafiles:
                print(i)
            downloadManager.download(datafiles, ".")

        See docs/howto/drsDownloading.md for more details

"""


import re
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from json import load as json_load, loads as json_loads, JSONDecodeError
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import humanfriendly
import requests
import zipfile
from cdislogging import get_logger
from dataclasses_json import dataclass_json, LetterCase, Undefined
from dateutil import parser as date_parser
from tqdm import tqdm
from urllib.parse import urlparse

from gen3.auth import Gen3Auth, Gen3AuthError, decode_token
from gen3.auth import _handle_access_token_response
from gen3.tools.download.drs_resolvers import resolve_drs
from gen3.utils import remove_trailing_whitespace_and_slashes_in_url
from gen3.metadata import Gen3Metadata

DEFAULT_EXPIRE: timedelta = timedelta(hours=1)

# package formats we handle for unpacking
PACKAGE_EXTENSIONS = [".zip"]

logger = get_logger("__name__")


# Add undefined=Undefined.EXCLUDE here because we only cares if the input manifest has the minimal required metadata fields for data download, any extra metadata fields should be ignored and should not cause a failure
@dataclass_json(letter_case=LetterCase.SNAKE, undefined=Undefined.EXCLUDE)
@dataclass
class Manifest:
    """Data class representing a Gen3 JSON manifest typically exported from a Gen3 discovery page.

    The class is passed to the DownloadManager to download or list all of the files in the manifest.
    The Download manager will cache additional information (if available)

    Attributes:
        object_id(str): the DRS object id. This is the only attribute that needs to be defined
        file_size(Optional[int]): the filesize of the object, if contained in the manifest
        file_name(Optional[str]): the name of the file pointed to by the DRS object id
        md5sum(Optional[str]): the checksum of the object
        commons_url(Optional[str]): url of the indexd server to retrieve file/bundle from
    """

    object_id: str  # only required member
    file_size: Optional[int] = -1  # -1 indicated not set
    file_name: Optional[str] = None
    md5sum: Optional[str] = None
    commons_url: Optional[str] = None

    @staticmethod
    def load_manifest(path: Path):
        """Loads a json manifest"""
        with open(path, "rt") as fin:
            data = json_load(fin)
            return Manifest.schema().load(data, many=True)

    @staticmethod
    def create_object_list(manifest) -> List["Downloadable"]:
        """Create a list of Downloadable instances from the manifest

        Args:
            manifest(list): list of manifest objects
        Returns:
            List of Downloadable instances
        """
        results = []
        for entry in manifest:
            results.append(
                Downloadable(
                    object_id=entry.object_id,
                    hostname=remove_trailing_whitespace_and_slashes_in_url(
                        entry.commons_url
                    ),
                )
            )
        return results

    @staticmethod
    def load(filename: Path) -> Optional[List["Downloadable"]]:
        """
        Method to load a json manifest and return a list of Bownloadable object.
        This list is passed to the DownloadManager methods of download, and list

        Args:
            filename(Path): path to manifest file
        Returns:
            list of Downloadable objects if successfully opened/parsed None otherwise
        """
        try:
            manifest = Manifest.load_manifest(filename)
            return Manifest.create_object_list(manifest)
        except FileNotFoundError as ex:
            logger.critical(f"Error file not found: {ex.filename}")
        except JSONDecodeError as ex:
            logger.critical(f"format of manifest file is valid JSON: {ex.msg}")

        return None


@dataclass
class KnownDRSEndpoint:
    """
    Dataclass used internally by the DownloadManager class to cache hostnames and tokens
    for Gen3 commons possibly accessed using the Workspace Token Service (WTS). The endpoint is
    assumed to support DRS and therefore caches additional DRS information.

    Attributes:
        hostname (str): hostname of DRS server
        last_refresh_time (datetime): last time the token has been refreshed
        idp (Optional[str]): cached idp information
        access_token (Optional[str]): current bearer token initially None
        identifier (Optional[str]): DRS prefix (if used)
        use_wts (bool): if True use WTS to create tokens

    """

    hostname: str
    expire: datetime = None
    idp: Optional[str] = None
    access_token: Optional[str] = None
    identifier: Optional[str] = None
    use_wts: bool = True

    @property
    def available(self):
        """If endpoint has access token it is available"""
        return self.access_token is not None

    def expired(self) -> bool:
        """check if WTS token is older than the default expiration date

        If not using the WTS return false and use standard Gen3 Auth
        """
        if not self.use_wts:
            return False

        return datetime.now() > self.expire

    def renew_token(self, wts_server_name: str, server_access_token):
        """Gets a new token from the WTS and updates the token and refresh time

        Args:
            wts_server_name (str): hostname of WTS server
            server_access_token (str): token used to authenticate use of WTS

        """
        token = wts_get_token(
            hostname=wts_server_name,
            idp=self.idp,
            access_token=server_access_token,
        )
        token_info = decode_token(token)
        # TODO: this would break if user is trying to download object from different commons
        # keep BRH token and wts sparate
        self.access_token = token
        self.expire = datetime.fromtimestamp(token_info["exp"])


class DRSObjectType(str, Enum):
    """Enum defining the 3 possible DRS object types."""

    unknown = "unknown"
    object = "object"
    bundle = "bundle"


@dataclass
class Downloadable:
    """
    Class handling the information for a DRS object. The information is populated from the manifest or
    by retrieving the information from a DRS server.

    Attributes:
        object_id (str): DRS object id (REQUIRED)
        object_type (DRSObjectType): type of DRS object
        hostname (str): hostname of DRS object
        file_size (int): size in bytes
        file_name (str): name of file
        updated_time (datetime): timestamp of last update to file
        created_time (datetime): timestamp when file is created
        access_methods (List[Dict[str, Any]]): list of access methods (e.g. s3) for DRS object
        children (List[Downloadable]): list of child objects (in the case of DRS bundles)
        _manager (DownloadManager): manager for this Downloadable
    """

    object_id: str
    object_type: Optional[DRSObjectType] = DRSObjectType.unknown
    hostname: Optional[str] = None
    file_size: Optional[int] = -1
    file_name: Optional[str] = None
    updated_time: Optional[datetime] = None
    created_time: Optional[datetime] = None
    access_methods: List[Dict[str, Any]] = field(default_factory=list)
    children: List["Downloadable"] = field(default_factory=list)
    _manager = None

    def __str__(self):
        return (
            f'{self.file_name if self.file_name is not None else "not available" : >45}; '
            f"{humanfriendly.format_size(self.file_size) :>12}; "
            f'{self.hostname if self.hostname is not None else "not resolved"}; '
            f'{self.created_time.strftime("%m/%d/%Y, %H:%M:%S") if self.created_time is not None else "not available"}'
        )

    def __repr__(self):
        return (
            f'(Downloadable: {self.file_name if self.file_name is not None else "not available"}; '
            f"{humanfriendly.format_size(self.file_size)}; "
            f'{self.hostname if self.hostname is not None else "not resolved"}; '
            f'{self.created_time.strftime("%m/%d/%Y, %H:%M:%S") if self.created_time is not None else "not available"})'
        )

    def download(self):
        """calls the manager to download this object. Allows Downloadables to be self downloading"""
        self._manager.download([self])

    def pprint(self, indent: str = ""):
        """
        Pretty prints the object information. This is used for listing an object.
        In the case of a DRS bundle the child objects are listed similar to the linux tree command
        """

        from os import linesep

        res = self.__str__() + linesep
        child_indent = f"{indent}    "

        pos = -1
        for x in self.children:
            pos += 1
            if pos == len(self.children) - 1:
                res += f"{child_indent}└── {x.pprint(child_indent)}"
            else:
                res += f"{child_indent}├── {x.pprint(child_indent)}"
        return res


@dataclass
class DownloadStatus:
    """Stores the download status of objectIDs.

    The DataManager will return a list of DownloadStatus as a result of calling the download method

    Status is "pending" until it is downloaded or an error occurs.
    Attributes:
        filename (str): the name of the file to download
        status (str): status of file download initially "pending"
        start_time (Optional[datetime]): start time of download as datetime initially None
        end_time (Optional[datetime]): end time of download as datetime initially None
    """

    filename: str
    status: str = "pending"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

    def __str__(self):
        return (
            f'filename: {self.filename if self.filename is not None else "not available"}; '
            f"status: {self.status}; "
            f'start_time: {self.start_time.strftime("%m/%d/%Y, %H:%M:%S") if self.start_time is not None else "n/a"}; '
            f'end_time: {self.end_time.strftime("%m/%d/%Y, %H:%M:%S") if self.start_time is not None else "n/a"}'
        )

    def __repr__(self):
        return self.__str__()


def wts_external_oidc(hostname: str) -> Dict[str, Any]:
    """
    Get the external_oidc from a connected WTS. Will report if WTS service
    is missing. Note that in some cases this can be considered a warning not a error.

    Args:
        hostname (str): hostname to access the WTS endpoint

    Returns:
        dict containing the oidc information
    """
    oidc = {}
    try:
        response = requests.get(f"https://{hostname}/wts/external_oidc/")
        response.raise_for_status()
        data = response.json()
        if "providers" not in data:
            logger.warning(
                'cannot find "providers". Likely no WTS service running for this commons'
            )
            return oidc
        for item in data["providers"]:
            oidc[urlparse(item["base_url"]).netloc] = item

    except requests.exceptions.HTTPError as exc:
        logger.critical(
            f'HTTP Error ({exc.response.status_code}): {json_loads(exc.response.text).get("message", "")}'
        )
    except JSONDecodeError as ex:
        logger.warning(
            f"Unable to process WTS response. Likely no WTS service running on this commons. "
            f"Certain commands might fail."
        )

    return oidc


def wts_get_token(hostname: str, idp: str, access_token: str):
    """
    Gets a auth token from a Gen3 WTS server for the supplied idp
    Args:
        hostname (str): Gen3 common's WTS service
        idp: identity provider to use
        access_token: Gen3 Auth to use to with WTS

    Returns:
        Token for idp if successful, None if failure
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Connection": "keep-alive",
        "Authorization": "bearer " + access_token,
    }

    try:
        url = f"https://{hostname}/wts/token/?idp={idp}"
        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            logger.critical(
                f"HTTP Error ({exc.response.status_code}): getting WTS token: {exc.response.text}"
            )
            logger.critical(
                "Please make sure the target commons is connected on your profile page and that connection has not expired."
            )
            return None

        return _handle_access_token_response(response, "token")

    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return None


def get_drs_object_info(hostname: str, object_id: str) -> Optional[dict]:
    """
    Retrieves information for a DRS object residing on the hostname
    Args:
        hostname (str): hostname of DRS object
        object_id (str): DRS object id

    Returns:
        GAG4H DRS object information if sucessful otherwise None
    """
    try:
        response = requests.get(f"https://{hostname}/ga4gh/drs/v1/objects/{object_id}")
        response.raise_for_status()
        data = response.json()
        return data

    except requests.HTTPError as exc:
        if exc.response.status_code == 404:
            logger.critical(
                f"HTTP Error ({exc.response.status_code}): {object_id} not found at {hostname}"
            )
        else:
            logger.critical(
                f"HTTP Error ({exc.response.status_code}): accessing object: {object_id}"
            )
        return None
    except ConnectionError as exc:
        logger.critical(f"Connection Error {exc} when accessing object: {object_id}")
        return None


def extract_filename_from_object_info(object_info: dict) -> Optional[str]:
    """Extracts the filename from the object_info.

    if filename is in object_info use that, otherwise try to extract it from the one of the access methods.
    Returns filename if found, else return None

    Args
        object_info (dict): DRS object dictionary
    """
    if "name" in object_info and object_info["name"]:
        return object_info["name"]

    for access_method in object_info["access_methods"]:
        url = access_method["access_url"]["url"]
        parts = url.split("/")
        if parts:
            return parts[-1]

    return None


def get_access_methods(object_info: dict) -> List[str]:
    """
    Returns the DRS GA4GH access methods from the object_info.

    Args:
        object_info (dict): dict of GA4GH DRS Object information

    Returns:
        List of access methods
    """
    if object_info is None:
        logger.critical("no access methods defined for this file")
        return []
    return object_info["access_methods"]


def get_drs_object_type(object_info: dict) -> DRSObjectType:
    """From the object info determine the type of object.

    Args:
        object_info (dict): DRS object dictionary

    Returns:
        type of object: either bundle or object
    """
    if "form" in object_info:
        if object_info["form"] is None:
            return DRSObjectType.object
        return DRSObjectType(object_info["form"])

    if "contents" in object_info and len(object_info["contents"]) > 0:
        return DRSObjectType.bundle
    else:
        return DRSObjectType.object


def get_drs_object_timestamp(s: Optional[str]) -> Optional[datetime]:
    """returns the timestamp in datetime if not none otherwise returns None

    Args:
        s (Optional[str]): string to parse
    Returns:
        datetime if not None
    """
    return date_parser.parse(s) if s is not None else None


def add_drs_object_info(info: Downloadable) -> bool:
    """
    Given a downloader object fill in the required fields
    from the resolved hostname.
    In the case of a bundle, try to resolve all object_ids contained in the bundle including other objects and bundles.

    Args:
        info (Downloadable): Downloadable to add information to
    Returns:
         True if object is valid and resolvable.
    """
    if info.hostname is None:
        return False

    object_info = get_drs_object_info(info.hostname, info.object_id)
    if (object_info) is None:
        return False

    # Get common information we want
    info.file_name = extract_filename_from_object_info(object_info)
    info.file_size = object_info.get("size", -1)
    info.updated_time = get_drs_object_timestamp(object_info.get("updated_time", None))
    info.created_time = get_drs_object_timestamp(object_info.get("created_time", None))

    info.object_type = get_drs_object_type(object_info)
    if info.object_type == DRSObjectType.object:
        info.access_methods = get_access_methods(object_info)
        return True
    else:  # a bundle,get everything else
        for item in object_info["contents"]:
            child_id = item.get("id", None)
            if child_id is None:
                continue
            child_object = Downloadable(hostname=info.hostname, object_id=child_id)
            add_drs_object_info(child_object)
            info.children.append(child_object)

    return True


class InvisibleProgress:
    """
    Invisible progress bar which stubs a tqdm progress bar
    """

    def update(self, value):  # pragma: no cover
        pass


def download_file_from_url(
    url: str, filename: Path, show_progress: bool = True
) -> bool:
    """
    Downloads a file using the URL. The URL is a pre-signed url created by the download manager
    from the access method of the DRS object.

    Args:
        url (str): URL to download from
        filename (str): name of the file to write data to
        show_progress (bool): show a progress bar (default)

    Returns:
        True if object has been downloaded
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

    except requests.exceptions.Timeout:
        logger.critical(f"Was unable to get the download url: {url}. Timeout Error.")
        return False
    except requests.exceptions.HTTPError as exc:
        logger.critical(
            f"HTTP Error ({exc.response.status_code}): downloading file from {url}"
        )
        return False

    total_size_in_bytes = int(response.headers.get("content-length", 0))

    if total_size_in_bytes == 0:
        logger.critical(f"content-length is 0 and it should not be")
        return False

    total_downloaded = 0
    block_size = 8092  # 8K blocks might want to tune this.

    progress_bar = (
        tqdm(
            desc=f"{str(filename) : <45}",
            total=total_size_in_bytes,
            unit="iB",
            unit_scale=True,
            bar_format="{l_bar:45}{bar:35}{r_bar}{bar:-10b}",
        )
        if show_progress
        else InvisibleProgress()
    )

    # if the file name contains '/', create subdirectories and download there
    ensure_dirpath_exists(Path(os.path.dirname(filename)))

    try:
        with open(filename, "wb") as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                total_downloaded += len(data)
                file.write(data)
    except IOError as ex:
        logger.critical(f"IOError opening {filename} for writing: {ex}")
        return False

    if total_downloaded != total_size_in_bytes:
        logger.critical(
            f"Error in downloading {filename}: expected {total_size_in_bytes} bytes, downloaded {total_downloaded} bytes"
        )
        return False
    return True


def unpackage_object(filepath: str):
    # allowed formats are set in PACKAGE_EXTENSIONS
    with zipfile.ZipFile(filepath, "r") as package:
        package.extractall(os.path.dirname(filepath))


def parse_drs_identifier(drs_candidate: str) -> Tuple[str, str, str]:
    """
    Parses a DRS identifier to extract a hostname in the case of hostname based DRS
    otherwise it look for a DRS compact identifier.

    If neither one is recognized return an empty string and a type of 'unknown'
    Note: The regex expressions used to extract hostname or identifier has a potential for
    a false positive.

    Args:
        drs_candidate (str): a drs object identifier

    Returns:
        Tuple (str): tuple of hostname/drs prefix, guid, string: one of "hostname", "compact", "unknown"
    """
    # determine if hostname or compact identifier or unknown
    drs_regex = r"drs://([A-Za-z0-9\.\-\~]+)/([A-Za-z0-9\.\-\_\~\/]+)"
    # either a drs prefix:

    matches = re.findall(drs_regex, drs_candidate, re.UNICODE)

    if len(matches) == 1:  # this could be a hostname DRS id
        hostname_regex = (
            r"^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*"
            r"([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$"
        )
        hostname_matches = re.findall(hostname_regex, matches[0][0], re.UNICODE)
        if len(hostname_matches) == 1:
            return matches[0][0], matches[0][1], "hostname"
    # possible compact rep
    compact_regex = r"([A-Za-z0-9\.\-\~]+)/([A-Za-z0-9\.\-\_\~\/]+)"
    matches = re.findall(compact_regex, drs_candidate, re.UNICODE)
    if len(matches) == 1 and len(matches[0]) == 2:
        return matches[0][0], matches[0][1], "compact"

    # can't figure out a this identifier
    return "", "", "unknown"


def resolve_drs_hostname_from_id(
    object_id: str, resolved_drs_prefix_cache: dict, mds_url: str
) -> Optional[Tuple[str, str, str]]:
    """Resolves and returns a DRS identifier
    The resolved_drs_prefix_cache is updated if needed and is a potential side effect of this
    call
    Args:
        object_id (str): DRS object id to resolve
        resolved_drs_prefix_cache (dict) : cache of resolved DRS prefixes
        mds_url (str): the URL for the Gen3 Aggregate MDS to use to help resolved DRS hostname

    Returns:
         the hostname of the DRS server if resolved, otherwise it returns None
    """
    hostname = None

    prefix, identifier, identifier_type = parse_drs_identifier(object_id)
    if identifier_type == "hostname":
        return prefix, identifier, identifier_type
    if identifier_type == "compact":
        if prefix not in resolved_drs_prefix_cache:
            hostname = resolve_drs(prefix, object_id, metadata_service_url=mds_url)
            if hostname is not None:
                resolved_drs_prefix_cache[prefix] = hostname
        else:
            hostname = resolved_drs_prefix_cache[prefix]

    return hostname, identifier, identifier_type


def resolve_objects_drs_hostname(
    object_ids: List[Downloadable], resolved_drs_prefix_cache: dict, mds_url: str, endpoint: str
) -> None:
    """Given a list of object_ids go through list and resolve + cache any unknown hosts

    Args:
        object_ids (List[Downloadable]): list of object to resolve
        resolved_drs_prefix_cache (dict): cache of resolved DRS prefixes
        mds_url (str): Gen3 metadata service to resolve DRS prefixes
        hostname (str): Hostname to main Gen3 environment
    """
    for entry in object_ids:
        if endpoint is not None and entry.hostname is None:
            entry.hostname = endpoint
        if entry.hostname is None:
            # if resolution fails the entry hostname will still be None
            entry.hostname, nid, drs_type = resolve_drs_hostname_from_id(
                entry.object_id, resolved_drs_prefix_cache, mds_url
            )
            if (
                drs_type == "hostname"
            ):  # drs_type is a hostname so object id will be the GUID
                entry.object_id = nid


def ensure_dirpath_exists(path: Path) -> Path:
    """Utility to create a directory if missing.
    Returns the path so that the call can be inlined in a another call

    Args:
        path (Path): path to create
    Returns
        path of created directory
    """
    assert path
    out_path: Path = path

    if not out_path.exists():
        out_path.mkdir(parents=True, exist_ok=True)

    return out_path


def get_download_url_using_drs(
    drs_hostname: str, object_id: str, access_method: str, access_token: str
) -> Optional[str]:
    """
    Returns the presigned URL for a DRS object, from a DRS hostname, via the access method
    Args:
        drs_hostname (str): hostname of DRS server
        object_id (str): DRS object id
        access_method (str): access method to use
        access_token (str): access token to DRS server

    Returns:
        presigned url to object
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "bearer " + access_token,
    }
    try:
        response = requests.get(
            url=f"https://{drs_hostname}/ga4gh/drs/v1/objects/{object_id}/access/{access_method}",
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("url", None)

    except requests.exceptions.Timeout:
        logger.critical(f"Was unable to download: {object_id}. Timeout Error.")
    except requests.exceptions.HTTPError as exc:
        logger.critical(
            f"HTTP Error ({exc.response.status_code}) when requesting download url from {access_method}"
        )
    return None


def get_user_auth(commons_url: str, access_token: str) -> Optional[List[str]]:
    """
    Retrieves a user's authz for the commons based on the access token.
    Any error will be logged and None is returned

    Args:
        commons_url (str): hostname of Gen3 indexd
        access_token (str): user's auth token

    Returns:
        The authz object from the user endpoint
    """
    """

    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "bearer " + access_token,
    }

    try:
        response = requests.get(url=f"https://{commons_url}/user/user", headers=headers)
        response.raise_for_status()
        data = response.json()
        authz = data["authz"]
        return authz
    except requests.exceptions.HTTPError as exc:
        logger.critical(f"HTTP Error ({exc.response.status_code}): getting user access")

    return None


def list_auth(hostname: str, authz: dict):
    """
    Prints the authz for a DRS hostname
    Args:
        hostname (str): hostname to list access
        authz (str): dictionary of authz stringts
    """
    print(
        f"───────────────────────────────────────────────────────────────────────────────────────────────────────"
    )
    print(f"Access for {hostname}:")
    if authz is not None and len(authz) > 0:
        for access, methods in authz.items():
            print(
                f"      {access : <55}: {' '.join(dict.fromkeys(x['method'] for x in methods).keys()):>40}"
            )
    else:
        print("      No access")


class DownloadManager:
    """
    Class to assist in downloading a list of Downloadable object which at a minimum is a json manifest
    of DRS object ids. The methods of interest are download and user_access.
    """

    def __init__(
        self,
        hostname: str,
        auth: Gen3Auth,
        download_list: List[Downloadable],
        show_progress: bool = False,
        endpoint: str = None
    ):
        """
        Initialize the DownloadManager so that is ready to start downloading.
        Note the downloadable objects are required so that all tokens are available
        to support the download.

        Args:
            hostname (str): Gen3 commons home commons
            auth (Gen3Auth) : Gen3 authentication
            download_list (List[Downloadable]): list of objects to download
        """

        self.hostname = hostname
        self.endpoint = endpoint
        self.access_token = auth.get_access_token()
        self.metadata = Gen3Metadata(auth)
        self.wts_endpoints = wts_external_oidc(hostname)
        self.resolved_compact_drs = {}
        # add COMMONS host as a DRSEndpoint as it does not use the WTS
        self.known_hosts = {
            self.hostname: KnownDRSEndpoint(
                hostname=self.hostname,
                access_token=self.access_token,
                use_wts=False,
                expire=datetime.fromtimestamp(decode_token(self.access_token)["exp"]),
            )
        }
        self.download_list = download_list
        self.resolve_objects(self.download_list, show_progress)

    def resolve_objects(self, object_list: List[Downloadable], show_progress: bool):
        """
        Given an Downloadable object list, resolve the DRS hostnames and update each Downloadable

        Args:
            object_list (List[Downloadable]): list of Downloadable objects to resolve
        """
        resolve_objects_drs_hostname(
            object_list,
            self.resolved_compact_drs,
            mds_url=f"http://{self.hostname}/mds/aggregate/info",
            endpoint=self.endpoint
        )
        progress_bar = (
            tqdm(desc=f"Resolving objects", total=len(object_list))
            if show_progress
            else InvisibleProgress()
        )
        for entry in object_list:
            add_drs_object_info(entry)
            # sugar to allow download objects to self download
            entry._manager = self
            progress_bar.update(1)

    def cache_hosts_wts_tokens(self, object_list):
        """
        Using the list of DRS host obtain a WTS token for all DRS hosts in the list. It's is possible
        """
        # create two sets: one of the know WTS host and the other of the host in the manifest
        wts_endpoint_set = set(self.wts_endpoints.keys())
        object_id_hostnames = {
            x.hostname for x in object_list if x.hostname is not None
        }

        drs_in_wts = wts_endpoint_set.intersection(
            object_id_hostnames
        )  # all DRS host in WTS
        drs_not_in_wts = object_id_hostnames.difference(
            wts_endpoint_set
        )  # all DRS host not in WTS
        for drs_hostname in drs_in_wts:
            endpoint = KnownDRSEndpoint(
                hostname=drs_hostname,
                idp=self.wts_endpoints[drs_hostname]["idp"],
            )
            endpoint.renew_token(self.hostname, self.access_token)
            self.known_hosts[drs_hostname] = endpoint
        for drs_hostname in drs_not_in_wts:
            # if we already know the host then we don't need to reset the host
            if drs_hostname in self.known_hosts:
                continue
            # mark hostname as unavailable
            self.known_hosts[drs_hostname] = KnownDRSEndpoint(
                hostname=drs_hostname,
            )
            logger.critical(
                f"Could not retrieve a token for {drs_hostname}: it is not available as a WTS endpoint."
            )

    def get_fresh_token(self, drs_hostname: str) -> Optional[str]:
        """Will return and/or refresh and return a WTS token if hostname is known otherwise returns None.
        Args:
            drs_hostname (str): hostname to get token for

        Returns:
            access token if successful otherwise None
        """
        if drs_hostname not in self.known_hosts:
            logger.critical(f"Could not find {drs_hostname} in cache.")
            return None
        if self.known_hosts[drs_hostname].available:
            if not self.known_hosts[drs_hostname].expired():
                return self.known_hosts[drs_hostname].access_token
            else:
                # update the token
                self.known_hosts[drs_hostname].renew_token(
                    self.hostname, self.access_token
                )
                return self.known_hosts[drs_hostname].access_token

        return None

    def download(
        self,
        object_list: List[Downloadable],
        save_directory: str = ".",
        show_progress: bool = False,
        unpack_packages: bool = True,
        delete_unpacked_packages: bool = False,
    ) -> Dict[str, Any]:
        """
        Downloads objects to the directory or current working directory.
        The input is an list of Downloadable object created by loading a manifest
        using the Manifest class or a call to Manifest.load(...

        The download manager will download each file in the manifest, in the
        case of errors they are logged and it continues.

        The return value is a list of DownloadStatus object, detailing the results
        of the download.

        Args:
            object_list (List[Downloadable]):
            save_directory (str): directory to save to (will be created)
            show_progress (bool): show a download progress bar
            unpack_packages (bool): set to False to disable the unpacking of downloaded packages
            delete_unpacked_packages (bool): set to True to delete package files after unpacking them

        Returns:
            List of DownloadStatus objects for each object id in object_list
        """

        self.cache_hosts_wts_tokens(object_list)
        output_dir = Path(save_directory)

        completed = {
            entry.object_id: DownloadStatus(filename=entry.file_name)
            for entry in object_list
        }

        for entry in object_list:
            # handle bundles first
            if entry.object_type is DRSObjectType.bundle:
                # append the filename to the directory path and
                child_dir = Path(save_directory, entry.file_name)
                # call download with the children object list
                child_status = self.download(
                    entry.children,
                    child_dir,
                    show_progress,
                    unpack_packages,
                    delete_unpacked_packages,
                )
                # when complete, append the return status
                completed[entry.object_id] = child_status
                continue

            if entry.hostname is None:
                logger.critical(
                    f"{entry.hostname} was not resolved, skipping {entry.object_id}."
                    f"Skipping {entry.file_name}"
                )
                completed[entry.object_id].status = "error (resolving DRS host)"
                continue

            # check to see if we have tokens
            if entry.hostname not in self.known_hosts:
                logger.critical(
                    f"{entry.hostname} is not present in this commons remote user access."
                    f"Skipping {entry.file_name}"
                )
                completed[entry.object_id].status = "error (resolving DRS host)"
                continue
            if self.known_hosts[entry.hostname].available is False:
                logger.critical(
                    f"Was unable to get user authorization from {entry.hostname}. Skipping {entry.file_name}"
                )
                completed[entry.object_id].status = "error (no auth)"
                continue

            drs_hostname = entry.hostname
            access_token = self.get_fresh_token(drs_hostname)

            if access_token is None:
                logger.critical(
                    f"No access token defined for {entry.object_id}. Skipping"
                )
                completed[entry.object_id].status = "error (no access token)"
                continue
            # TODO refine the selection of access_method
            if len(entry.access_methods) == 0:
                logger.critical(
                    f"No access methods defined for {entry.object_id}. Skipping"
                )
                completed[entry.object_id].status = "error (no access methods)"
                continue
            access_method = entry.access_methods[0]["access_id"]

            download_url = get_download_url_using_drs(
                drs_hostname,
                entry.object_id,
                access_method,
                access_token,
            )

            if download_url is None:
                completed[entry.object_id].status = "error"
                continue

            completed[entry.object_id].start_time = datetime.now(timezone.utc)
            filepath = output_dir.joinpath(entry.file_name)
            res = download_file_from_url(
                url=download_url, filename=filepath, show_progress=show_progress
            )

            # check if the file is a package; if so, unpack it in place
            ext = os.path.splitext(entry.file_name)[-1]
            if unpack_packages and ext in PACKAGE_EXTENSIONS:
                try:
                    mds_entry = self.metadata.get(entry.object_id)
                except Exception:
                    mds_entry = {}  # no MDS or object not in MDS
                    logger.debug(
                        f"{entry.file_name} is not a package and will not be expanded"
                    )

                # if the metadata type is "package", then unpack
                if mds_entry.get("type") == "package":
                    try:
                        unpackage_object(filepath)
                    except Exception as e:
                        logger.critical(
                            f"{entry.file_name} had an issue while being unpackaged: {e}"
                        )
                        res = False

                    if delete_unpacked_packages:
                        filepath.unlink()
            if res:
                completed[entry.object_id].status = "downloaded"
                logger.debug(
                    f"object {entry.object_id} has been successfully downloaded."
                )
            else:
                completed[entry.object_id].status = "error"
                logger.debug(f"object {entry.object_id} has failed to be downloaded.")
            completed[entry.object_id].end_time = datetime.now(timezone.utc)

        return completed

    def user_access(self):
        """
        List the user's access permissions on each host needed to download
        DRS objects in the manifest. A useful way to determine if access permissions
        are one reason a download failed.

        Returns:
            list of authz for each DRS host
        """
        results = {}
        self.cache_hosts_wts_tokens(self.download_list)
        for hostname in self.known_hosts.keys():
            if self.known_hosts[hostname].available is False:
                logger.critical(
                    f"Was unable to get user authorization from {hostname}."
                )
                continue
            access_token = self.known_hosts[hostname].access_token
            authz = get_user_auth(hostname, access_token)
            results[hostname] = authz

        return results


def _download(
    hostname,
    auth,
    infile,
    output_dir=".",
    show_progress=False,
    unpack_packages=True,
    delete_unpacked_packages=False,
) -> Optional[Dict[str, Any]]:
    """
    A convenience function used to download a json manifest.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth (str): Gen3 Auth instance
        infile (str): manifest file
        output_dir: directory to save downloaded files to
        show_progress: show progress bar
        unpack_packages (bool): set to False to disable the unpacking of downloaded packages
        delete_unpacked_packages (bool): set to True to delete package files after unpacking them

    Returns:
        List of DownloadStatus objects for each object id in object_list
    """
    object_list = Manifest.load(Path(infile))
    if object_list is None:
        logger.critical(f"Error loading {infile}")
        return None
    try:
        auth.get_access_token()
    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return

    downloader = DownloadManager(
        hostname=hostname,
        auth=auth,
        download_list=object_list,
        show_progress=show_progress,
        endpoint=hostname
    )

    out_dir_path = ensure_dirpath_exists(Path(output_dir))
    return downloader.download(
        object_list,
        str(out_dir_path),
        show_progress=show_progress,
        unpack_packages=unpack_packages,
        delete_unpacked_packages=delete_unpacked_packages,
    )


def _download_obj(
    hostname,
    auth,
    object_ids,
    output_dir=".",
    show_progress=False,
    unpack_packages=True,
    delete_unpacked_packages=False,
) -> Optional[Dict[str, Any]]:
    """
    A convenience function used to download a single DRS object.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth: Gen3 Auth instance
        object_ids (List[str]): DRS object id
        output_dir: directory to save downloaded files to
        show_progress: show progress bar
        unpack_packages (bool): set to False to disable the unpacking of downloaded packages
        delete_unpacked_packages (bool): set to True to delete package files after unpacking them

    Returns:
        List of DownloadStatus objects for the DRS object
    """
    try:
        auth.get_access_token()
    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return None

    object_list = [Downloadable(object_id=object_id) for object_id in object_ids]
    downloader = DownloadManager(
        hostname=hostname,
        auth=auth,
        download_list=object_list,
        show_progress=show_progress,
        endpoint=hostname
    )

    out_dir_path = ensure_dirpath_exists(Path(output_dir))
    return downloader.download(
        object_list,
        str(out_dir_path),
        show_progress=show_progress,
        unpack_packages=unpack_packages,
        delete_unpacked_packages=delete_unpacked_packages,
    )


def _listfiles(hostname, auth, infile: str) -> bool:
    """
    A wrapper function used by the cli to list files in a manifest.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth: Gen3 Auth instance
        infile (str): manifest file

    Returns:
        True if successfully listed
    """
    object_list = Manifest.load(Path(infile))
    if object_list is None:
        return False

    try:
        auth.get_access_token()
    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return False
    except requests.exceptions.RequestException as ex:
        logger.critical(
            f"Unable to authenticate your credentials with {hostname}: {str(ex)}"
        )
        return False

    DownloadManager(
        hostname=hostname, auth=auth, download_list=object_list, show_progress=True
    )

    for x in object_list:
        print(x.pprint())

    return True


def _list_object(hostname, auth, object_id: str) -> bool:
    """
    Lists a DRS object.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth: Gen3 Auth instance
        object_id (str): DRS object

    Returns:
        True if successfully listed
    """
    try:
        auth.get_access_token()
    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return False
    except requests.exceptions.RequestException as ex:
        logger.critical(
            f"Unable to authenticate your credentials with {hostname}: {ex}"
        )
        return False

    object_list = [Downloadable(object_id=object_id)]
    DownloadManager(
        hostname=hostname, auth=auth, download_list=object_list, show_progress=False
    )

    for x in object_list:
        print(x.pprint())

    return True


def _list_access(hostname, auth, infile: str) -> bool:
    """
    A convenience function to list a users access for all DRS hostname in a manifest.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth: Gen3 Auth instance
        infile (str): manifest file

    Returns:
        True if successfully listed
    """
    object_list = Manifest.load(Path(infile))
    if object_list is None:
        return False

    try:
        auth.get_access_token()
    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return False
    except requests.exceptions.RequestException as ex:
        logger.critical(
            f"Unable to authenticate your credentials with {hostname}: {ex}"
        )
        return False

    download = DownloadManager(
        hostname=hostname, auth=auth, download_list=object_list, show_progress=False
    )
    access = download.user_access()
    for h, access in access.items():
        list_auth(h, access)

    return True


# These functions are exposed to the SDK's cli under the drs-pull subcommand
def list_files_in_drs_manifest(hostname, auth, infile: str) -> bool:
    """
    A wrapper function used by the cli to list files in a manifest.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth: Gen3 Auth instance
        infile (str): manifest file

    Returns:
        True if successfully listed
    """
    return _listfiles(hostname, auth, infile)


def list_drs_object(hostname, auth, object_id: str) -> bool:
    """
    A convenience function used to list a DRS object.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth: Gen3 Auth instance
        object_id (str): DRS object

    Returns:
        True if successfully listed
    """
    return _list_object(hostname, auth, object_id)  # pragma: no cover


def download_files_in_drs_manifest(
    hostname,
    auth,
    infile,
    output_dir,
    show_progress=True,
    unpack_packages=True,
    delete_unpacked_packages=False,
) -> None:
    """
    A convenience function used to download a json manifest.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth (str): Gen3 Auth instance
        infile (str): manifest file
        output_dir: directory to save downloaded files to
        unpack_packages (bool): set to False to disable the unpacking of downloaded packages
        delete_unpacked_packages (bool): set to True to delete package files after unpacking them

    Returns:
    """
    _download(
        hostname,
        auth,
        infile,
        output_dir,
        show_progress,
        unpack_packages,
        delete_unpacked_packages,
    )


def download_drs_objects(
    hostname,
    auth,
    object_ids,
    output_dir,
    show_progress=True,
    unpack_packages=True,
    delete_unpacked_packages=False,
) -> None:
    """
    A convenience function used to download a single DRS object.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth: Gen3 Auth instance
        object_ids (List[str]): DRS object ids
        output_dir: directory to save downloaded files to
        unpack_packages (bool): set to False to disable the unpacking of downloaded packages
        delete_unpacked_packages (bool): set to True to delete package files after unpacking them

    Returns:
        List of DownloadStatus objects for the DRS object
    """
    return _download_obj(
        hostname,
        auth,
        object_ids,
        output_dir,
        show_progress,
        unpack_packages,
        delete_unpacked_packages,
    )


def list_access_in_drs_manifest(hostname, auth, infile) -> bool:
    """
    A convenience function to list a users access for all DRS hostname in a manifest.
    Args:
        hostname (str): hostname of Gen3 commons to use for access and WTS
        auth: Gen3 Auth instance
        infile (str): manifest file

    Returns:
        True if successfully listed
    """
    return _list_access(hostname, auth, infile)
