import re
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from json import load as json_load, loads as json_loads, JSONDecodeError
from logging import StreamHandler, Formatter, INFO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import humanfriendly
import requests
from cdiserrors import get_logger
from dataclasses_json import dataclass_json, LetterCase
from dateutil import parser as date_parser
from tqdm import tqdm

from gen3.auth import Gen3Auth, Gen3AuthError
from gen3.auth import _handle_access_token_response
from gen3.tools.download.drs_resolvers import resolve_drs

# Setup custom logger to create a console/friendly message
logger = get_logger("download", log_level="warning")
console = StreamHandler()
console.setLevel(INFO)
formatter = Formatter("%(message)s")
console.setFormatter(formatter)
logger.addHandler(console)

DEFAULT_EXPIRE: timedelta = timedelta(hours=1)

class Downloadable:
    pass


@dataclass_json(letter_case=LetterCase.SNAKE)
@dataclass
class Manifest:
    object_id: str  # only required member
    file_size: Optional[int] = -1
    file_name: Optional[str] = None
    md5sum: Optional[str] = None
    commons_url: Optional[str] = None

    @staticmethod
    def load_manifest(path: Path):
        with open(path, "rt") as fin:
            data = json_load(fin)
            return Manifest.schema().load(data, many=True)

    @staticmethod
    def create_object_list(manifest) -> List[Downloadable]:
        results = []
        for entry in manifest:
            results.append(
                Downloadable(object_id=entry.object_id, hostname=entry.commons_url)
            )
        return results

    @staticmethod
    def load(filename: Path) -> Optional[List[Downloadable]]:
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
    hostname: str
    last_refresh_time: datetime = None
    idp: Optional[str] = None
    access_token: Optional[str] = None
    identifier: Optional[str] = None
    use_wts: bool = True

    @property
    def available(self):
        return self.access_token is not None

    def expired(self):
        if not self.use_wts:
            return False

        res = (datetime.now(timezone.utc) - self.last_refresh_time) > DEFAULT_EXPIRE
        return res

    def renew_token(self, wts_server_name: str, server_access_token):
        token = wts_get_token(
            hostname=wts_server_name,
            idp=self.idp,
            access_token=server_access_token,
        )
        self.access_token = token
        self.last_refresh_time = datetime.now(timezone.utc)


class DRSObjectType(str, Enum):
    unknown = "unknown"
    object = "object"
    bundle = "bundle"


@dataclass
class Downloadable:
    object_id: str
    object_type: Optional[DRSObjectType] = DRSObjectType.unknown
    hostname: Optional[str] = None
    file_size: Optional[int] = -1
    file_name: Optional[str] = None
    updated_time: Optional[datetime] = None
    created_time: Optional[datetime] = None
    access_methods: List[Dict[str, Any]] = field(default_factory=list)
    children: List[Downloadable] = field(default_factory=list)
    _manager = None

    def __str__(self):
        return (
            f'{self.file_name if self.file_name is not None else "not available" : >45} '
            f"{humanfriendly.format_size(self.file_size) :>12} "
            f'{self.hostname if self.hostname is not None else "not resolved"} '
            f'{self.created_time.strftime("%m/%d/%Y, %H:%M:%S") if self.created_time is not None else "not available"}'
        )

    def __repr__(self):
        return (
            f'(Downloadable: {self.file_name if self.file_name is not None else "not available"} '
            f"{humanfriendly.format_size(self.file_size)} "
            f'{self.hostname if self.hostname is not None else "not resolved"} '
            f'{self.created_time.strftime("%m/%d/%Y, %H:%M:%S") if self.created_time is not None else "not available"})'
        )

    def download(self):
        self._manager.download([self])


@dataclass
class DownloadStatus:
    filename: str
    status: str = "pending"
    startTime: Optional[datetime] = None
    endTime: Optional[datetime] = None

    def __str__(self):
        return (
            f'filename: {self.filename if self.filename is not None else "not available"} '
            f"status: {self.status} "
            f'startTime: {self.startTime.strftime("%m/%d/%Y, %H:%M:%S") if self.startTime is not None else "n/a"} '
            f'endTime: {self.endTime.strftime("%m/%d/%Y, %H:%M:%S") if self.startTime is not None else "n/a"}'
        )

    def __repr__(self):
        return (
            f'filename: {self.filename if self.filename is not None else "not available"} '
            f"status: {self.status} "
            f'startTime: {self.startTime.strftime("%m/%d/%Y, %H:%M:%S") if self.startTime is not None else "n/a"} '
            f'endTime: {self.endTime.strftime("%m/%d/%Y, %H:%M:%S") if self.startTime is not None else "n/a"}'
        )


def wts_external_oidc(hostname: str) -> Dict[str, Any]:
    oidc = {}
    try:
        response = requests.get(f"https://{hostname}/wts/external_oidc/")
        response.raise_for_status()
        data = response.json()
        for item in data["providers"]:
            oidc[item["base_url"].replace("https://", "")] = item

    except requests.exceptions.HTTPError as ex:
        logger.critical(
            f'{ex.response.status_code}: {json_loads(ex.response.text).get("message", "")}'
        )
        raise

    return oidc


def wts_get_token(hostname: str, idp: str, access_token: str):
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
        except requests.exceptions.HTTPError as ex:
            logger.critical(
                f"HTTP Error getting WTS token: {ex.response.status_code}: {ex.response.text}"
            )
            return None

        return _handle_access_token_response(response, "token")

    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return None


def get_drs_object_info(hostname: str, object_id: str) -> Optional[dict]:
    try:
        response = requests.get(f"https://{hostname}/ga4gh/drs/v1/objects/{object_id}")
        response.raise_for_status()
        data = response.json()
        return data

    except requests.HTTPError as exc:
        if exc.response.status_code == 404:
            logger.critical(f"{object_id} not found at {hostname}")
        else:
            logger.critical(
                f"HTTPError {exc.response.status_code} when accessing object: {object_id}"
            )
        return None


def extract_filename_from_object_info(object_info: dict) -> Optional[str]:
    """
    Get the filename from the object_info:
    if name is object_info use that, otherwise try to extract it from the one of the access methods.
    Returns filename if found, else return None
    """
    if "name" in object_info and object_info["name"]:
        return object_info["name"]

    for access_method in object_info["access_methods"]:
        # TODO: add checks on dictionary path to 'url'
        #  and if the last item is truly a filename
        url = access_method["access_url"]["url"]
        parts = url.split("/")
        if parts:
            return parts[-1]

    return None


def get_access_methods(object_info: dict) -> List[str]:
    if object_info is None:
        logger.critical("no access methods defined for this file")
        return []
    return object_info["access_methods"]


def get_drs_object_type(object_info) -> DRSObjectType:
    """
    From the object info determine the type of object.
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
    return date_parser.parse(s) if s is not None else None


def add_drs_object_info(info: Downloadable) -> bool:
    """
    Given a downloader object fill in the required fields
    from the resolved hostname. Returns True if object is valid
    and resolvable.
    In the case of a bundle, try to resolve all object_ids contained
    in the bundle including other object and bundles
    """
    if info.hostname is None:
        return False

    if (object_info := get_drs_object_info(info.hostname, info.object_id)) is None:
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
        # one assumption is that bundles are not federated, i.e. a DRS object cannot
        # refer to an DRS object on another ga4gh server
        # TODO confirm with spec.
        for item in object_info["contents"]:
            child_id = item.get("id", None)
            if child_id is None:
                continue
            child_object = Downloadable(hostname=info.hostname, object_id=child_id)
            add_drs_object_info(child_object)
            info.children.append(child_object)

    return True


class DummyProgress:
    def update(self, value):  # pragma: no cover
        pass

    def close(self):  # pragma: no cover
        pass


def download_file_from_url(url: str, filename: Path, showProgress: bool = True) -> bool:
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

    except requests.exceptions.Timeout:
        raise
    except requests.exceptions.HTTPError as exc:
        logger.critical(f"Error download file from url: {exc.response.status_code} ")
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
        if showProgress
        else DummyProgress()
    )
    try:
        with open(filename, "wb") as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                total_downloaded += len(data)
                file.write(data)
    except IOError as ex:
        logger.critical(f"IOError {ex} opening {filename} for writing.")
        return False

    progress_bar.close()
    if total_downloaded != total_size_in_bytes:
        logger.critical(
            f"Error in downloading {filename} expected {total_size_in_bytes} got {total_downloaded}"
        )
        return False
    return True


def parse_drs_identifier(drs_candidate: str) -> Tuple[str, str, str]:
    """
    Parses a DRS identifier to extract a hostname in the case of hostname based DRS
    a compact identifier.
    If neither one is recognized return an empty string and a type of 'unknown'
    Note: The regex expressions used to extract hostname or identifier has a potential for
    a false positive.
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

    # can't figure it out
    return "", "", "unknown"


def resolve_drs_hostname_from_id(
    object_id: str, resolved_drs_prefix_cache: dict, mds_url: str
) -> Optional[Tuple[str, str, str]]:
    """
    resolves and returns a DRS identifier, including calling a DRS server to
    resolve and compact id.
    Returns the hostname of the DRS server if resolved, otherwise it returns None

    The resolved_drs_prefix_cache is updated if needed and is a potential side effect of this
    call
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

        hostname = resolved_drs_prefix_cache[prefix]

    return hostname, identifier, identifier_type


def resolve_objects_drs_hostname_from_id(
    object_ids: List[Downloadable], resolved_drs_prefix_cache: dict,
    mds_url: str
) -> None:
    """
    Given a list of object_ids go through list and resolve + cache any unknown
    """

    for entry in object_ids:
        if entry.hostname is None:
            # if resolution fails the entry hostname will still be None
            entry.hostname, nid, idtype = resolve_drs_hostname_from_id(
                entry.object_id, resolved_drs_prefix_cache, mds_url
            )
            if idtype == "hostname":
                entry.object_id = nid


def ensure_dirpath_exists(path: Path) -> Path:
    """
    Utility to create a directory if missing
    """
    assert path
    out_path: Path = path

    if not out_path.exists():
        out_path.mkdir(parents=True, exist_ok=True)

    return out_path


def get_download_url_using_drs(
    drs_hostname: str, object_id: str, access_method: str, access_token: str
) -> Optional[str]:
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
        return data["url"]
    except requests.exceptions.Timeout:
        raise
    except requests.exceptions.HTTPError as exc:
        err_msg = exc.response.json()
        logger.critical(
            f"HTTP Error getting download url {exc.response.status_code} {err_msg}"
        )
    return None


def get_download_url(
    hostname: str, object_id: str, access_method: str, access_token: str
) -> Optional[str]:
    try:
        download_url = get_download_url_using_drs(
            hostname, object_id, access_method, access_token
        )
        if download_url is None:
            logger.critical(
                f"Was unable to get the download url for {object_id} from {hostname}. Skipping."
            )
        return download_url
    except requests.exceptions.Timeout:
        logger.critical(f"Was unable to get the download url for {object_id}. Timeout.")
    return None


def get_user_auth(commons_url: str, access_token: str) -> Optional[List[str]]:
    """
    Get a user authz for the commons based on the access token.
    return the authz object from the user endpoint
    Any error will be logged and None is returned
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
        err_msg = exc.response.json()
        logger.critical(
            f"HTTP Error getting user access {exc.response.status_code} {err_msg}"
        )

    return None


def list_auth(hostname: str, authz: dict):
    print(f"Access for {hostname:}")
    if authz is not None and len(authz) > 0:
        for access, methods in authz.items():
            print(f"      {access}: {' '.join([x['method'] for x in methods])}")
    else:
        print("      No access")


class DownloadManager:
    """
    Class to assist in downloading a list of Downloadables which at a minimum is a json manifest
    of DRS object ids
    The parameters are:
        * hostname: commons to start from
        * auth: Gen3 authentication
        * list of objects to download
        * DRS resolution strategy
    """

    def __init__(
        self, hostname: str,
            auth: Gen3Auth,
            download_list: List[Downloadable],
    ):

        self.hostname = hostname
        self.access_token = auth.get_access_token()
        self.wts_endpoints = wts_external_oidc(hostname)
        self.resolved_compact_drs = {}
        # add COMMONS host as a DRSEndpoint which does not use the WTS
        self.known_hosts = {
            self.hostname: KnownDRSEndpoint(
                hostname=self.hostname,
                access_token=self.access_token,
                use_wts=False,
                last_refresh_time=datetime.now(timezone.utc),
            )
        }
        self.download_list = download_list
        self.resolve_objects(self.download_list)

    def resolve_objects(self, object_list: List[Downloadable]):
        resolve_objects_drs_hostname_from_id(object_list, self.resolved_compact_drs, f"http://{self.hostname}/mds/metadata")
        for entry in object_list:
            add_drs_object_info(entry)
            # sugar to allow download objects to self download
            entry._manager = self

    def cache_hosts_wts_tokens(self, object_list: List[Downloadable]):
        for entry in object_list:
            if entry.hostname is None:  # handle workspace manifest with common_url
                logger.warning(f"Hostname not defined for {entry.object_id}")
                continue
            drs_hostname = entry.hostname
            if drs_hostname is not None:
                # handle DRS
                if drs_hostname not in self.known_hosts:
                    if drs_hostname not in self.wts_endpoints:
                        # log error and mark hostname as unavailable
                        self.known_hosts[drs_hostname] = KnownDRSEndpoint(
                            hostname=drs_hostname,
                            last_refresh_time=datetime.now(timezone.utc),
                        )
                        logger.critical(
                            f"Could not retrieve a token for {drs_hostname}: it is not available as a WTS endpoint."
                        )
                    else:  # create a KnownDRSEndpoint and try to obtain a token from WTS
                        endpoint = KnownDRSEndpoint(
                            hostname=drs_hostname,
                            idp=self.wts_endpoints[drs_hostname]["idp"],
                        )
                        endpoint.renew_token(self.hostname, self.access_token)
                        self.known_hosts[drs_hostname] = endpoint

    def get_fresh_token(self, drs_hostname: str) -> Optional[str]:
        """
        Will return and or refresh and return a WTS token if hostname is known
        otherwise returns None.
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
        self, object_list: List[Downloadable], save_directory: str = "."
    ) -> Dict[str, Any]:
        """
        * load the object ids
        * use Gen3Auth to send request to host WTS
        * get list of external oidc
        * loop:
            * process manifest entry getting and cache WTS token as needed
            * use token and DRS endpoint to get pre-signed URL
            * download file

        returns: list of files requests and results.
        """

        self.cache_hosts_wts_tokens(object_list)
        output_dir = Path(save_directory)

        completed = {
            entry.object_id: DownloadStatus(filename=entry.file_name)
            for entry in object_list
        }

        for entry in object_list:
            drs_hostname = None
            access_token = None
            if entry.hostname is not None:
                # handle hostname DRS
                # check to see if we have tokens
                if entry.hostname not in self.known_hosts:
                    logger.critical(
                        f"{entry.hostname} is not present in this commons remote user access. "
                        f"Skipping {entry.file_name}"
                    )
                    continue
                if self.known_hosts[entry.hostname].available is False:
                    logger.critical(
                        f"Was unable to get user authorization from {entry.hostname}. Skipping {entry.file_name}"
                    )
                    continue

                drs_hostname = entry.hostname
                access_token = self.get_fresh_token(drs_hostname)

            if access_token is None:
                logger.critical(
                    f"No access token defined for {entry.object_id}. Skipping"
                )
                continue
            # TODO refine the selection of access_method
            if len(entry.access_methods) == 0:
                logger.critical(
                    f"No access methods defined for {entry.object_id}. Skipping"
                )
                continue
            access_method = entry.access_methods[0]["access_id"]

            download_url = get_download_url(
                drs_hostname,
                entry.object_id,
                access_method,
                access_token,
            )

            if download_url is None:
                completed[entry.object_id].status = "error"
                continue

            completed[entry.object_id].startTime = datetime.now(timezone.utc)
            filepath = output_dir.joinpath(entry.file_name)
            res = download_file_from_url(url=download_url, filename=filepath)
            completed[entry.object_id].status = "downloaded" if res else "error"
            completed[entry.object_id].endTime = datetime.now(timezone.utc)

        return completed

    def user_access(self):
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


def _download(hostname, auth, infile, output_dir=".") -> Optional[Dict[str, Any]]:
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
        hostname=hostname, auth=auth, download_list=object_list
    )

    out_dir_path = ensure_dirpath_exists(Path(output_dir))
    return downloader.download(object_list, out_dir_path)


def _download_obj(
    hostname, auth, object_id, output_dir="."
) -> Optional[Dict[str, Any]]:
    try:
        auth.get_access_token()
    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return None

    object_list = [Downloadable(object_id=object_id)]
    downloader = DownloadManager(
        hostname=hostname, auth=auth, download_list=object_list
    )

    out_dir_path = ensure_dirpath_exists(Path(output_dir))
    return downloader.download(object_list, out_dir_path)


def _listfiles(hostname, auth, infile: str) -> None:
    object_list = Manifest.load(Path(infile))
    if object_list is None:
        return

    try:
        auth.get_access_token()
    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return

    DownloadManager(hostname=hostname, auth=auth, download_list=object_list)

    for x in object_list:
        print(x)


def _list_access(hostname, auth, infile: str) -> None:
    object_list = Manifest.load(Path(infile))
    if object_list is None:
        return

    try:
        auth.get_access_token()
    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return

    download = DownloadManager(hostname=hostname, auth=auth, download_list=object_list)
    access = download.user_access()
    for h, authz in access.items():
        list_auth(h, authz)


# These functions are exposed to the SDK
def list_files_in_workspace_manifest(hostname, auth, infile: str) -> None:
    _listfiles(hostname, auth, infile)


def download_files_in_workspace_manifest(hostname, auth, infile, output_dir) -> None:
    _download(hostname, auth, infile, output_dir)


def download_drs_object(hostname, auth, object_id, output_dir) -> None:
    _download_obj(hostname, auth, object_id, output_dir)


def list_access_in_manifest(hostname, auth, infile) -> None:
    _list_access(hostname, auth, infile)
