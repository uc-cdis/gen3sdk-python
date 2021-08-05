import datetime
from dataclasses import dataclass
from json import load as json_load, JSONDecodeError
from pathlib import Path
from typing import List, Optional

import click
import requests
import humanfriendly
from cdiserrors import get_logger
from dataclasses_json import dataclass_json, LetterCase
from tqdm import tqdm

import os

from gen3.auth import Gen3Auth, Gen3AuthError
from gen3.auth import _handle_access_token_response

logger = get_logger("manifest", log_level="warning")


@dataclass_json(letter_case=LetterCase.SNAKE)
@dataclass
class Manifest:
    object_id: str
    file_size: int
    file_name: str
    md5sum: str
    commons_url: Optional[str] = None

    @staticmethod
    def load(path: Path):
        try:
            with open(path, "rt") as fin:
                data = json_load(fin)
                return Manifest.schema().load(data, many=True)
        except FileNotFoundError as ex:
            print(f"Error file not found: {ex.filename}")
        except JSONDecodeError as ex:
            print(f"format of manifest file is not correct: {ex.msg}")
        return None


@dataclass
class KnownDRSEndpoint:
    hostname: str
    available: bool
    idp: Optional[str] = None
    access_token: Optional[str] = None


def wts_external_oidc(hostname: str):
    oidc = {}
    try:
        response = requests.get(f"https://{hostname}/wts/external_oidc/")
        response.raise_for_status()

        data = response.json()
        for item in data["providers"]:
            oidc[item["base_url"].replace("https://", "")] = item

    except requests.exceptions.HTTPError as errh:
        logger.critical(f"Http Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logger.critical(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logger.critical(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logger.critical(f"Other Error: {err}")

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
        response = requests.get(url=url, headers=headers)
        response.raise_for_status()

        return _handle_access_token_response(response, "token")

    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return None


def download_file_from_url(url: str, filename: str) -> bool:
    response = requests.get(url, stream=True)

    if response.status_code != 200:
        logger.critical(f"cannot download {filename}. Skipping")
        return False

    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 8092  # 8K
    progress_bar = tqdm(
        desc=f"{filename : <45}",
        total=total_size_in_bytes,
        unit="iB",
        unit_scale=True,
        bar_format="{l_bar:45}{bar:75}{r_bar}{bar:-10b}",
    )
    with open(filename, "wb") as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        logger.critical(f"Error in downloading {filename}")
        return False
    return True


class ManifestDownloader:
    def __init__(self, manifest_items: List[Manifest], hostname: str, auth: Gen3Auth, output_dir: str):
        self.manifest: List[Manifest] = manifest_items
        self.known_hosts = {}
        self.hostname = hostname
        self.output_dir = output_dir
        if self.output_dir[-1] != "/":
                self.output_dir += "/"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.auth = auth
        self.access_token = self.auth.get_access_token()
        self.wts_endpoints = wts_external_oidc(hostname)
        self.cache_hosts_wts_tokens()

    def cache_hosts_wts_tokens(self):
        for entry in self.manifest:
            if entry.commons_url is None:
                continue
            commons_url = entry.commons_url
            if commons_url is not None:
                # handle hostname DRS
                if commons_url not in self.known_hosts:
                    # get token using WTS
                    # find end point in wts_endpoints
                    if commons_url not in self.wts_endpoints:
                        # log error and mark hostname as unavailable
                        self.known_hosts[commons_url] = KnownDRSEndpoint(
                            hostname=commons_url, available=False
                        )
                        logger.critical(f"Could not retrieve a token for {commons_url}; it is not available as a WTS endpoint.")
                    else:
                        token = wts_get_token(
                            hostname=self.hostname,
                            idp=self.wts_endpoints[commons_url]["idp"],
                            access_token=self.access_token,
                        )
                        if token is None:
                            self.known_hosts[commons_url] = KnownDRSEndpoint(
                                hostname=commons_url, available=False
                            )
                        else:
                            self.known_hosts[commons_url] = KnownDRSEndpoint(
                                hostname=commons_url,
                                idp=self.wts_endpoints[commons_url]["idp"],
                                access_token=token,
                                available=True,
                            )

    @staticmethod
    def authenticate_user(hostname: str, access_token: str) -> bool:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "bearer " + access_token,
        }

        response = requests.get(url=f"https://{hostname}/user/user", headers=headers)
        response.raise_for_status()
        if response.status_code == 200:
            return True

        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return False

    @staticmethod
    def get_drs_object_info(
        hostname: str, object_id: str, filename: str
    ) -> Optional[dict]:

        response = requests.get(f"https://{hostname}/ga4gh/drs/v1/objects/{object_id}")
        if response.status_code == 200:
            data = response.json()
            return data
        if response.status_code == 404:
            logger.critical(f"{filename} not found")
            return None

        logger.critical(f"Error finding filename: {filename}")
        return None

    @staticmethod
    def get_access_method(object_info: dict) -> List[str]:
        if object_info is None:
            logger.critical("no access methods defined for this file")
            return []
        return [x["access_id"] for x in object_info["access_methods"]]

    def get_user_auth(self, commons_url: str, access_token: str) -> Optional[List[str]]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "bearer " + access_token,
        }

        response = requests.get(url=f"https://{commons_url}/user/user", headers=headers)
        if response.status_code == 200:
            data = response.json()
            authz = data["authz"]
            return authz
        if response.status_code == 404:
            logger.critical(f"user info not found")
            return None

        return None

    @staticmethod
    def get_download_url_using_drs(
        hostname: str, object_id: str, access_method: str, access_token: str
    ) -> Optional[str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "bearer " + access_token,
        }
        response = requests.get(
            url=f"https://{hostname}/ga4gh/drs/v1/objects/{object_id}/access/{access_method}",
            headers=headers,
        )
        if response.status_code == 200:
            data = response.json()
            return data["url"]

        err_msg = response.json()
        logger.critical(f"{err_msg['msg']}")
        return None

    def get_download_url_using_fence(
        self, object_id: str, access_method: str, access_token: str
    ) -> Optional[str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "bearer " + access_token,
        }
        response = requests.get(
            url=f"https://{self.hostname}/data/download/{object_id}?protocol={access_method}",
            headers=headers,
        )
        if response.status_code == 200:
            data = response.json()
            return data["url"]
        logger.critical(f"Unable to get a Fence presigned URL for {object_id} from {self.hostname}; the response was {response.status_code}. {response.text}")
        return None

    @staticmethod
    def get_download_url(
        commons_url: str, object_id: str, file_name: str, access_token: str
    ) -> Optional[str]:
        object_info = ManifestDownloader.get_drs_object_info(
            commons_url, object_id, file_name
        )
        access_methods = ManifestDownloader.get_access_method(object_info)

        # use the first access method
        if len(access_methods) == 0:
            return None

        download_url = ManifestDownloader.get_download_url_using_drs(
            commons_url, object_id, access_methods[0], access_token
        )
        if download_url is None:
            logger.critical(
                f"Was unable to get the download url for {file_name}. Skipping "
            )
            return None

        return download_url

    @staticmethod
    def list_auth(hostname: str, authz: dict):
        print(f"Access for {hostname:}")
        if authz is not None and len(authz) > 0:
            for access, methods in authz.items():
                print(f"      {access}: {' '.join([x['method'] for x in methods])}")
        else:
            print("      No access")

    def user_access(self):
        for hostname in self.known_hosts.keys():

            if self.known_hosts[hostname].available is False:
                logger.critical(
                    f"Was unable to get user authorization from {hostname}."
                )
                continue
            access_token = self.known_hosts[hostname].access_token
            authz = self.get_user_auth(hostname, access_token)
            ManifestDownloader.list_auth(hostname, authz)

        authz = self.get_user_auth(self.hostname, self.access_token)
        ManifestDownloader.list_auth(self.hostname, authz)

    def download(self):
        """
        * load the manifest
        * use Gen3Auth to send request to host WTS
        * get list of external oidc
        * loop:
            * process manifest entry getting and cache WTS token as needed
            * use token and DRS endpoint to get pre-signed URL
            * download file
        """

        completed = {
            entry.file_name: {"status": "pending", "startTime": None, "endTime": None}
            for entry in self.manifest
        }

        for entry in self.manifest:
            commons_url = entry.commons_url
            if commons_url is not None:
                # handle hostname DRS
                # check to see if we have tokens
                if commons_url not in self.known_hosts:
                    logger.critical(
                        f"{commons_url} is not present in this commons remote user access. Skipping {entry.file_name}"
                    )
                    continue
                if self.known_hosts[commons_url].available is False:
                    logger.critical(
                        f"Was unable to get user authorization from {commons_url}. Skipping {entry.file_name}"
                    )
                    continue

                download_url = ManifestDownloader.get_download_url(
                    commons_url,
                    entry.object_id,
                    entry.file_name,
                    self.known_hosts[commons_url].access_token,
                )
            else:
                download_url = ManifestDownloader.get_download_url(
                    self.hostname, entry.object_id, entry.file_name, self.access_token
                )
            if download_url is None:
                completed[entry.file_name]["completed"] = "error"
                continue

            completed[entry.file_name]["startTime"] = datetime.datetime.now(
                datetime.timezone.utc
            )
            filepath = self.output_dir + entry.file_name
            res = download_file_from_url(url=download_url, filename=filepath)
            completed[entry.file_name].update(
                {
                    "completed": "downloaded" if res else "error",
                    "endTime": datetime.datetime.now(datetime.timezone.utc),
                }
            )


def _my_access(hostname, auth, infile):
    manifest_items = Manifest.load(Path(infile))
    if manifest_items is None:
        return

    downloader = ManifestDownloader(
        manifest_items=manifest_items,
        hostname=hostname,
        auth=auth,
    )
    downloader.user_access()

def _download(hostname, auth, infile, output_dir):
    manifest_items = Manifest.load(Path(infile))
    if manifest_items is None:
        return

    downloader = ManifestDownloader(
        manifest_items=manifest_items,
        hostname=hostname,
        auth=auth,
        output_dir=output_dir
    )
    downloader.download()


@click.command()
@click.argument("infile")
@click.pass_context
def my_access(ctx, infile: str):
    _my_access(ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile)


@click.command()
@click.argument("infile")
def listfiles(infile: str):
    manifest_items = Manifest.load(Path(infile))
    if manifest_items is None:
        return

    for item in manifest_items:
        print(
            f"{item.file_name : <45} {humanfriendly.format_size(item.file_size) :>12}"
        )


@click.command()
@click.argument("infile")
@click.argument("output_dir")
@click.pass_context
def download(ctx, infile: str, output_dir: str):
    _download(ctx.obj["endpoint"], ctx.obj["auth_factory"].get(), infile, output_dir)

@click.group()
def manifest():
    """Commands for downloading Gen3 manifests"""
    pass

manifest.add_command(my_access, name="access")
manifest.add_command(listfiles, name="list")
manifest.add_command(download, name="download")

# These functions are exposed to the SDK
def describe_access_to_files_in_workspace_manifest(hostname, auth, infile):
    _my_access(hostname, auth, infile)

def list_files_in_workspace_manifest(infile):
    listfiles(infile)

def download_files_in_workspace_manifest(hostname, auth, infile, output_dir):
    # TODO: make this async
    _download(hostname, auth, infile, output_dir)
