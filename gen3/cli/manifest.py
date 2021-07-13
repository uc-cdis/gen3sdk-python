from dataclasses import dataclass
from dataclasses_json import dataclass_json, LetterCase
from typing import Any, Dict, List, Optional
from pathlib import Path
from json import load as json_load, JSONDecodeError
import click
import requests
import datetime
from cdiserrors import get_logger
from gen3.auth import _handle_access_token_response
from tqdm import tqdm

from gen3.auth import Gen3Auth, Gen3AuthError

logger = get_logger("__name__", log_level="warning")


@dataclass_json(letter_case=LetterCase.SNAKE)
@dataclass
class Manifest:
    object_id: str
    file_size: int
    file_name: str
    md5sum: str
    commons_url: Optional[str] = None

    @staticmethod
    def load(path: Path) -> Optional[List[Dict[str, Any]]]:
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

    except Gen3AuthError as ex:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return None


def download_file_from_url(url: str, filename: str, md5sum: str) -> bool:
    response = requests.get(url, stream=True)

    if response.status_code != 200:
        logger.critical(f"cannot download {filename}. Skipping")
        return False

    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 8092  # 8K
    progress_bar = tqdm(
        desc=f"{filename : >45}",
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
    def __init__(self, manifest: List[Dict[Any, Any]], hostname: str, auth: Gen3Auth):
        self.manifest = manifest
        self.known_hosts = {}
        self.hostname = hostname
        self.auth = auth
        self.access_token = self.auth.get_access_token()
        self.wts_endpoints = wts_external_oidc(hostname)
        self.cache_hosts_wts_tokens()

    def cache_hosts_wts_tokens(self):
        for entry in self.manifest:
            if entry.commons_url == None:
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
                    else:
                        token = wts_get_token(
                            hostname=self.hostname,
                            idp=self.wts_endpoints[commons_url]["idp"],
                            access_token=self.access_token,
                        )
                        if token == None:
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
    def authenicate_user(hostname: str, access_token: str) -> bool:
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

        return None

    @staticmethod
    def get_download_url_using_fence(
        object_id: str, access_method: str, access_token: str
    ) -> Optional[str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "bearer " + access_token,
        }
        response = requests.get(
            url=f"https://{hostname}/data/download/{object_id}?protocol={access_method}",
            headers=headers,
        )
        if response.status_code == 200:
            data = response.json()
            return data["url"]

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
        if download_url == None:
            logger.critical(
                f"Was unable to get the download url for {file_name}. Skipping "
            )
            return None

        return download_url

    def download(self):
        """
        * load the manifest
        * use Gen3Auth to send request to host WTS
        * get list of external oidc
        * loop:
            * process manifest entry getting and cache WTS token as needed
            * use token and DRS enpoint to get presigned URL
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
            if download_url == None:
                completed[entry.file_name]["completed"] = "error"
                continue

            completed[entry.file_name]["startTime"] = datetime.datetime.now(
                datetime.timezone.utc
            )
            res = download_file_from_url(
                url=download_url, filename=entry.file_name, md5sum=entry.md5sum
            )
            completed[entry.file_name].update(
                {
                    "completed": "downloaded" if res else "error",
                    "endTime": datetime.datetime.now(datetime.timezone.utc),
                }
            )


@click.command()
@click.argument("manifest")
@click.pass_context
def download(ctx, manifest: str):
    m = Manifest.load(manifest)
    if m is None:
        return
    downloader = ManifestDownloader(
        manifest=m, hostname=ctx.obj["endpoint"], auth=ctx.obj["auth_factory"].get()
    )
    downloader.download()


@click.group()
def manifest():
    """Commands for downloading Gen3 manifests"""
    pass


manifest.add_command(download, name="download")
