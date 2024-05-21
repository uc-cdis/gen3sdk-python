"""
Module for downloading and listing data from external repositories.

    Examples:

        See docs/howto/drsDownloading.md for more details

"""
import json
import os
from pathlib import Path
from typing import Any, Dict, List

import requests
from cdislogging import get_logger

from gen3.auth import Gen3AuthError
from gen3.tools.download.drs_download import DownloadStatus, wts_get_token

REQUIRED_EXTERNAL_FILE_METADATA_KEYS = ["external_oidc_idp", "file_retriever"]

logger = get_logger("__name__", log_level="debug")


def download_files_from_metadata(
    hostname: str, auth, metadata_file, retrievers, download_path="."
) -> Dict[str, Any]:
    """
    Download data from an external repository using
    external-file metadata and a retreiver function.

    Parses the metadata for 'external_file_metadata' list
    with IDP and retriever fields.

    JSON does not allow for storing references to functions. We store
    a string for 'file_retriever' value and then map the string
    to a function.

    Example metadata:
    "gen3_discovery: {
        ...,
        "external_file_metadata": [
            {
            "external_oidc_idp": "qdr-keycloak",
            "file_retriever": "QDR",
            "study_id": "QDR_study_01"
            },
            {
            "external_oidc_idp": "qdr-keycloak",
            "file_retriever": "QDR",
            "file_id": "QDR_file_02"
            },
        ],
        ...,
    }

    Corresponding example retrievers mapping:
    { "QDR": get_syracuse_qdr_files }

    Calls the appropriate retriever function to download data.

    Args:
        hostname (str): hostname of Gen3 commons to use for WTS
        auth: Gen3 Auth instance for WTS
        metadata_file (str): path to metadata json file
        retrievers (dict): map of function name string to retreiver function reference.
    Returns:
        dictionary of download status for each external_file_metadata object
    """

    # check for empty retrievers
    if retrievers == {}:
        logger.critical(f"ERROR: 'retrievers' specification is empty {retrievers}")
        return None

    # # get access token for WTS commons
    # try:
    #     server_access_token = auth.get_access_token()
    # except Gen3AuthError:
    #     logger.critical(f"Unable to authenticate your credentials with {hostname}")
    #     return None

    # verify the metadata_file path is valid
    if not os.path.isfile(metadata_file):
        logger.critical(f"ERROR: cannot find metadata json file: {metadata_file}")
        return None

    # read metadata
    metadata = load_metadata(Path(metadata_file))

    # get list of external_file_metadata objects
    logger.debug(f"Metadata = {metadata}")
    external_file_metadata = extract_external_file_metadata(metadata)
    # TODO: Handle empty value - log error and return
    logger.debug(f"External file metadata={external_file_metadata}")
    if external_file_metadata == None:
        logger.critical(
            f"ERROR: cannot find external_file_metadata value in metadata: {metadata_file}"
        )
        return None

    download_status = pull_files(
        wts_server_name=hostname,
        auth=auth,
        external_file_metadata=external_file_metadata,
        retrievers=retrievers,
        download_path=download_path,
    )

    for key in download_status:
        logger.info(download_status[key])

    return download_status


def pull_files(
    wts_server_name: str,
    auth,
    external_file_metadata,
    retrievers,
    download_path,
) -> Dict[str, Any]:
    """
    Pull data files from external repo with external function.

    Args:
        wts_server_name (str): hostname of WTS server
        auth: Gen3Auth object for access to WTS
        external_file_metadata (dict):
        retrievers (dict):
        download_path: path to write data files
    Returns:
        list of download status for each external_file_metadata object, None if errors
    """
    completed = {}

    # loop over external file objects
    metadata_count = 0
    for file_metadata in external_file_metadata:
        # create an identifier and DownloadStatus to store in 'completed' list.
        if file_metadata.get("study_id"):
            filename = file_metadata.get("study_id")
        elif file_metadata.get("file_id"):
            filename = file_metadata.get("file_id")
        else:
            filename = f"{metadata_count}"
        logger.debug(f"Filename = {filename}")
        completed[filename] = DownloadStatus(filename=filename, status="pending")
        metadata_count += 1

        if not is_valid_external_file_metadata(file_metadata):
            logger.info(f"Skipping invalid external file metadata: {file_metadata}")
            completed[filename].status = "invalid file metadata"
            continue
        if not file_metadata.get("file_retriever") in retrievers:
            logger.info(f"Skipping, 'file_retriever' not found in retrievers")
            completed[filename].status = "invalid file retriever"
            continue
        logger.info("Valid metadata, ready to download")

        # get the idp for the calls to wts/external_idp
        idp = file_metadata.get("external_oidc_idp")

        # user should be logged in (say via portal) by now and have refresh tokens for idp
        # check that user is logged in to commons with WTS server
        connected_status = wts_connected(wts_server_name, auth.get_access_token())
        if connected_status != 200:
            logger.info(f"Skipping, cannot get verify connected at WTS server")
            completed[filename].status = "failed"
            continue

        # user should be already logged in (say via portal) and have refresh tokens for idp
        try:
            wts_access_token = auth.get_access_token()
        except Gen3AuthError:
            logger.critical(
                f"Unable to authenticate your credentials with {wts_server_name}"
            )
            completed[filename].status = "failed"
            continue
        idp_access_token = wts_get_token(
            hostname=wts_server_name, idp=idp, access_token=wts_access_token
        )
        if idp_access_token == None:
            logger.critical(f"Could not get token for idp {idp}")
            completed[filename].status = "failed"
            continue

        # retrieve files from external host
        success = retriever_manager(
            file_metadata, idp_access_token, retrievers, download_path
        )
        logger.info(f"Downloaded = {success}")
        if success:
            completed[filename].status = "downloaded"
        else:
            completed[filename].status = "failed"

    if completed == {}:
        return None
    else:
        return completed


def retriever_manager(file_metadata, token, retrievers, download_path) -> bool:
    """
    Retrieve data from external repo using idp, token, and retriever function.
    The manager will call the the file_retriever function specified in the file_metadata.

    Args
        file_metadata: the external_file_metadata with idp and retriever function
        token: the access token for the external repo
        retrievers: dict for mapping retriever function string in metadata to function reference
        download_path: path for output files.
    Returns:
        boolean: True if data was downloaded and written to file
    """

    download_success = False
    idp = file_metadata.get("external_oidc_idp")
    # use metadata and retrievers to determine which retriever to call
    retriever_function_string = file_metadata.get("file_retriever")
    logger.debug(f"File metadata = {file_metadata}")
    logger.debug(f"retriever string = {retriever_function_string}")

    retriever_function = retrievers.get(retriever_function_string)
    study_id = file_metadata.get("study_id")
    file_id = file_metadata.get("file_id")
    try:
        logger.info(
            f"Ready to retrieve data from {idp} with retriever {retriever_function.__name__}"
        )
        download_success = retriever_function(file_metadata, token, download_path)
    # This is handled further up in main function.
    except NameError:
        logger.critical("ERROR: Retriever function not in scope.")
    # handle errors specific to heal-sdk here
    #
    # other errors
    except Exception as e:
        logger.critical("Got an error")
        logger.critical(e)

    return download_success


# metadata and external_file_metadata methods


def extract_external_file_metadata(mds_object: dict) -> List:
    """
    Extract the list of dictionaries of external_file_metadata from an mds object.
    Return None if the external_file_metadata is not present.

    Args:
        mds_object (dict): the metadata from the input file
    Returns:
        List of external_file_metadata dictionaries
    """
    try:
        external_file_metadata = mds_object.get("gen3_discovery").get(
            "external_file_metadata", None
        )
    except AttributeError as e:
        logger.critical("ERROR: Missing key 'gen3_discovery' in metadata")
        logger.critical(e)
        return None
    return external_file_metadata


def is_valid_external_file_metadata(external_file_metadata: dict) -> bool:
    """
    Check that the external_file dict has the required keys:
    "external_oidc_idp" and "file_retriever".

    Args:
        external_file_metadata: dict
    Returns:
        boolean
    """
    isValid = True
    for key in REQUIRED_EXTERNAL_FILE_METADATA_KEYS:
        if key not in external_file_metadata:
            logger.info(f"Missing key in external file metadata: {key}")
            isValid = False

    return isValid


def load_metadata(path: Path) -> Dict:
    """
    Loads a metadata object from a json file.

    Args:
        path (Path): Path to metadata json file
    Returns:
        dict of metadata, None if failure
    """
    try:
        with open(path, "rt") as f:
            metadata = json.load(f)
            return metadata
    except FileNotFoundError as ex:
        logger.critical(f"Error file not found: {ex.filename}")
    except json.JSONDecodeError as ex:
        logger.critical(f"format of manifest file is valid JSON: {ex.msg}")
    return None


# Might not need this one.
def wts_connected(hostname: str, access_token: str) -> int:
    """
    Check that user is connected to commons with WTS service
    Args:
        hostname (str): Gen3 common's WTS service
        access_token: Gen3 Auth to use to with WTS

    Returns:
        response.status (int): 200 or 403
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Connection": "keep-alive",
        "Authorization": "bearer " + access_token,
    }

    try:
        url = f"https://{hostname}/wts/oauth2/connected"
        try:
            response = requests.get(url=url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            logger.critical(
                f"HTTP Error ({exc.response.status_code}): checking WTS connected: {exc.response.text}"
            )
            logger.critical(
                "Please make sure the target commons is connected on your profile page and that connection has not expired."
            )
            return None

        print("No errors")
        print(f"Status code: {response.status_code}")
        return response.status_code

    except Gen3AuthError:
        logger.critical(f"Unable to authenticate your credentials with {hostname}")
        return response.status
