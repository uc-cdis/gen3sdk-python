"""
Module for downloading and listing data from external repositories.

    Examples:

        See docs/howto/drsDownloading.md for more details

"""
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from cdislogging import get_logger

from gen3.auth import Gen3Auth

REQUIRED_EXTERNAL_FILE_METADATA_KEYS = ["external_oidc_idp", "file_retriever"]

logger = get_logger("__name__", log_level="debug")


def download_files_from_metadata(
    hostname: str,
    auth: Gen3Auth,
    metadata_file: str,
    retrievers: Dict,
    download_path: str = ".",
) -> Dict[str, Any]:
    """
    Download data from an external repository using
    external-file metadata and a retriever function.

    Parses the metadata for 'external_file_metadata' list
    with IDP and retriever fields. The 'external_file_metadata'
    must be nested under 'gen3_discovery'.

    JSON does not allow for storing references to functions. We store
    a string for 'file_retriever' value and then map the string
    to a function.

    Example metadata:
    "gen3_discovery: {
        ...,
        "external_file_metadata": [
            {
            "external_oidc_idp": "externaldata-keycloak",
            "file_retriever": "QDR",
            "study_id": "QDR_study_01"
            },
            {
            "external_oidc_idp": "externaldata-keycloak",
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
        auth: Gen3Auth instance for WTS
        metadata_file (str): path to external file metadata json file
        retrievers (dict): map of function name string to retreiver function reference.
    Returns:
        dictionary of DownloadStatus for external file objects, None if errors
    """

    if retrievers == {}:
        logger.critical(f"ERROR: 'retrievers' specification is empty {retrievers}")
        return None

    # verify the metadata_file path is valid
    if not os.path.isfile(metadata_file):
        logger.critical(f"ERROR: cannot find metadata json file: {metadata_file}")
        return None

    metadata = load_all_metadata(Path(metadata_file))
    logger.debug(f"Metadata = {metadata}")
    external_file_metadata = extract_external_file_metadata(metadata)
    logger.debug(f"External file metadata={external_file_metadata}")
    if external_file_metadata == None:
        logger.critical(
            f"ERROR: cannot find external_file_metadata value in metadata: {metadata_file}"
        )
        return None

    download_status = pull_files(
        wts_server_name=hostname,
        auth=auth,
        file_metadata_list=external_file_metadata,
        retrievers=retrievers,
        download_path=download_path,
    )

    if download_status == None:
        logger.critical("Download status is None")
    else:
        for key in download_status:
            logger.info(download_status[key])

    return download_status


def pull_files(
    wts_server_name: str,
    auth,
    file_metadata_list,
    retrievers,
    download_path,
) -> Dict[str, Any]:
    """
    Pull data files from external repo with external function.

    Args:
        wts_server_name (str): hostname of WTS server
        auth: Gen3Auth object for access to WTS
        file_metadata_list (list): list of external file metadata objects
        retrievers (dict):
        download_path: path to write data files
    Returns:
        dictionary of DownloadStatus for external file objects, None if errors
    """
    completed = {}

    metadata_grouped_by_retriever = check_data_and_group_by_retriever(
        file_metadata_list
    )

    if metadata_grouped_by_retriever == None:
        logger.critical(f"ERROR: Cannot group external metadata by retrievers")
        return None
    logger.debug(f"Grouped metadata = {metadata_grouped_by_retriever}")

    for key, val in metadata_grouped_by_retriever.items():
        retriever_function = retrievers.get(key)
        if retriever_function == None:
            logger.critical(f"Could not find retriever function for {key}")
            continue
        file_metadata_list = val
        download_status = None

        try:
            logger.info(
                f"Ready to retrieve data with retriever {retriever_function.__name__}"
            )
            download_status = retriever_function(
                wts_server_name, auth, file_metadata_list, download_path
            )
        except NameError:
            logger.critical("ERROR: Retriever function not in scope.")
        except Exception as e:
            logger.critical("Error from retriever function")
            logger.critical(e)

        if download_status:
            logger.info(f"Adding download_status = {download_status}")
            completed.update(download_status)

    if completed == {}:
        return None
    else:
        return completed


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
    'external_oidc_idp' and 'file_retriever'.

    Args:
        external_file_metadata: dict
    Returns:
        boolean
    """
    if not isinstance(external_file_metadata, dict):
        logger.info(f"File_metadata object is not a dict: {external_file_metadata}")
        return False

    isValid = True
    logger.debug(f"Checking metadata: {external_file_metadata}")
    for key in REQUIRED_EXTERNAL_FILE_METADATA_KEYS:
        if key not in external_file_metadata:
            logger.info(f"Missing key in external file metadata: {key}")
            isValid = False

    return isValid


def load_all_metadata(path: Path) -> Dict:
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


def check_data_and_group_by_retriever(file_metadata_list: list) -> Dict:
    """
    Group the file metadata by retriever. Exclude any invalid file_metadata items.

    Returns:
        Dict of retrievers and file_metadata lists, for example
        {
            "retriever1": [list1],
            "retriever2": [list2],
            ...
        }
    """
    grouped_by_retriever = {}
    for item in file_metadata_list:
        logger.debug(f"Checking item = {item}")
        if not is_valid_external_file_metadata(item):
            continue
        retriever = item.get("file_retriever")
        logger.debug(f"Found retriever = {retriever}")
        if retriever in grouped_by_retriever:
            grouped_by_retriever[retriever].append(item)
        else:
            grouped_by_retriever[retriever] = [item]

    if grouped_by_retriever == {}:
        return None
    return grouped_by_retriever
