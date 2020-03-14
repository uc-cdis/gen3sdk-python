import aiohttp
import backoff
import requests
import urllib.parse
import logging
import sys

from gen3.utils import append_query_params


def __log_backoff_retry(details):
    args_str = ", ".join(map(str, details["args"]))
    kwargs_str = (
        (", " + _print_kwargs(details["kwargs"])) if details.get("kwargs") else ""
    )
    func_call_log = "{}({}{})".format(
        _print_func_name(details["target"]), args_str, kwargs_str
    )
    logging.warning(
        "backoff: call {func_call} delay {wait:0.1f} seconds after {tries} tries".format(
            func_call=func_call_log, **details
        )
    )


def __log_backoff_giveup(details):
    args_str = ", ".join(map(str, details["args"]))
    kwargs_str = (
        (", " + _print_kwargs(details["kwargs"])) if details.get("kwargs") else ""
    )
    func_call_log = "{}({}{})".format(
        _print_func_name(details["target"]), args_str, kwargs_str
    )
    logging.error(
        "backoff: gave up call {func_call} after {tries} tries; exception: {exc}".format(
            func_call=func_call_log, exc=sys.exc_info(), **details
        )
    )


# Default settings to control usage of backoff library.
BACKOFF_SETTINGS = {
    "on_backoff": __log_backoff_retry,
    "on_giveup": __log_backoff_giveup,
    "max_tries": 2,
}


class Gen3Metadata:
    """
    A class for interacting with the Gen3 Index services.

    Args:
        endpoint (str): The URL of the data commons.
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Metadata class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> endpoint = "https://nci-crdc-demo.datacommons.io"
        ... auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... sub = Gen3Submission(endpoint, auth)

    """

    def __init__(self, endpoint, auth_provider=None, service_location="mds"):
        endpoint = endpoint.strip("/")
        # if running locally, mds is deployed by itself without a location relative
        # to the commons
        if "http://localhost" in endpoint:
            service_location = ""

        if not endpoint.endswith(service_location):
            endpoint += "/" + service_location

        self.endpoint = endpoint.rstrip("/")
        self.admin_endpoint = endpoint.rstrip("/") + "-admin"

    def is_healthy(self):
        """
        Return if indexd is healthy or not
        """
        try:
            response = requests.get(self.endpoint + "/_status")
            response.raise_for_status()
        except Exception as exc:
            logging.error(exc)
            return False

        return response.json().get("status") == "OK"

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def get_version(self):
        """
        Return the version of indexd
        """
        response = requests.get(self.endpoint + "/version")
        response.raise_for_status()
        return response.text

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def get_index_key_paths(self):
        """
        List all the metadata key paths indexed in the database.
        """
        response = requests.get(self.admin_endpoint + "/metadata_index")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def create_index_key_path(self, path):
        """
        Create a metadata key path indexed in the database.
        """
        response = requests.post(self.admin_endpoint + f"/metadata_index/{path}")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def delete_index_key_path(self, path):
        """
        List all the metadata key paths indexed in the database.
        """
        response = requests.delete(self.admin_endpoint + f"/metadata_index/{path}")
        response.raise_for_status()
        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def query(self, query, return_full_metadata=False, limit=10, offset=0, **kwargs):
        """
        Query the metadata given a query
        """
        url = self.endpoint + f"/metadata?{query}"

        url_with_params = append_query_params(
            url, data=return_full_metadata, limit=limit, offset=offset, **kwargs
        )
        logging.debug(f"hitting: {url_with_params}")
        response = requests.get(url_with_params)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def get(self, guid, **kwargs):
        """
        Get the metadata associated with the guid
        """
        url = self.endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        response = requests.get(url_with_params)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def batch_create(self, metadata_list, overwrite=True, **kwargs):
        """
        Create the list of metadata associated with the list of guids
        """
        url = self.admin_endpoint + f"/metadata"

        url_with_params = append_query_params(url, overwrite=overwrite, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {metadata_list}")
        response = requests.post(url_with_params, data=metadata_list)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def create(self, guid, metadata, overwrite=False, **kwargs):
        """
        Create the metadata associated with the guid
        """
        url = self.admin_endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, overwrite=overwrite, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {metadata}")
        response = requests.post(url_with_params, data=metadata)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def update(self, guid, metadata, overwrite=False, **kwargs):
        """
        Update the metadata associated with the guid
        """
        url = self.admin_endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, overwrite=overwrite, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        logging.debug(f"data: {metadata}")
        response = requests.put(url_with_params, data=metadata)
        response.raise_for_status()

        return response.json()

    @backoff.on_exception(backoff.expo, Exception, **BACKOFF_SETTINGS)
    def delete(self, guid, **kwargs):
        """
        Delete the metadata associated with the guid
        """
        url = self.admin_endpoint + f"/metadata/{guid}"

        url_with_params = append_query_params(url, **kwargs)
        logging.debug(f"hitting: {url_with_params}")
        response = requests.delete(url_with_params)
        response.raise_for_status()

        return response.json()


def _print_func_name(function):
    return "{}.{}".format(function.__module__, function.__name__)


def _print_kwargs(kwargs):
    return ", ".join("{}={}".format(k, repr(v)) for k, v in list(kwargs.items()))
